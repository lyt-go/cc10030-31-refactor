"""热力聚合领域服务：按网格层级和时间粒度对定位点进行聚合计算，生成热力聚合数据集。

取数口径：
* 只从分区已发布且在用的"格 + 点"完整配对读取，凭就绪凭据核对；
* 文件仍可读不代表可以放行：已撤回的配对不再进入常规取数；
* 任何一次重算都记录它实际采用的格版本、当时采用的点与凭据；
* 每次交接（确认产出）都留档，之后可按交接时的格与点原样复盘。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services import spatial_inputs
from app.services.base import BaseModuleService
from app.services.spatial_inputs import (
    GridPublicationLedger,
    PartitionNotPublished,
    SpatialInputError,
    SpatialSource,
    resolve_spatial_source,
    resolve_version_pair,
)

RECOMPUTE_PENDING = "待确认"
RECOMPUTE_CONFIRMED = "已交接"

ELIGIBLE_PUBLISHED = "已发布可用"
ELIGIBLE_LEGACY = "旧文件复算"
ELIGIBLE_WITHDRAWN = "已撤回"
ELIGIBLE_NOT_READY = "未就绪"
ELIGIBLE_MISSING = "无输入"


class HeatmapAggregateService(BaseModuleService):
    MODULE = "heatmap_aggregate"
    ENTITY = "聚合窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "网格层级", "时间粒度", "聚合指标", "落点总量", "峰值网格", "窗口类型", "聚合水位"]
    REQUIRED_FIELDS = ["窗口编号", "网格层级", "时间粒度"]
    STATUS_ORDER = ["草稿", "待聚合", "聚合中", "已产出"]
    ACTION_RULES = {"启动聚合": "聚合中", "确认产出": "已产出", "标记作废": "已产出"}
    NEGATIVE_ACTIONS = []

    # -- 台账 --

    @property
    def publications(self) -> GridPublicationLedger:
        return GridPublicationLedger(self)

    @property
    def _recompute_path(self) -> Path:
        return self._data_dir("heatmap") / "_recompute_sessions.json"

    @property
    def _handover_path(self) -> Path:
        return self._data_dir("heatmap") / "_handovers.jsonl"

    @property
    def _pending_path(self) -> Path:
        return self._data_dir("heatmap") / "_pending_recomputes.jsonl"

    # -- 取数资格（常规热力看到的资格）--

    def check_data_eligibility(self, batch_key: str, grid_version: str | None = None
                               ) -> dict[str, Any]:
        """常规热力取数看到的资格：文件存在可读不等于仍可发布。

        已撤回的分区即使归档与平铺文件都完好，也只能作为"不可用"；
        没有发布登记的旧产物按旧文件配对复算，但不会被称为已发布。
        """
        ledger = self.publications
        description = ledger.describe(batch_key, grid_version)
        readiness = ledger.readiness(batch_key)
        record = description.get("version") or {}

        archive_readable = bool(
            record.get("archive_grid_path")
            and record.get("archive_point_path")
            and Path(record["archive_grid_path"]).exists()
            and Path(record["archive_point_path"]).exists())
        flat_readable = bool(spatial_inputs.read_grid_file(self, batch_key)[0])
        files_readable = archive_readable or flat_readable

        if description["has_publication"]:
            eligible = bool(description["usable"])
            if eligible:
                status = ELIGIBLE_PUBLISHED
            elif description["state"] == spatial_inputs.STATE_WITHDRAWN:
                status = ELIGIBLE_WITHDRAWN
            elif not description["selected_version"]:
                status = ELIGIBLE_WITHDRAWN if description.get("withdrawn") else ELIGIBLE_NOT_READY
            else:
                status = ELIGIBLE_NOT_READY
            reason = description.get("reason")
            source = "published"
        elif flat_readable:
            # 没有发布登记时仍可复算（格与同批点快照成对），但不会被称为已发布；
            # 坐标纠偏后的待重建状态单独暴露，供热力核实。
            eligible = True
            status = ELIGIBLE_LEGACY
            reason = "分区没有发布登记，按旧文件配对复算"
            source = "legacy"
        else:
            eligible = False
            status = ELIGIBLE_MISSING
            reason = "分区没有任何可读的网格产物"
            source = None

        return {
            "batch_key": batch_key,
            "eligible": eligible,
            "status": status,
            "published": bool(description.get("published")),
            "withdrawn": description.get("state") == spatial_inputs.STATE_WITHDRAWN,
            "files_readable": files_readable,
            "state": description.get("state"),
            "selected_version": description.get("selected_version"),
            "source": source,
            "reason": reason,
            "input_readiness": readiness,
            "partition_readiness": readiness,
        }

    check_fetch_eligibility = check_data_eligibility
    data_eligibility = check_data_eligibility
    eligibility = check_data_eligibility

    def verify_partition_inputs(self, batch_key: str) -> dict[str, Any]:
        """独立核实旧索引与纠偏后待重建分区，不依赖网格生成端的返回。"""
        ledger = self.publications
        return {
            "batch_key": batch_key,
            "input_readiness": ledger.readiness(batch_key),
            "publication": ledger.describe(batch_key),
            "eligible": self.check_data_eligibility(batch_key),
        }

    verify_inputs = verify_partition_inputs

    # -- 来源解析 --

    def _resolve_current(self, batch_key: str, grid_version: str | None = None) -> SpatialSource:
        return resolve_spatial_source(self, batch_key, grid_version)

    def _resolve_version(self, batch_key: str, grid_version: str) -> SpatialSource:
        return resolve_version_pair(self, batch_key, grid_version, allow_withdrawn=True)

    # -- 重算会话 --

    def _load_recomputes(self) -> dict[str, Any]:
        path = self._recompute_path
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_recomputes(self, data: dict[str, Any]) -> None:
        with open(self._recompute_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _session_snapshot(self, source: SpatialSource) -> dict[str, Any]:
        return {
            "grid_version": source.grid_version,
            "source_revision": source.source_revision,
            "source": source.origin,
            "grid_checksum": source.grid_digest,
            "point_checksum": source.point_digest,
            "point_revision": source.point_revision,
            "readiness_token": source.token,
            "grid_count": len(source.grids),
            "point_count": len(source.points),
            "at": time.time(),
        }

    def start_recompute(self, batch_key: str, grid_version: str | None = None
                        ) -> dict[str, Any]:
        """启动一份尚未确认的重算，固定本次可搭配的格与点。

        只接受当前有资格、且格与点同版本的配对。已有未确认会话时沿用其选择；
        需要更换版本请显式传入 grid_version。
        """
        sessions = self._load_recomputes()
        existing = sessions.get(batch_key)
        if (existing and existing.get("state") == RECOMPUTE_PENDING
                and grid_version is None):
            return dict(existing, resumed=True)

        source = self._resolve_current(batch_key, grid_version)
        now = time.time()
        history = list((existing or {}).get("history", []))
        history.append(self._session_snapshot(source))
        session = {
            "recompute_id": f"RCL-{batch_key}-{int(now * 1000)}",
            "batch_key": batch_key,
            "state": RECOMPUTE_PENDING,
            "grid_version": source.grid_version,
            "source_revision": source.source_revision,
            "source": source.origin,
            "grid_checksum": source.grid_digest,
            "point_checksum": source.point_digest,
            "point_revision": source.point_revision,
            "readiness_token": source.token,
            "grid_count": len(source.grids),
            "point_count": len(source.points),
            "created_at": now,
            "updated_at": now,
            "confirmed_at": None,
            "handover_id": None,
            "pending_recompute_ids": [],
            "history": history[-20:],
        }
        sessions[batch_key] = session
        self._save_recomputes(sessions)
        return dict(session)

    def get_recompute(self, batch_key: str) -> dict[str, Any] | None:
        session = self._load_recomputes().get(batch_key)
        return dict(session) if session else None

    get_recompute_session = get_recompute

    def switch_recompute_source(self, batch_key: str, grid_version: str) -> dict[str, Any]:
        """未确认前允许在两个都已发布且就绪的版本之间切换，但不允许跨版本配对。"""
        session = self._load_recomputes().get(batch_key)
        if session and session.get("state") == RECOMPUTE_CONFIRMED:
            raise SpatialInputError("该重算已完成交接，不能再切换来源版本")
        return self.start_recompute(batch_key, grid_version)

    def _ensure_session(self, batch_key: str, grid_version: str | None
                        ) -> tuple[dict[str, Any], bool]:
        """返回 (会话, 是否本次新启动)。"""
        session = self._load_recomputes().get(batch_key)
        if session is None or session.get("state") == RECOMPUTE_CONFIRMED:
            return self.start_recompute(batch_key, grid_version), True
        if grid_version is not None and grid_version != session["grid_version"]:
            return self.start_recompute(batch_key, grid_version), True
        return session, False

    def _source_for_session(self, batch_key: str, session: dict[str, Any]) -> SpatialSource:
        if session.get("source") == "published":
            source = self._resolve_version(batch_key, session["grid_version"])
        else:
            source = self._resolve_current(batch_key, session["grid_version"])
        # 会话固定的是启动时可搭配的那一对格与点；中途文件被替换就应报错，
        # 不能让这次重算悄悄换用另一份输入。
        if (session.get("grid_checksum") and source.grid_digest != session["grid_checksum"]) \
                or (session.get("point_checksum")
                    and source.point_digest != session["point_checksum"]):
            raise SpatialInputError(
                f"批次 {batch_key} 重算来源已变化，与启动时固定的格或点不一致")
        return source

    def run_recompute(self, batch_key: str, grid_version: str | None = None,
                      time_bucket_sec: int = 3600,
                      grid_level: str = "L6") -> dict[str, Any]:
        """执行未确认重算：用会话固定的格与点算热力，不改变交接记录。"""
        session, started = self._ensure_session(batch_key, grid_version)
        source = self._source_for_session(batch_key, session)
        result = self._aggregate_with_source(source, time_bucket_sec, grid_level)
        return self._decorate(result, session, started=started)

    def confirm_recompute(self, batch_key: str, grid_version: str | None = None,
                          time_bucket_sec: int = 3600,
                          grid_level: str = "L6") -> dict[str, Any]:
        """确认重算并登记为一次交接。

        若本次采用旧版本、而分区已存在更新的可用版本，则另留一条新格重算待办。
        """
        session, started = self._ensure_session(batch_key, grid_version)
        source = self._source_for_session(batch_key, session)
        if source.withdrawn:
            raise PartitionNotPublished(
                f"批次 {batch_key} 的版本 {source.grid_version} 已撤回，不能确认交接")
        result = self._aggregate_with_source(source, time_bucket_sec, grid_level)

        handover = {
            "handover_id": f"HND-{batch_key}-{int(time.time() * 1000)}",
            "batch_key": batch_key,
            "recompute_id": session["recompute_id"],
            "grid_version": source.grid_version,
            "source_revision": source.source_revision,
            "source": source.origin,
            "grid_checksum": source.grid_digest,
            "point_checksum": source.point_digest,
            "point_digest": source.point_digest,
            "point_revision": source.point_revision,
            "readiness_token": source.token,
            "grid_count": len(source.grids),
            "point_count": len(source.points),
            "time_bucket_sec": time_bucket_sec,
            "grid_level": grid_level,
            "heatmap_cells": result["heatmap_cells"],
            "total_points": result["total_points_aggregated"],
            "peak_grid": result["peak_grid"],
            "peak_count": result["peak_count"],
            "cells": result["_cells"],
            "output_file": result["output_file"],
            "handed_over_at": time.time(),
        }
        self._write_jsonl(self._handover_path, [handover])

        pending_records = self._pending_for_handover(batch_key, handover)
        if pending_records:
            self._write_jsonl(self._pending_path, pending_records)

        session.update({
            "state": RECOMPUTE_CONFIRMED,
            "confirmed_at": time.time(),
            "updated_at": time.time(),
            "handover_id": handover["handover_id"],
            "pending_recompute_ids": [t["todo_id"] for t in pending_records],
        })
        sessions = self._load_recomputes()
        sessions[batch_key] = session
        self._save_recomputes(sessions)

        payload = self._decorate(json.loads(json.dumps(result, default=str)), session,
                                 started=started)
        payload.update({
            "confirmed": True,
            "handover_id": handover["handover_id"],
            "pending_recomputes": pending_records,
            "pending_recompute": bool(pending_records),
        })
        return payload

    def _pending_for_handover(self, batch_key: str, handover: dict[str, Any]
                              ) -> list[dict[str, Any]]:
        """采用旧版交接时，为之后发布且可用的网格版本留下重算待办。"""
        versions = self.publications.versions(batch_key)
        used_index = next((i for i, v in enumerate(versions)
                           if v["version"] == handover["grid_version"]), None)
        if used_index is None:
            return []
        # 只关心本次交接版本之后发布的版本，旧版本不该反过来变成待办。
        newer = [v for v in versions[used_index + 1:] if self.publications.is_ready(v)]
        if not newer:
            return []
        record = newer[-1]
        return [{
            "todo_id": f"TODO-{batch_key}-{record['version']}-{int(time.time() * 1000)}",
            "batch_key": batch_key,
            "state": "待重算",
            "reason": "存在更新的已发布网格版本，本次交接仍使用旧配对",
            "used_version": handover["grid_version"],
            "available_version": record["version"],
            "available_grid_checksum": record.get("grid_digest"),
            "available_point_checksum": record.get("point_digest"),
            "readiness_token": record.get("readiness_token"),
            "handover_id": handover["handover_id"],
            "created_at": time.time(),
        }]

    def list_pending_recomputes(self, batch_key: str) -> list[dict[str, Any]]:
        """列出因采用旧版本而留下的新格重算待办。"""
        return [r for r in self._read_jsonl(self._pending_path)
                if r.get("batch_key") == batch_key]

    get_pending_recomputes = list_pending_recomputes

    # -- 交接复盘 --

    def list_handovers(self, batch_key: str) -> list[dict[str, Any]]:
        return [r for r in self._read_jsonl(self._handover_path)
                if r.get("batch_key") == batch_key]

    def get_handover(self, batch_key: str, handover_id: str | None = None
                     ) -> dict[str, Any] | None:
        records = self.list_handovers(batch_key)
        if not records:
            return None
        if handover_id is None:
            return records[-1]
        return next((r for r in records if r["handover_id"] == handover_id), None)

    def replay_handover(self, batch_key: str, handover_id: str | None = None,
                        *, time_bucket_sec: int | None = None,
                        grid_level: str | None = None) -> dict[str, Any]:
        """按上次交接所用的格与点复盘，与当前分区发布/就绪状态无关。

        新格未就绪、已就绪，甚至原版本已撤回，都能复算出交接时的原值。
        """
        handover = self.get_handover(batch_key, handover_id)
        if handover is None:
            raise FileNotFoundError(f"批次 {batch_key} 没有可复盘的交接记录")
        if self.publications.find(batch_key, handover["grid_version"]) is not None:
            source = self._resolve_version(batch_key, handover["grid_version"])
        elif handover.get("source") == "published":
            raise SpatialInputError(
                f"批次 {batch_key} 交接记录引用的网格版本 {handover['grid_version']} 已不存在")
        else:
            # 旧文件配对交接：用交接时留存的格与点摘要核对，不跟随当前文件改动。
            source = self._resolve_current(batch_key)
            if (source.grid_digest != handover["grid_checksum"]
                    or source.point_digest != handover["point_checksum"]):
                raise SpatialInputError(
                    f"批次 {batch_key} 的旧文件已变化，无法按交接时的配对复盘")
        result = self._aggregate_with_source(
            source,
            time_bucket_sec if time_bucket_sec is not None else handover["time_bucket_sec"],
            grid_level if grid_level is not None else handover["grid_level"],
        )
        reproduced = (
            result["_cells"] == handover.get("cells")
            and result["total_points_aggregated"] == handover.get("total_points")
            and result["peak_grid"] == handover.get("peak_grid")
            and result["peak_count"] == handover.get("peak_count")
        )
        result.pop("_cells", None)
        result.update({
            "replayed_from": handover["handover_id"],
            "grid_version_used": source.grid_version,
            "points_used": source.points_meta(),
            "reproduced": reproduced,
            "handover": {key: handover[key] for key in (
                "handover_id", "grid_version", "source_revision", "grid_checksum",
                "point_checksum", "readiness_token", "heatmap_cells",
                "total_points", "peak_grid", "peak_count") if key in handover},
        })
        return result

    replay_last_handover = replay_handover

    # -- 常规聚合 --

    def aggregate_grid_heatmap(self, batch_key: str, grid_level: str = "L6",
                               time_bucket_sec: int = 3600,
                               grid_version: str | None = None) -> dict[str, Any]:
        """按网格层级和时间粒度聚合定位点，生成热力数据集。

        使用与网格同版本发布、且在用的定位点；不允许把纠偏后的新坐标配到旧网格上。
        存在未确认重算会话时沿用其固定的来源版本。
        """
        session = self._load_recomputes().get(batch_key)
        selected_version = grid_version
        if selected_version is None and session and session.get("state") == RECOMPUTE_PENDING:
            # 未确认重算已固定来源时，常规取数也沿用同一版本，避免新旧口径混算。
            selected_version = session["grid_version"]

        source = self._resolve_current(batch_key, selected_version)
        if (session and session.get("state") == RECOMPUTE_PENDING
                and session.get("grid_checksum")
                and source.grid_digest != session["grid_checksum"]):
            raise SpatialInputError(
                f"批次 {batch_key} 仍有未确认重算固定在其他来源版本，"
                f"请先完成或切换该重算（当前要求 {session['grid_version']}）")
        result = self._aggregate_with_source(source, time_bucket_sec, grid_level)
        if session:
            result["recompute_id"] = session["recompute_id"]
            result["recompute_state"] = session["state"]
        return result

    def _decorate(self, result: dict[str, Any], session: dict[str, Any],
                  *, started: bool) -> dict[str, Any]:
        result.pop("_cells", None)
        result["recompute_id"] = session["recompute_id"]
        result["recompute_state"] = session["state"]
        result["recompute_started"] = started
        result["confirmed"] = session["state"] == RECOMPUTE_CONFIRMED
        result["handover_id"] = session.get("handover_id")
        return result

    def _aggregate_with_source(self, source: SpatialSource, time_bucket_sec: int,
                               grid_level: str) -> dict[str, Any]:
        """用给定配对算热力。落格与热点口径与原实现保持一致。"""
        points = source.points
        batch_key = source.batch_key
        base = {
            "batch_key": batch_key,
            "grid_level": grid_level,
            "time_bucket_sec": time_bucket_sec,
            "grid_version": source.grid_version,
            "grid_version_used": source.grid_version,
            "source_revision": source.source_revision,
            "source": source.origin,
            "grid_checksum": source.grid_digest,
            "point_checksum": source.point_digest,
            "readiness_token": source.token,
            "points_used": source.points_meta(),
        }
        if not points:
            return {**base,
                    "heatmap_cells": 0, "total_points_aggregated": 0,
                    "peak_grid": None, "peak_count": 0,
                    "output_file": None, "_cells": []}

        heatmap: dict[str, dict[str, Any]] = {}
        for p in points:
            lat = p.get("trans_lat", p.get("lat", 0))
            lon = p.get("trans_lon", p.get("lon", 0))
            gh = self._geohash_encode(lat, lon, 6)
            ts = p.get("timestamp", 0)
            time_bucket = int(ts // time_bucket_sec) * time_bucket_sec
            key = f"{gh}_{time_bucket}"
            if key not in heatmap:
                heatmap[key] = {"geohash": gh, "time_bucket": time_bucket,
                                "point_count": 0, "lats": [], "lons": []}
            heatmap[key]["point_count"] += 1
            heatmap[key]["lats"].append(lat)
            heatmap[key]["lons"].append(lon)

        cells = []
        peak_grid = None
        peak_count = 0
        total_points = 0
        for cell_id, info in enumerate(heatmap.values(), start=1):
            total_points += info["point_count"]
            center_lat = sum(info["lats"]) / len(info["lats"])
            center_lon = sum(info["lons"]) / len(info["lons"])
            cells.append({
                "cell_id": cell_id,
                "batch_key": batch_key,
                "geohash": info["geohash"],
                "grid_level": grid_level,
                "time_bucket": info["time_bucket"],
                "time_bucket_str": time.strftime("%Y-%m-%dT%H:%M:%S",
                                                 time.localtime(info["time_bucket"])),
                "point_count": info["point_count"],
                "center_lat": round(center_lat, 6),
                "center_lon": round(center_lon, 6),
                "grid_version": source.grid_version,
                "source_revision": source.source_revision,
                "readiness_token": source.token,
            })
            if info["point_count"] > peak_count:
                peak_count = info["point_count"]
                peak_grid = info["geohash"]

        output_file = self._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl"
        self._write_jsonl_replace(output_file, cells)

        return {**base,
                "heatmap_cells": len(cells),
                "total_points_aggregated": total_points,
                "peak_grid": peak_grid,
                "peak_count": peak_count,
                "output_file": str(output_file),
                "_cells": cells}

    def get_heatmap_data(self, batch_key: str) -> tuple[list[dict[str, Any]], str]:
        """读取指定批次的热力聚合数据集文件。"""
        file_path = self._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl"
        if not file_path.exists():
            raise FileNotFoundError(f"批次 {batch_key} 的热力聚合数据集不存在")
        return self._read_jsonl(file_path), str(file_path)
