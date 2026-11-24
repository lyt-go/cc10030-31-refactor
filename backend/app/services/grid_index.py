"""网格索引领域服务：按空间层级划分网格单元，生成geohash索引并统计落点数量。

网格产物按"分区 + 版本"管理：
* 生成时读取当前有效定位点（坐标转换结果优先，缺失回退原始点），
  落格口径与原来一致，同时归档本次输入点快照并登记生成清单；
* 发布时把整份"格 + 点"登记为完整配对，并写入输入就绪状态与就绪凭据；
* 撤回只取消该配对的取数资格，归档文件保持可读，供已交接的旧结果复盘。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.services import spatial_inputs
from app.services.base import BaseModuleService
from app.services.spatial_inputs import (
    STATE_READY,
    STATE_REBUILD,
    STATE_UNPUBLISHED,
    GridPublicationLedger,
    SpatialInputError,
    SpatialSource,
    canonical_digest,
    grid_semantic_digest,
    point_revision,
    read_grid_file,
    read_grid_manifest,
    read_grid_points_snapshot,
    read_point_file,
    resolve_spatial_source,
    write_grid_artifacts,
)


class GridIndexService(BaseModuleService):
    MODULE = "grid_index"
    ENTITY = "网格单元"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["网格编号", "网格层级", "边界范围", "中心坐标", "落点数量", "索引精度", "生成方式", "索引版本"]
    REQUIRED_FIELDS = ["网格编号", "网格层级", "边界范围"]
    STATUS_ORDER = ["草稿", "待生成", "已生成", "已失效"]
    ACTION_RULES = {"启动生成": "待生成", "确认生成": "已生成", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = []

    # -- 输入解析（与热力共用同一份实现）--

    @property
    def publications(self) -> GridPublicationLedger:
        return GridPublicationLedger(self)

    def resolve_source(self, batch_key: str, version: str | None = None) -> SpatialSource:
        """解析一份可搭配的格与点来源，供网格复算和热力取数共用。"""
        return resolve_spatial_source(self, batch_key, version)

    # -- 生成 --

    def index_points_from_batch(self, batch_key: str, precision: int = 6,
                                *, grid_version: str | None = None,
                                source_revision: str | None = None) -> dict[str, Any]:
        """读取批次转换后的定位点数据文件，按geohash精度对定位点生成网格索引数据集。"""
        points, points_file = read_point_file(self, batch_key)
        if not points:
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据不存在")
        source_revision = source_revision or point_revision(points)

        grid_map = spatial_inputs.group_points(points, precision)
        preview = self._build_grids(batch_key, grid_map, precision, "pending", source_revision)
        # 输入与已发布版本等价时沿用版本号，避免重复生成凭空多版本。
        grid_version = grid_version or self._match_or_next_version(batch_key, preview, points)

        grids = self._build_grids(batch_key, grid_map, precision, grid_version, source_revision)
        artifacts = write_grid_artifacts(self, batch_key, grids, points,
                                         source_revision, grid_version)

        for g in grids:
            entry, _ = self.create_entry({
                "网格编号": f"GRID-{batch_key}-{grid_version}-{g['grid_id']:03d}",
                "网格层级": g["level"],
                "边界范围": g["geohash"],
                "中心坐标": f"{g['center_lat']},{g['center_lon']}",
                "落点数量": g["point_count"],
                "索引精度": str(precision),
                "生成方式": "geohash",
                "索引版本": grid_version,
            })
            if entry:
                self.run_action(entry["id"], "确认生成")

        # 生成只更新平铺产物与清单，不改变发布状态；发布是独立入口。
        self.publications.record_readiness(batch_key)

        return {
            "batch_key": batch_key,
            "grid_version": grid_version,
            "source_revision": source_revision,
            "total_points": len(points),
            "grids_generated": len(grids),
            "grid_checksum": canonical_digest(grids),
            "point_checksum": canonical_digest(points),
            "input_file": str(points_file) if points_file else None,
            "output_file": str(artifacts["grid_path"]),
            "point_snapshot": str(artifacts["point_path"]),
        }

    @staticmethod
    def _build_grids(batch_key: str, grid_map: dict[str, dict[str, Any]],
                     precision: int, grid_version: str,
                     source_revision: str) -> list[dict[str, Any]]:
        grids = []
        for grid_id, (gh, info) in enumerate(grid_map.items(), start=1):
            center_lat = sum(info["lats"]) / len(info["lats"])
            center_lon = sum(info["lons"]) / len(info["lons"])
            grids.append({
                "grid_id": grid_id,
                "geohash": gh,
                "batch_key": batch_key,
                "level": info["level"],
                "precision": precision,
                "center_lat": round(center_lat, 6),
                "center_lon": round(center_lon, 6),
                "point_count": info["point_count"],
                "source_revision": source_revision,
                "grid_version": grid_version,
                "generated_at": time.time(),
            })
        return grids

    def _match_or_next_version(self, batch_key: str, grids: list[dict[str, Any]],
                               points: list[dict[str, Any]]) -> str:
        semantic = grid_semantic_digest(grids)
        point_digest = canonical_digest(points)
        for record in self.publications.versions(batch_key):
            if (record.get("grid_semantic_digest") == semantic
                    and record.get("point_digest") == point_digest
                    and not record.get("withdrawn_at")):
                return record["version"]
        return self._next_version(batch_key)

    def _next_version(self, batch_key: str) -> str:
        highest = 0
        for version in (v["version"] for v in self.publications.versions(batch_key)):
            if version.startswith("v") and version[1:].isdigit():
                highest = max(highest, int(version[1:]))
        return f"v{highest + 1}"

    # -- 读取 --

    def get_grid_index(self, batch_key: str, version: str | None = None
                       ) -> tuple[list[dict[str, Any]], str]:
        """读取指定批次的网格索引数据集文件。

        指定 version 时读取该已发布版本的归档；否则读取最新平铺产物。
        """
        if version is not None:
            record = self.publications.find(batch_key, version)
            if record is None:
                raise FileNotFoundError(f"批次 {batch_key} 不存在网格版本 {version}")
            path = (self._data_dir("grid_index") / spatial_inputs._batch_dir(batch_key)
                    / version / "grid.jsonl")
            if not path.exists():
                raise FileNotFoundError(f"批次 {batch_key} 版本 {version} 的网格数据集不存在")
            return self._read_jsonl(path), str(path)
        grids, path = read_grid_file(self, batch_key)
        if not grids or path is None:
            raise FileNotFoundError(f"批次 {batch_key} 的网格索引数据集不存在")
        return grids, str(path)

    def list_versions(self, batch_key: str) -> list[dict[str, Any]]:
        return self.publications.versions(batch_key)

    # -- 输入就绪登记 --

    def check_input_readiness(self, batch_key: str) -> dict[str, Any]:
        """核对分区输入就绪状态：旧索引是否可用、纠偏后是否待重建。"""
        return self.publications.readiness(batch_key)

    partition_readiness = check_input_readiness
    input_readiness = check_input_readiness

    def record_input_readiness(self, batch_key: str) -> dict[str, Any]:
        """把就绪状态登记进发布台账，供热力独立核实。"""
        return self.publications.record_readiness(batch_key)

    register_readiness = record_input_readiness

    # -- 发布 --

    def publish_grid_index(self, batch_key: str, version: str | None = None,
                           *, note: str = "") -> dict[str, Any]:
        """把当前"格 + 点"整套输入发布为该分区的一个完整配对。

        只在新坐标已经配上对应网格时才允许发布为就绪；坐标更新而网格未重建时
        明确拒绝，不能仅凭坐标文件存在就放行。
        """
        manifest = read_grid_manifest(self, batch_key)
        if not manifest:
            raise SpatialInputError(
                f"批次 {batch_key} 没有生成清单，无法登记输入就绪状态，请重新生成网格")
        grids, _ = read_grid_file(self, batch_key)
        if not grids:
            raise FileNotFoundError(f"批次 {batch_key} 的网格索引数据不存在，请先生成网格")
        points, _ = read_grid_points_snapshot(self, batch_key)
        if not points:
            points, _ = read_point_file(self, batch_key)
        if not points:
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据不存在")

        readiness = self.check_input_readiness(batch_key)
        if readiness["state"] == STATE_REBUILD:
            raise SpatialInputError(
                f"批次 {batch_key} 的{readiness['reason']}，不能发布为就绪配对")
        if readiness["state"] == STATE_UNPUBLISHED:
            raise SpatialInputError(
                f"批次 {batch_key} 的分区输入未就绪：{readiness['reason']}")
        if readiness["state"] != STATE_READY:
            raise SpatialInputError(
                f"批次 {batch_key} 的网格缺少生成清单，请重新生成后再发布")

        version = version or manifest.get("grid_version") or self._next_version(batch_key)
        record = self.publications.publish(
            batch_key=batch_key,
            version=version,
            source_revision=manifest.get("source_revision", "unknown"),
            grids=grids,
            points=points,
            grid_path=None,
            point_path=None,
            readiness={
                "state": STATE_READY,
                "grids_ready": True,
                "coordinates_ready": True,
                "rebuild_required": False,
                "reason": None,
                "grid_digest": canonical_digest(grids),
                "point_digest": canonical_digest(points),
                "point_revision": point_revision(points),
            },
            note=note,
        )
        self.publications.record_readiness(batch_key)
        return {
            "batch_key": batch_key,
            "grid_version": record["version"],
            "source_revision": record["source_revision"],
            "grid_checksum": record["grid_digest"],
            "point_checksum": record["point_digest"],
            "readiness_token": record["readiness_token"],
            "input_readiness": record["input_readiness"],
            "published": True,
            "output_file": record["archive_grid_path"],
            "point_snapshot": record["archive_point_path"],
        }

    publish_partition = publish_grid_index
    publish = publish_grid_index

    def withdraw_grid_index(self, batch_key: str, version: str | None = None,
                            *, reason: str = "") -> dict[str, Any]:
        """撤回某个已发布的完整配对；文件仍可读，但常规取数不再放行。"""
        record = self.publications.withdraw(batch_key, version, reason)
        if record is None:
            raise FileNotFoundError(f"批次 {batch_key} 没有可撤回的已发布网格配对")
        grid_path = Path(record["archive_grid_path"]) if record.get("archive_grid_path") else None
        point_path = Path(record["archive_point_path"]) if record.get("archive_point_path") else None
        return {
            "batch_key": batch_key,
            "grid_version": record["version"],
            "withdrawn": True,
            "withdraw_reason": record.get("withdraw_reason"),
            "withdrawn_at": record.get("withdrawn_at"),
            "files_readable": bool(
                grid_path and point_path and grid_path.exists() and point_path.exists()),
            "current_version": self.publications.load()["partitions"]
            .get(batch_key, {}).get("current"),
        }

    withdraw_partition = withdraw_grid_index
    withdraw = withdraw_grid_index

    def get_publication(self, batch_key: str, version: str | None = None) -> dict[str, Any]:
        """返回分区发布状态、输入就绪状态与各版本信息。"""
        description = self.publications.describe(batch_key, version)
        description["input_readiness"] = self.check_input_readiness(batch_key)
        return description

    get_publication_status = get_publication
    describe_publication = get_publication

    def get_published_pair(self, batch_key: str, version: str | None = None) -> dict[str, Any]:
        """返回一份已发布配对的格、点与就绪凭据摘要。"""
        return self.resolve_source(batch_key, version).summary()
