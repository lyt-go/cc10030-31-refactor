"""网格索引与热力聚合共用的空间来源解析与发布配对登记。

把原先两边各写一遍的"格 + 点"读取、回退顺序与配对检查收拢到这里：

* 读取网格数据集（平铺的最新产物，或按版本归档的发布产物）；
* 读取定位点数据集（优先坐标转换结果，缺失时回退采集原始点）；
* 核对格与点是否属于同一来源版本，拒绝跨版本配对；
* 登记每个分区的完整发布配对（格 + 点 + 就绪凭据）与输入就绪状态；
* 支持撤回已发布配对，撤回后文件仍可读但不再具备常规取数资格。

落格与热点算法本身仍留在各领域服务里，保证稳定输入的计算口径不变。
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService, _ensure_dir

PUBLICATION_FILE = "_grid_publications.json"
GRID_MANIFEST_SUFFIX = "_grid_manifest.json"
GRID_POINTS_SUFFIX = "_points.jsonl"
GRID_FILE_SUFFIX = "_grid.jsonl"

# 分区输入就绪状态
STATE_READY = "就绪"
STATE_REBUILD = "待重建"        # 坐标已更新，对应格还没有重建
STATE_WITHDRAWN = "已撤回"
STATE_UNPUBLISHED = "未发布"
STATE_LEGACY = "仅旧文件"       # 没有发布登记，只有可直接读取的旧产物


def _batch_dir(batch_key: str) -> str:
    """归档子目录名：批次号里带路径分隔符时不能直接当作目录。"""
    return batch_key.replace("/", "_").replace("\\", "_").replace("..", "_")


class SpatialInputError(ValueError):
    """空间来源不可读取、不可搭配或不允许用于取数。"""


class PartitionNotPublished(SpatialInputError):
    """分区没有可用的发布配对（未发布或已撤回）。"""


# ---------------------------------------------------------------------------
# 摘要与凭据
# ---------------------------------------------------------------------------

def canonical_digest(records: list[dict[str, Any]]) -> str:
    """对数据集内容做稳定摘要：与文件路径、写入时间无关，只随记录内容变化。"""
    payload = json.dumps(records, sort_keys=True, ensure_ascii=False, default=str,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def grid_semantic_digest(grids: list[dict[str, Any]]) -> str:
    """网格落格结果的语义摘要：忽略版本号与生成时间，只比较格与落点分布。

    用于判断"同一批输入重新生成"是否等价，避免时间戳不同就凭空多出版本。
    """
    normalized = [
        {key: value for key, value in grid.items()
         if key not in ("generated_at", "grid_version")}
        for grid in grids
    ]
    return canonical_digest(normalized)


def point_revision(points: list[dict[str, Any]]) -> str:
    """依据记录字段判断点数据来源：坐标转换结果或采集原始点。"""
    if points and all("trans_lat" in p and "trans_lon" in p for p in points):
        return "coord_transform"
    return "geo_point"


def point_coordinates(point: dict[str, Any]) -> tuple[float, float]:
    """取定位点的生效坐标：坐标转换结果优先，缺失时回退原始坐标。"""
    return (point.get("trans_lat", point.get("lat", 0)),
            point.get("trans_lon", point.get("lon", 0)))


def group_points(points: list[dict[str, Any]], precision: int
                 ) -> dict[str, dict[str, Any]]:
    """按geohash精度把定位点归入网格，记录落点数量与参与中心计算的点。

    网格生成与配对核对共用这一份落格逻辑，保证两边口径一致。
    """
    from app.services.base import BaseModuleService

    grid_map: dict[str, dict[str, Any]] = {}
    for p in points:
        lat, lon = point_coordinates(p)
        gh = BaseModuleService._geohash_encode(lat, lon, precision)
        info = grid_map.setdefault(gh, {
            "geohash": gh, "precision": precision, "level": f"L{precision}",
            "point_count": 0, "lats": [], "lons": [],
        })
        info["point_count"] += 1
        info["lats"].append(lat)
        info["lons"].append(lon)
    return grid_map


def grids_match_points(grids: list[dict[str, Any]], points: list[dict[str, Any]],
                       precision: int | None = None) -> bool:
    """用定位点重算落格，核对网格数据集是否就是这批点算出来的。

    比较格集合、落点数量与中心坐标；坐标被纠偏但格没重建时，
    中心坐标会整体偏移，据此拒绝跨版本配对。
    """
    if not grids:
        return not points
    precision = precision or grids[0].get("precision") or 6
    expected = group_points(points, int(precision))
    if set(expected) != {g.get("geohash") for g in grids}:
        return False
    if len(expected) != len(grids):
        return False
    for grid in grids:
        info = expected.get(grid.get("geohash"))
        if info is None or info["point_count"] != grid.get("point_count"):
            return False
        center_lat = round(sum(info["lats"]) / len(info["lats"]), 6)
        center_lon = round(sum(info["lons"]) / len(info["lons"]), 6)
        if (abs(center_lat - float(grid.get("center_lat", 0))) > 1e-6
                or abs(center_lon - float(grid.get("center_lon", 0))) > 1e-6):
            return False
    return True


def readiness_token(batch_key: str, grid_digest: str, point_digest: str,
                    source_revision: str) -> str:
    """发布配对的就绪凭据：格、点与来源版本一起决定，缺一不可。"""
    raw = f"{batch_key}|{source_revision}|{grid_digest}|{point_digest}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# 数据集读取
# ---------------------------------------------------------------------------

def read_grid_file(service: BaseModuleService,
                   batch_key: str) -> tuple[list[dict[str, Any]], Path | None]:
    """读取批次最新的平铺网格数据集。"""
    path = service._data_dir("grid_index") / f"{batch_key}{GRID_FILE_SUFFIX}"
    if not path.exists():
        return [], None
    return service._read_jsonl(path), path


def read_point_file(service: BaseModuleService,
                    batch_key: str) -> tuple[list[dict[str, Any]], Path | None]:
    """读取批次定位点：优先坐标转换结果，缺失时回退采集原始点。

    这正是网格生成与热力取数原先各自实现过的"先转换后原始"顺序，现统一在此。
    """
    transformed = service._data_dir("coord_transformed") / f"{batch_key}_transformed.jsonl"
    if transformed.exists() and transformed.stat().st_size > 0:
        return service._read_jsonl(transformed), transformed
    raw = service._data_dir("geo_points") / f"{batch_key}.jsonl"
    if raw.exists():
        return service._read_jsonl(raw), raw
    return [], None


def read_grid_manifest(service: BaseModuleService, batch_key: str) -> dict[str, Any]:
    """读取网格生成时登记的输入清单（点摘要、来源版本等）。"""
    path = service._data_dir("grid_index") / f"{batch_key}{GRID_MANIFEST_SUFFIX}"
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def write_manifest(service: BaseModuleService, batch_key: str,
                   manifest: dict[str, Any]) -> Path:
    path = service._data_dir("grid_index") / f"{batch_key}{GRID_MANIFEST_SUFFIX}"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return path


def read_grid_points_snapshot(service: BaseModuleService,
                              batch_key: str) -> tuple[list[dict[str, Any]], Path | None]:
    """读取与最新网格同批归档的定位点快照。

    网格生成时把当次输入点一并存下来，坐标文件后来被纠偏覆盖也不会改变
    "这份格当时用的是哪些点"。
    """
    path = service._data_dir("grid_index") / f"{batch_key}{GRID_POINTS_SUFFIX}"
    if not path.exists():
        return [], None
    return service._read_jsonl(path), path


def write_grid_artifacts(service: BaseModuleService, batch_key: str,
                         grids: list[dict[str, Any]], points: list[dict[str, Any]],
                         source_revision: str, grid_version: str) -> dict[str, Any]:
    """覆盖写入网格产物，并把本次输入点快照与清单一起登记。"""
    grid_path = service._data_dir("grid_index") / f"{batch_key}{GRID_FILE_SUFFIX}"
    service._write_jsonl_replace(grid_path, grids)
    point_path = service._data_dir("grid_index") / f"{batch_key}{GRID_POINTS_SUFFIX}"
    service._write_jsonl_replace(point_path, points)

    manifest = {
        "batch_key": batch_key,
        "grid_version": grid_version,
        "source_revision": source_revision,
        "grid_digest": canonical_digest(grids),
        "grid_semantic_digest": grid_semantic_digest(grids),
        "point_digest": canonical_digest(points),
        "point_revision": point_revision(points),
        "grid_count": len(grids),
        "point_count": len(points),
        "generated_at": time.time(),
    }
    manifest_path = write_manifest(service, batch_key, manifest)
    return {"grid_path": grid_path, "point_path": point_path,
            "manifest_path": manifest_path, **manifest}


# ---------------------------------------------------------------------------
# 一份可搭配的"格 + 点"
# ---------------------------------------------------------------------------

@dataclass
class SpatialSource:
    """一份可搭配的"格 + 点"来源。"""

    batch_key: str
    grid_version: str
    source_revision: str
    grids: list[dict[str, Any]]
    points: list[dict[str, Any]]
    grid_path: Path | None
    point_path: Path | None
    grid_digest: str
    point_digest: str
    token: str
    origin: str  # published / legacy
    withdrawn: bool = False
    input_readiness: dict[str, Any] = field(default_factory=dict)

    @property
    def point_revision(self) -> str:
        return point_revision(self.points)

    def points_meta(self) -> dict[str, Any]:
        return {
            "point_revision": self.point_revision,
            "point_count": len(self.points),
            "point_checksum": self.point_digest,
            "point_path": str(self.point_path) if self.point_path else None,
        }

    def summary(self) -> dict[str, Any]:
        return {
            "batch_key": self.batch_key,
            "grid_version": self.grid_version,
            "grid_version_used": self.grid_version,
            "source_version": self.source_revision,
            "source_revision": self.source_revision,
            "source": self.origin,
            "withdrawn": self.withdrawn,
            "grid_count": len(self.grids),
            "point_count": len(self.points),
            "grid_path": str(self.grid_path) if self.grid_path else None,
            "point_path": str(self.point_path) if self.point_path else None,
            "grid_checksum": self.grid_digest,
            "point_checksum": self.point_digest,
            "point_digest": self.point_digest,
            "point_revision": self.point_revision,
            "points_used": self.points_meta(),
            "readiness_token": self.token,
            "input_readiness": self.input_readiness,
        }


def read_archive_pair(service: BaseModuleService, grid_path: str | Path,
                      point_path: str | Path) -> tuple[list[dict[str, Any]],
                                                        list[dict[str, Any]]]:
    """读取一对归档的格与点文件。"""
    grid_file, point_file = Path(grid_path), Path(point_path)
    if not grid_file.exists() or not point_file.exists():
        raise SpatialInputError(f"归档配对文件缺失：{grid_file} / {point_file}")
    return service._read_jsonl(grid_file), service._read_jsonl(point_file)


def resolve_legacy_pair(service: BaseModuleService, batch_key: str) -> SpatialSource:
    """解析未走发布登记的旧产物，并核对格与点是否同一来源版本。

    有生成清单时用清单里的点摘要核对；点优先取网格生成时保存的快照，
    因此纠偏后的坐标文件不会把旧格配到新点上。
    """
    grids, grid_path = read_grid_file(service, batch_key)
    if not grids:
        raise FileNotFoundError(f"批次 {batch_key} 的网格索引数据不存在，请先生成网格索引")
    manifest = read_grid_manifest(service, batch_key)

    snapshot, snapshot_path = read_grid_points_snapshot(service, batch_key)
    if snapshot:
        points = snapshot
        point_path = snapshot_path
    else:
        points, point_path = read_point_file(service, batch_key)
        if not points:
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据不存在")

    grid_digest = canonical_digest(grids)
    point_digest = canonical_digest(points)

    if manifest:
        if manifest.get("grid_digest") and manifest["grid_digest"] != grid_digest:
            raise SpatialInputError(
                f"批次 {batch_key} 的网格数据集与生成清单不一致，疑似被其他版本覆盖")
        if manifest.get("point_digest") and manifest["point_digest"] != point_digest:
            raise SpatialInputError(
                f"批次 {batch_key} 的网格与当前定位点不属于同一版本，不能跨版本配对")
    elif not grids_match_points(grids, points):
        raise SpatialInputError(
            f"批次 {batch_key} 的网格与当前定位点不属于同一版本，不能跨版本配对")

    revisions = {g.get("source_revision") for g in grids if g.get("source_revision")}
    if len(revisions) > 1:
        raise SpatialInputError(
            f"批次 {batch_key} 的网格记录来自多个来源版本：{sorted(revisions)}")
    source_revision = manifest.get("source_revision") or (
        next(iter(revisions)) if revisions else "legacy")
    grid_version = manifest.get("grid_version") or source_revision

    return SpatialSource(
        batch_key=batch_key,
        grid_version=grid_version,
        source_revision=source_revision,
        grids=grids,
        points=points,
        grid_path=grid_path,
        point_path=point_path,
        grid_digest=grid_digest,
        point_digest=point_digest,
        token=readiness_token(batch_key, grid_digest, point_digest, source_revision),
        origin="legacy",
        input_readiness={"state": STATE_LEGACY, "grids_ready": True,
                         "coordinates_ready": True, "rebuild_required": False,
                         "legacy": True, "reason": None},
    )


# ---------------------------------------------------------------------------
# 发布登记：每分区的已发布格 + 点 + 就绪凭据
# ---------------------------------------------------------------------------

class GridPublicationLedger:
    """每个分区已发布的完整配对及其输入就绪状态。

    登记只增加版本、不删除文件：撤回只是取消资格，归档文件仍可被
    已交接的记录复盘，但不会再被常规取数放行。
    """

    def __init__(self, service: BaseModuleService) -> None:
        self.service = service

    @property
    def path(self) -> Path:
        return self.service._data_dir("grid_index") / PUBLICATION_FILE

    # -- 存取 --

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"partitions": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"partitions": {}}
        data.setdefault("partitions", {})
        return data

    def save(self, data: dict[str, Any]) -> None:
        _ensure_dir(self.path.parent)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def partition(self, data: dict[str, Any], batch_key: str) -> dict[str, Any]:
        return data["partitions"].setdefault(
            batch_key, {"versions": [], "current": None, "readiness": None})

    def has_publication(self, batch_key: str) -> bool:
        return bool(self.versions(batch_key))

    def versions(self, batch_key: str) -> list[dict[str, Any]]:
        return list(self.load()["partitions"].get(batch_key, {}).get("versions", []))

    def find(self, batch_key: str, version: str | None) -> dict[str, Any] | None:
        versions = self.versions(batch_key)
        if version is None:
            current = self.load()["partitions"].get(batch_key, {}).get("current")
            if current:
                return next((v for v in versions if v["version"] == current), None)
            return None
        return next((v for v in versions if v["version"] == version), None)

    def current(self, batch_key: str) -> dict[str, Any] | None:
        return self.find(batch_key, None)

    @staticmethod
    def is_ready(record: dict[str, Any]) -> bool:
        return (not record.get("withdrawn_at")
                and record.get("input_readiness", {}).get("state", STATE_READY) == STATE_READY)

    def candidates(self, batch_key: str) -> list[dict[str, Any]]:
        """当前可用于常规取数的版本（未撤回且输入就绪）。"""
        return [v for v in self.versions(batch_key) if self.is_ready(v)]

    def latest_usable(self, batch_key: str, exclude: str | None = None
                      ) -> dict[str, Any] | None:
        usable = [v for v in self.candidates(batch_key) if v["version"] != exclude]
        return usable[-1] if usable else None

    # -- 发布 / 撤回 --

    def publish(self, batch_key: str, version: str, source_revision: str,
                grids: list[dict[str, Any]], points: list[dict[str, Any]],
                grid_path: Path | None, point_path: Path | None,
                readiness: dict[str, Any], note: str = "") -> dict[str, Any]:
        data = self.load()
        partition = self.partition(data, batch_key)
        grid_digest = canonical_digest(grids)
        point_digest = canonical_digest(points)
        existing = next((v for v in partition["versions"] if v["version"] == version), None)
        if existing is not None:
            if (existing.get("grid_digest") == grid_digest
                    and existing.get("point_digest") == point_digest
                    and not existing.get("withdrawn_at")):
                return existing
            raise SpatialInputError(
                f"批次 {batch_key} 的网格版本 {version} 已发布且内容不同，不能覆盖")

        archive_dir = self.service._data_dir("grid_index") / _batch_dir(batch_key) / version
        _ensure_dir(archive_dir)
        archive_grids = archive_dir / "grid.jsonl"
        archive_points = archive_dir / "points.jsonl"
        self.service._write_jsonl_replace(archive_grids, grids)
        self.service._write_jsonl_replace(archive_points, points)

        record = {
            "version": version,
            "source_revision": source_revision,
            "grid_digest": grid_digest,
            "grid_semantic_digest": grid_semantic_digest(grids),
            "point_digest": point_digest,
            "point_revision": point_revision(points),
            "grid_checksum": grid_digest,
            "point_checksum": point_digest,
            "readiness_token": readiness_token(batch_key, grid_digest, point_digest,
                                               source_revision),
            "grid_count": len(grids),
            "point_count": len(points),
            "archive_dir": str(archive_dir),
            "archive_grid_path": str(archive_grids),
            "archive_point_path": str(archive_points),
            "published_at": time.time(),
            "withdrawn_at": None,
            "withdraw_reason": None,
            "input_readiness": dict(readiness),
            "note": note,
        }
        partition["versions"].append(record)
        if readiness.get("state") == STATE_READY:
            partition["current"] = version
        data["partitions"][batch_key] = partition
        self.save(data)
        return record

    def withdraw(self, batch_key: str, version: str | None = None,
                 reason: str = "") -> dict[str, Any] | None:
        data = self.load()
        partition = data["partitions"].get(batch_key)
        if not partition or not partition.get("versions"):
            return None
        target = version or partition.get("current")
        if target is None:
            return None
        record = next((v for v in partition["versions"] if v["version"] == target), None)
        if record is None:
            return None
        if not record.get("withdrawn_at"):
            record["withdrawn_at"] = time.time()
            record["withdraw_reason"] = reason
        if partition.get("current") == target:
            remaining = [v for v in partition["versions"] if self.is_ready(v)]
            partition["current"] = remaining[-1]["version"] if remaining else None
        data["partitions"][batch_key] = partition
        self.save(data)
        return record

    # -- 就绪登记 --

    def record_readiness(self, batch_key: str) -> dict[str, Any]:
        """把分区当前输入就绪状态登记进发布台账，供热力独立核实。

        登记的是平铺工作区的就绪情况（旧索引可用 / 纠偏后待重建），
        不改写各已发布版本的配对状态：已发布配对是自洽的格 + 点，不随
        新坐标出现而失效，否则正在交接的旧格与原始点就无从复算。
        """
        readiness = self.readiness(batch_key)
        data = self.load()
        partition = self.partition(data, batch_key)
        partition["readiness"] = readiness
        data["partitions"][batch_key] = partition
        self.save(data)
        return readiness

    def readiness(self, batch_key: str) -> dict[str, Any]:
        """分区输入就绪核对：旧索引是否就绪、纠偏后是否需要重建。"""
        current_grids, _ = read_grid_file(self.service, batch_key)
        current_points, current_path = read_point_file(self.service, batch_key)
        manifest = read_grid_manifest(self.service, batch_key)
        grid_digest = canonical_digest(current_grids)
        point_digest = canonical_digest(current_points)

        grids_ready = bool(current_grids)
        coordinates_ready = bool(current_points)
        legacy = False
        rebuild_required = False
        reason = None

        if manifest.get("point_digest"):
            if point_digest != manifest["point_digest"]:
                # 坐标已更新，但平铺网格仍是旧坐标算出的结果
                rebuild_required = True
                reason = "坐标已更新但对应网格尚未重新生成"
            elif grid_digest != manifest.get("grid_digest"):
                rebuild_required = True
                reason = "网格数据集与生成清单不一致"
        elif current_grids:
            legacy = True
            reason = "网格没有生成清单，只能按旧产物核对"

        state = STATE_READY
        if rebuild_required:
            state = STATE_REBUILD
        elif not grids_ready:
            state = STATE_UNPUBLISHED
        elif legacy:
            state = STATE_LEGACY

        return {
            "batch_key": batch_key,
            "state": state,
            "grids_ready": grids_ready,
            "coordinates_ready": coordinates_ready,
            "rebuild_required": rebuild_required,
            "legacy": legacy,
            "reason": reason,
            "grid_digest": grid_digest,
            "point_digest": point_digest,
            "point_revision": point_revision(current_points),
            "current_point_path": str(current_path) if current_path else None,
            "manifest": manifest,
            "published_versions": [v["version"] for v in self.versions(batch_key)],
            "current_version": self.load()["partitions"].get(batch_key, {}).get("current"),
        }

    # -- 发布状态 --

    def describe(self, batch_key: str, version: str | None = None) -> dict[str, Any]:
        """返回分区发布状态、各版本输入就绪情况与分区就绪快照。"""
        data = self.load()
        partition = data["partitions"].get(batch_key)
        versions = partition["versions"] if partition else []
        if not partition or not versions:
            return {
                "batch_key": batch_key,
                "has_publication": False,
                "requested_version": version,
                "selected_version": None,
                "grid_version": None,
                "current_version": None,
                "published": False,
                "usable": False,
                "withdrawn": False,
                "state": STATE_LEGACY,
                "files_readable": bool(read_grid_file(self.service, batch_key)[0]),
                "versions": [],
                "version": None,
                "reason": "分区没有发布登记",
                "partition_readiness": self.readiness(batch_key),
            }

        current = partition.get("current")
        if version is not None:
            selected = next((v for v in versions if v["version"] == version), None)
        else:
            selected = next((v for v in versions if v["version"] == current), None)

        files_readable = any(
            Path(v.get("archive_grid_path", "")).exists() for v in versions)
        base = {
            "batch_key": batch_key,
            "has_publication": True,
            "requested_version": version,
            "current_version": current,
            "versions": versions,
            "files_readable": files_readable,
            "partition_readiness": self.readiness(batch_key),
            "candidates": [v["version"] for v in versions if self.is_ready(v)],
        }

        if selected is None:
            all_withdrawn = all(v.get("withdrawn_at") for v in versions)
            state = STATE_WITHDRAWN if all_withdrawn else STATE_UNPUBLISHED
            return {
                **base,
                "selected_version": None,
                "grid_version": None,
                "published": False,
                "usable": False,
                "withdrawn": all_withdrawn,
                "state": state,
                "version": None,
                "reason": ("分区已发布的配对全部撤回，文件仍可读但不再放行"
                           if all_withdrawn
                           else ("请求的版本没有发布记录" if version
                                 else "分区没有当前可用版本")),
            }

        withdrawn = bool(selected.get("withdrawn_at"))
        readiness_state = selected.get("input_readiness", {}).get("state", STATE_READY)
        state = STATE_WITHDRAWN if withdrawn else readiness_state
        usable = not withdrawn and readiness_state == STATE_READY
        reason = None
        if withdrawn:
            reason = f"分区版本 {selected['version']} 已撤回，文件仍可读但不再放行"
        elif not usable:
            reason = selected.get("input_readiness", {}).get("reason", "分区输入未就绪")
        return {
            **base,
            "selected_version": selected["version"],
            "grid_version": selected["version"],
            "source_revision": selected.get("source_revision"),
            "published": not withdrawn,
            "usable": usable,
            "withdrawn": withdrawn,
            "state": state,
            "version": selected,
            "reason": reason,
        }


# ---------------------------------------------------------------------------
# 统一来源解析
# ---------------------------------------------------------------------------

def resolve_spatial_source(service: BaseModuleService, batch_key: str,
                           version: str | None = None) -> SpatialSource:
    """统一解析取数可用的格与点。

    分区有发布登记时只使用已发布且就绪的版本；绝不因为归档文件仍可读
    就绕过撤回状态，也不回退到未登记的平铺旧产物。
    """
    ledger = GridPublicationLedger(service)
    if ledger.has_publication(batch_key):
        description = ledger.describe(batch_key, version)
        if not description["usable"]:
            raise PartitionNotPublished(
                description.get("reason") or "分区没有可用的发布配对")
        record = description["version"]
        grids, points = read_archive_pair(service, record["archive_grid_path"],
                                          record["archive_point_path"])
        grid_digest = canonical_digest(grids)
        point_digest = canonical_digest(points)
        if record.get("grid_digest") and grid_digest != record["grid_digest"]:
            raise SpatialInputError(f"批次 {batch_key} 的归档网格摘要不一致")
        if record.get("point_digest") and point_digest != record["point_digest"]:
            raise SpatialInputError(f"批次 {batch_key} 的归档点摘要不一致")
        return SpatialSource(
            batch_key=batch_key,
            grid_version=record["version"],
            source_revision=record.get("source_revision", "unknown"),
            grids=grids,
            points=points,
            grid_path=Path(record["archive_grid_path"]),
            point_path=Path(record["archive_point_path"]),
            grid_digest=grid_digest,
            point_digest=point_digest,
            token=record.get("readiness_token") or readiness_token(
                batch_key, grid_digest, point_digest, record.get("source_revision", "unknown")),
            origin="published",
            withdrawn=bool(record.get("withdrawn_at")),
            input_readiness=record.get("input_readiness", {}),
        )

    return resolve_legacy_pair(service, batch_key)


def resolve_version_pair(service: BaseModuleService, batch_key: str,
                         version: str, *, allow_withdrawn: bool = True) -> SpatialSource:
    """按版本号读取归档配对。

    复盘已交接结果时允许读到已撤回版本（文件仍在，配对不变）；
    常规取数应走 resolve_spatial_source，不会放行撤回版本。
    """
    ledger = GridPublicationLedger(service)
    record = ledger.find(batch_key, version)
    if record is None:
        raise SpatialInputError(f"批次 {batch_key} 不存在网格版本 {version}")
    if record.get("withdrawn_at") and not allow_withdrawn:
        raise PartitionNotPublished(f"批次 {batch_key} 版本 {version} 已撤回")
    grids, points = read_archive_pair(service, record["archive_grid_path"],
                                      record["archive_point_path"])
    grid_digest = canonical_digest(grids)
    point_digest = canonical_digest(points)
    if record.get("grid_digest") and grid_digest != record["grid_digest"]:
        raise SpatialInputError(f"批次 {batch_key} 版本 {version} 的归档网格摘要不一致")
    if record.get("point_digest") and point_digest != record["point_digest"]:
        raise SpatialInputError(f"批次 {batch_key} 版本 {version} 的归档点摘要不一致")
    return SpatialSource(
        batch_key=batch_key,
        grid_version=version,
        source_revision=record.get("source_revision", "unknown"),
        grids=grids,
        points=points,
        grid_path=Path(record["archive_grid_path"]),
        point_path=Path(record["archive_point_path"]),
        grid_digest=grid_digest,
        point_digest=point_digest,
        token=record.get("readiness_token") or readiness_token(
            batch_key, grid_digest, point_digest, record.get("source_revision", "unknown")),
        origin="published",
        withdrawn=bool(record.get("withdrawn_at")),
        input_readiness=record.get("input_readiness", {}),
    )
