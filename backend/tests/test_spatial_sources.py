"""空间来源配对、网格发布与热力交接复盘测试。

覆盖：
* 网格与热力共用同一份输入解析；
* 分区发布登记格、点与就绪凭据，热力只取已发布且就绪的配对；
* 坐标更新但网格未重建时不能发布；
* 撤回后文件仍可读，但常规取数不再放行，旧交接仍按旧配对复算；
* 未确认重算固定来源版本，两个已发布版本各自可用但不能跨版本配对；
* 另用一区验证两版本重算，不撤回，交接各自可复盘。
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def setup_data_dir(monkeypatch):
    tmpdir = Path(tempfile.mkdtemp(prefix="geo_source_test_"))
    import app.services.base as base_mod
    original_root = base_mod.DATA_ROOT
    base_mod.DATA_ROOT = tmpdir
    for sub in ["geo_points", "coord_transformed", "track_segments", "dwell_events",
                "grid_index", "heatmap", "dedup_merged", "spatial_join",
                "poi_match", "region_aggregates", "od_matrix", "route_restore",
                "density_estimate", "quality_reports", "snapshots", "exports", "checkpoints"]:
        (tmpdir / sub).mkdir(parents=True, exist_ok=True)
    yield tmpdir
    base_mod.DATA_ROOT = original_root
    if tmpdir.exists():
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def services():
    from app.services.geo_source import GeoSourceService
    from app.services.coord_transform import CoordTransformService
    from app.services.grid_index import GridIndexService
    from app.services.heatmap_aggregate import HeatmapAggregateService
    return {
        "geo": GeoSourceService(),
        "coord": CoordTransformService(),
        "grid": GridIndexService(),
        "heat": HeatmapAggregateService(),
    }


def _prepare(services, batch_key: str, count: int = 20):
    services["geo"].onboard_batch({
        "批次编号": batch_key, "采集终端": "TERM-T", "定位方式": "GPS", "采样频率": "1Hz",
    }, point_count=count)
    services["coord"].transform_batch(batch_key)


def _shift_coordinates(services, batch_key: str, delta: float) -> None:
    """模拟纠偏：就地更正转换后的坐标文件。"""
    import app.services.base as base_mod
    coord = services["coord"]
    points, _ = coord.get_transformed_points(batch_key)
    for p in points:
        p["trans_lat"] = round(p["trans_lat"] + delta, 6)
        p["trans_lon"] = round(p["trans_lon"] + delta, 6)
    target = base_mod.DATA_ROOT / "coord_transformed" / f"{batch_key}_transformed.jsonl"
    coord._write_jsonl_replace(target, points)


def _publish_ready(services, batch_key: str) -> str:
    gen = services["grid"].index_points_from_batch(batch_key)
    published = services["grid"].publish_grid_index(batch_key)
    assert published["grid_version"] == gen["grid_version"]
    return published["grid_version"]


class TestSharedSpatialInputs:
    def test_grid_and_heatmap_use_same_resolver(self, services):
        """网格与热力解析出的格与点来自同一次实现，摘要一致。"""
        batch = "SHARED-01"
        _prepare(services, batch)
        services["grid"].index_points_from_batch(batch)
        grid_source = services["grid"].resolve_source(batch)
        heat_source = services["heat"]._resolve_current(batch)
        assert grid_source.grid_digest == heat_source.grid_digest
        assert grid_source.point_digest == heat_source.point_digest
        assert grid_source.token == heat_source.token
        assert grid_source.point_revision == "coord_transform"

    def test_point_reader_prefers_transformed_then_raw(self, services):
        """共享点读取保留原有优先级：转换结果优先，缺失时回退采集原始点。"""
        from app.services.spatial_inputs import read_point_file
        batch = "FALLBACK-01"
        _prepare(services, batch)
        points, path = read_point_file(services["grid"], batch)
        assert points and "trans_lat" in points[0]
        assert path.name.endswith("_transformed.jsonl")

        import app.services.base as base_mod
        (base_mod.DATA_ROOT / "coord_transformed" / f"{batch}_transformed.jsonl").unlink()
        points, path = read_point_file(services["grid"], batch)
        assert points and "lat" in points[0] and "trans_lat" not in points[0]
        assert path.name == f"{batch}.jsonl"

    def test_legacy_pair_rejects_cross_version_points(self, services):
        """只有旧格、坐标文件已纠偏时，不能把新点配到旧格。"""
        from app.services.spatial_inputs import SpatialInputError, resolve_legacy_pair
        batch = "CROSS-01"
        _prepare(services, batch)
        services["grid"].index_points_from_batch(batch)
        _shift_coordinates(services, batch, 0.002)
        # 快照仍是旧点，因此按快照复算而不是报错；
        # 但删掉快照后只剩纠偏后的坐标，就必须拒绝。
        import app.services.base as base_mod
        (base_mod.DATA_ROOT / "grid_index" / f"{batch}_points.jsonl").unlink()
        (base_mod.DATA_ROOT / "grid_index" / f"{batch}_grid_manifest.json").unlink()
        with pytest.raises(SpatialInputError):
            resolve_legacy_pair(services["grid"], batch)


class TestGridPublication:
    def test_publish_registers_pair_and_token(self, services):
        """发布登记格、点与就绪凭据，热力可独立核实。"""
        batch = "PUB-01"
        _prepare(services, batch)
        version = _publish_ready(services, batch)

        pair = services["grid"].get_published_pair(batch, version)
        assert pair["grid_version"] == version
        assert pair["grid_checksum"] and pair["point_checksum"]
        assert pair["readiness_token"]
        assert pair["points_used"]["point_revision"] == "coord_transform"

        description = services["grid"].get_publication(batch)
        assert description["usable"] is True
        assert description["selected_version"] == version
        assert description["input_readiness"]["state"] == "就绪"

        verify = services["heat"].verify_partition_inputs(batch)
        assert verify["input_readiness"]["state"] == "就绪"
        assert verify["eligible"]["status"] == "已发布可用"

    def test_coordinate_update_without_grid_cannot_publish(self, services):
        """坐标已更新但对应格还没生成时，不得只靠新坐标就标成可发布。"""
        from app.services.spatial_inputs import SpatialInputError
        batch = "REBUILD-01"
        _prepare(services, batch)
        _publish_ready(services, batch)

        _shift_coordinates(services, batch, 0.003)
        readiness = services["grid"].check_input_readiness(batch)
        assert readiness["state"] == "待重建"
        assert readiness["rebuild_required"] is True

        with pytest.raises(SpatialInputError):
            services["grid"].publish_grid_index(batch, version="v2")

        # 已发布旧配对仍可用：格与点同版本，不因新坐标出现就失效。
        assert services["heat"].check_data_eligibility(batch)["eligible"] is True

        # 重新生成后坐标与格对齐，方可发布新版本。
        gen = services["grid"].index_points_from_batch(batch)
        assert gen["grid_version"] == "v2"
        readiness = services["grid"].check_input_readiness(batch)
        assert readiness["state"] == "就绪"
        published = services["grid"].publish_grid_index(batch)
        assert published["grid_version"] == "v2"

    def test_withdraw_keeps_files_but_blocks_fetch(self, services):
        """撤回后文件仍可读，但常规热力取数不再放行。"""
        from app.services.spatial_inputs import PartitionNotPublished
        batch = "WITHDRAW-01"
        _prepare(services, batch)
        version = _publish_ready(services, batch)

        withdrawn = services["grid"].withdraw_grid_index(batch, version, reason="人工撤回")
        assert withdrawn["withdrawn"] is True
        assert withdrawn["files_readable"] is True

        eligibility = services["heat"].check_data_eligibility(batch)
        assert eligibility["files_readable"] is True
        assert eligibility["eligible"] is False
        assert eligibility["status"] == "已撤回"
        assert eligibility["withdrawn"] is True
        assert eligibility["published"] is False

        with pytest.raises(PartitionNotPublished):
            services["heat"].aggregate_grid_heatmap(batch)

    def test_one_withdrawn_version_falls_back_to_remaining(self, services):
        """只撤回新版本时，仍可回落到未撤回的旧配对。"""
        batch = "WITHDRAW-02"
        _prepare(services, batch)
        v1 = _publish_ready(services, batch)
        _shift_coordinates(services, batch, 0.002)
        services["grid"].index_points_from_batch(batch)
        v2 = services["grid"].publish_grid_index(batch)["grid_version"]
        assert (v1, v2) == ("v1", "v2")

        services["grid"].withdraw_grid_index(batch, v2)
        eligibility = services["heat"].check_data_eligibility(batch)
        assert eligibility["eligible"] is True
        assert eligibility["selected_version"] == v1
        result = services["heat"].aggregate_grid_heatmap(batch)
        assert result["grid_version_used"] == v1


class TestHeatmapHandoverReplay:
    def test_handover_replays_after_withdraw_and_new_versions(self, services):
        """旧交接可按交接时的格与点复盘，不受撤回与新版本影响。"""
        batch = "REPLAY-01"
        _prepare(services, batch)
        _publish_ready(services, batch)
        handover = services["heat"].confirm_recompute(batch)
        assert handover["confirmed"] is True
        assert handover["grid_version_used"] == "v1"

        # 纠偏 + 新格发布
        _shift_coordinates(services, batch, 0.002)
        services["grid"].index_points_from_batch(batch)
        services["grid"].publish_grid_index(batch)

        replay = services["heat"].replay_handover(batch)
        assert replay["reproduced"] is True
        assert replay["grid_version_used"] == "v1"
        assert replay["handover"]["handover_id"] == handover["handover_id"]

        # 撤回两版后仍可复盘
        services["grid"].withdraw_grid_index(batch, "v1")
        services["grid"].withdraw_grid_index(batch, "v2")
        replay = services["heat"].replay_handover(batch)
        assert replay["reproduced"] is True

    def test_recompute_records_actual_grid_and_points(self, services):
        """每次重算都记录实际采用的格版本、当时采用的点与凭据。"""
        batch = "RECORD-01"
        _prepare(services, batch)
        _publish_ready(services, batch)
        session = services["heat"].start_recompute(batch)
        result = services["heat"].run_recompute(batch)
        assert result["grid_version_used"] == session["grid_version"]
        assert result["points_used"]["point_checksum"] == session["point_checksum"]
        assert result["readiness_token"] == session["readiness_token"]
        assert result["points_used"]["point_revision"] == "coord_transform"

    def test_unconfirmed_recompute_pins_source_and_ignores_new_grid(self, services):
        """未确认重算先启动时只有原版整套输入有效，新格发布后也不改变其来源。"""
        batch = "PIN-01"
        _prepare(services, batch)
        _publish_ready(services, batch)
        handover = services["heat"].confirm_recompute(batch)
        original_token = handover["readiness_token"]

        session = services["heat"].start_recompute(batch)
        assert session["grid_version"] == "v1"
        assert session["readiness_token"] == original_token

        # 确认前坐标更正与配套新格随后独立发布
        _shift_coordinates(services, batch, 0.003)
        services["grid"].index_points_from_batch(batch)
        services["grid"].publish_grid_index(batch)
        assert services["heat"].check_data_eligibility(batch)["selected_version"] == "v2"

        # 未确认重算仍固定原版格与原始点
        run = services["heat"].run_recompute(batch)
        assert run["grid_version_used"] == "v1"
        assert run["points_used"]["point_checksum"] == session["point_checksum"]
        assert run["readiness_token"] == original_token

        # 两个版本都可用于这份尚未交接的重算，但不能跨版本配对
        switched = services["heat"].switch_recompute_source(batch, "v2")
        assert switched["grid_version"] == "v2"
        run_v2 = services["heat"].run_recompute(batch)
        assert run_v2["grid_version_used"] == "v2"
        assert run_v2["points_used"]["point_checksum"] != session["point_checksum"]
        assert run_v2["readiness_token"] != original_token

    def test_old_version_handover_leaves_pending_recompute(self, services):
        """若按旧版完成交接，为已发布的新格另留重算待办。"""
        batch = "PENDING-01"
        _prepare(services, batch)
        _publish_ready(services, batch)
        services["heat"].confirm_recompute(batch)

        _shift_coordinates(services, batch, 0.003)
        services["grid"].index_points_from_batch(batch)
        services["grid"].publish_grid_index(batch)

        # 未确认重算继续用旧版
        services["heat"].start_recompute(batch, "v1")
        confirmed = services["heat"].confirm_recompute(batch, "v1")
        assert confirmed["grid_version_used"] == "v1"
        assert confirmed["pending_recompute"] is True
        assert confirmed["pending_recomputes"][0]["available_version"] == "v2"

        pending = services["heat"].list_pending_recomputes(batch)
        assert [p["available_version"] for p in pending] == ["v2"]
        assert pending[0]["used_version"] == "v1"

        # 按新版交接则不再留下待办
        services["heat"].start_recompute(batch, "v2")
        confirmed_v2 = services["heat"].confirm_recompute(batch, "v2")
        assert confirmed_v2["grid_version_used"] == "v2"
        assert confirmed_v2["pending_recompute"] is False

    def test_other_partition_two_version_recompute_without_withdrawal(self, services):
        """另一分区两版本重算：都不撤回，各自交接均可复盘。"""
        batch = "ZONE-B"
        _prepare(services, batch)
        _publish_ready(services, batch)
        first = services["heat"].confirm_recompute(batch)
        assert first["grid_version_used"] == "v1"

        _shift_coordinates(services, batch, 0.004)
        services["grid"].index_points_from_batch(batch)
        services["grid"].publish_grid_index(batch)
        second = services["heat"].confirm_recompute(batch, "v2")
        assert second["grid_version_used"] == "v2"

        assert services["heat"].check_data_eligibility(batch)["status"] == "已发布可用"
        assert services["heat"].replay_handover(
            batch, first["handover_id"])["reproduced"] is True
        assert services["heat"].replay_handover(
            batch, second["handover_id"])["reproduced"] is True

    def test_observation_moves_to_neighbour_cell_after_rectification(self, services):
        """纠偏后同一观测落格变化，但复盘仍能还原各版本原值。"""
        from app.services.spatial_inputs import canonical_digest
        batch = "MOVE-01"
        _prepare(services, batch, count=30)
        v1 = _publish_ready(services, batch)
        old_source = services["heat"]._resolve_current(batch)
        first = services["heat"].confirm_recompute(batch)

        _shift_coordinates(services, batch, 0.05)
        services["grid"].index_points_from_batch(batch)
        v2 = services["grid"].publish_grid_index(batch)["grid_version"]
        new_source = services["heat"]._resolve_current(batch)

        assert v1 == "v1" and v2 == "v2"
        # 落格确实变化：格分布摘要不同
        assert canonical_digest(old_source.grids) != canonical_digest(new_source.grids)

        second = services["heat"].confirm_recompute(batch, "v2")
        assert services["heat"].replay_handover(
            batch, first["handover_id"])["reproduced"] is True
        assert services["heat"].replay_handover(
            batch, second["handover_id"])["reproduced"] is True
        assert services["heat"].replay_handover(
            batch, first["handover_id"])["grid_version_used"] == "v1"
        assert services["heat"].replay_handover(
            batch, second["handover_id"])["grid_version_used"] == "v2"

    def test_handover_replays_when_new_grid_not_ready_then_ready(self, services):
        """已交接旧热力在新格未就绪与就绪后都能复算原值。"""
        batch = "REPLAY-02"
        _prepare(services, batch)
        _publish_ready(services, batch)
        handover = services["heat"].confirm_recompute(batch)

        # 新格未就绪：只有坐标更新，网格尚未重建
        _shift_coordinates(services, batch, 0.002)
        assert services["grid"].check_input_readiness(batch)["state"] == "待重建"
        assert services["heat"].replay_handover(batch)["reproduced"] is True

        # 新格就绪并发布
        services["grid"].index_points_from_batch(batch)
        services["grid"].publish_grid_index(batch)
        assert services["grid"].check_input_readiness(batch)["state"] == "就绪"
        replay = services["heat"].replay_handover(batch)
        assert replay["reproduced"] is True
        assert replay["handover"]["handover_id"] == handover["handover_id"]

    def test_heatmap_independently_verifies_registered_readiness(self, services):
        """网格发布端登记的分区就绪状态，热力可独立核实。"""
        batch = "VERIFY-01"
        _prepare(services, batch)
        _publish_ready(services, batch)

        verify = services["heat"].verify_partition_inputs(batch)
        assert verify["input_readiness"]["state"] == "就绪"
        assert verify["publication"]["partition_readiness"]["state"] == "就绪"

        _shift_coordinates(services, batch, 0.003)
        services["grid"].record_input_readiness(batch)
        verify = services["heat"].verify_partition_inputs(batch)
        assert verify["input_readiness"]["state"] == "待重建"
        assert verify["input_readiness"]["rebuild_required"] is True
        # 已发布旧配对仍然自洽可用
        assert verify["publication"]["usable"] is True
        assert verify["eligible"]["status"] == "已发布可用"

    def test_recompute_refuses_source_changed_after_start(self, services):
        """未确认重算启动后来源文件被替换时，不能悄悄换用另一份输入。"""
        from app.services.spatial_inputs import SpatialInputError
        batch = "PIN-02"
        _prepare(services, batch)
        version = _publish_ready(services, batch)
        services["heat"].start_recompute(batch)
        # 直接篡改该已发布版本的归档点文件，模拟来源漂移
        record = services["grid"].publications.find(batch, version)
        archive = Path(record["archive_point_path"])
        points = services["grid"]._read_jsonl(archive)
        for p in points:
            p["trans_lat"] = round(p["trans_lat"] + 0.002, 6)
        services["grid"]._write_jsonl_replace(archive, points)
        with pytest.raises(SpatialInputError):
            services["heat"].run_recompute(batch)

