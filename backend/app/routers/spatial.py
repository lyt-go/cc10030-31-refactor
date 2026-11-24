"""空间来源专项接口：网格发布/撤回/就绪登记，热力重算与交接复盘。

这些是跨模块数据流入口，不走通用资源路由，因此单独注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.grid_index import GridIndexService
from app.services.heatmap_aggregate import HeatmapAggregateService
from app.services.spatial_inputs import SpatialInputError

router = APIRouter(prefix="/api", tags=["空间来源"])

grid_service = GridIndexService()
heatmap_service = HeatmapAggregateService()


class PartitionPayload(BaseModel):
    batch_key: str
    version: str | None = None
    reason: str | None = None


class GridPublishPayload(BaseModel):
    batch_key: str
    version: str | None = None
    note: str = ""


class HeatmapRunPayload(BaseModel):
    batch_key: str
    grid_version: str | None = None
    time_bucket_sec: int = Field(default=3600, ge=1)
    grid_level: str = "L6"


class HandoverReplayPayload(BaseModel):
    batch_key: str
    handover_id: str | None = None


def _handle(fn, *args, **kwargs) -> Any:
    try:
        return fn(*args, **kwargs)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SpatialInputError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


# -- 网格发布端 --

@router.get("/grid_index/versions/{batch_key}", response_model=dict)
def grid_versions(batch_key: str) -> dict[str, Any]:
    """列出分区的网格版本与发布/就绪状态。"""
    return _handle(grid_service.get_publication, batch_key)


@router.post("/grid_index/readiness", response_model=dict)
def grid_readiness(payload: PartitionPayload) -> dict[str, Any]:
    """核对分区输入就绪状态：旧索引是否可用、纠偏后是否待重建。"""
    return _handle(grid_service.check_input_readiness, payload.batch_key)


@router.post("/grid_index/readiness/record", response_model=dict)
def grid_record_readiness(payload: PartitionPayload) -> dict[str, Any]:
    """把分区输入就绪状态登记到发布台账。"""
    return _handle(grid_service.record_input_readiness, payload.batch_key)


@router.post("/grid_index/publish", response_model=dict)
def grid_publish(payload: GridPublishPayload) -> dict[str, Any]:
    """发布当前"格 + 点"整套输入为分区的一个完整配对。"""
    return _handle(grid_service.publish_grid_index, payload.batch_key,
                   version=payload.version, note=payload.note)


@router.post("/grid_index/withdraw", response_model=dict)
def grid_withdraw(payload: PartitionPayload) -> dict[str, Any]:
    """撤回某个已发布配对；文件仍可读但不再放行常规取数。"""
    return _handle(grid_service.withdraw_grid_index, payload.batch_key,
                   version=payload.version, reason=payload.reason or "")


@router.get("/grid_index/pair/{batch_key}", response_model=dict)
def grid_pair(batch_key: str, version: str | None = None) -> dict[str, Any]:
    """查看一份已发布配对的格、点与就绪凭据。"""
    return _handle(grid_service.get_published_pair, batch_key, version)


# -- 热力取数端 --

@router.get("/heatmap_aggregate/eligibility/{batch_key}", response_model=dict)
def heatmap_eligibility(batch_key: str, version: str | None = None) -> dict[str, Any]:
    """常规热力取数看到的资格：文件可读不等于仍可发布。"""
    return _handle(heatmap_service.check_data_eligibility, batch_key, version)


@router.post("/heatmap_aggregate/recompute/start", response_model=dict)
def heatmap_recompute_start(payload: HeatmapRunPayload) -> dict[str, Any]:
    """启动一份尚未确认的重算，固定本次可搭配的格与点。"""
    return _handle(heatmap_service.start_recompute, payload.batch_key,
                   payload.grid_version)


@router.post("/heatmap_aggregate/recompute/run", response_model=dict)
def heatmap_recompute_run(payload: HeatmapRunPayload) -> dict[str, Any]:
    """执行未确认重算。"""
    return _handle(heatmap_service.run_recompute, payload.batch_key,
                   payload.grid_version, payload.time_bucket_sec, payload.grid_level)


@router.post("/heatmap_aggregate/recompute/confirm", response_model=dict)
def heatmap_recompute_confirm(payload: HeatmapRunPayload) -> dict[str, Any]:
    """确认重算并登记一次交接。"""
    return _handle(heatmap_service.confirm_recompute, payload.batch_key,
                   payload.grid_version, payload.time_bucket_sec, payload.grid_level)


@router.get("/heatmap_aggregate/handover/{batch_key}", response_model=dict)
def heatmap_handover(batch_key: str, handover_id: str | None = None) -> dict[str, Any]:
    """查看已交接记录（默认最近一次）。"""
    handover = _handle(heatmap_service.get_handover, batch_key, handover_id)
    if handover is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_key} 没有交接记录")
    return handover


@router.post("/heatmap_aggregate/handover/replay", response_model=dict)
def heatmap_handover_replay(payload: HandoverReplayPayload) -> dict[str, Any]:
    """按交接时所用的格与点复盘，并报告是否复现原值。"""
    return _handle(heatmap_service.replay_handover, payload.batch_key,
                   payload.handover_id)
