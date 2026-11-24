# 地理空间轨迹与网格聚合分析平台

面向定位点采集、坐标标准化、网格切分、轨迹分段驻留识别、空间关联聚合与区域维度产出的地理空间数据分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 定位源接入 | `geo_source` | 采集批次 | 批次编号、采集终端、定位方式 |
| 坐标标准化 | `coord_transform` | 转换规则 | 规则编号、源坐标系、目标坐标系 |
| 轨迹切分 | `track_segment` | 轨迹分段 | 分段编号、终端编号、起始时间 |
| 驻留识别 | `dwell_detect` | 驻留事件 | 事件编号、分段编号、中心坐标 |
| 网格索引 | `grid_index` | 网格单元 | 网格编号、网格层级、边界范围 |
| 围栏判定 | `geofence_rule` | 电子围栏 | 围栏编号、围栏形状、顶点序列 |
| 空间关联 | `spatial_join` | 关联任务 | 任务编号、左数据集、右数据集 |
| 兴趣点匹配 | `poi_match` | 匹配候选 | 候选编号、驻留事件、兴趣点编号 |
| 区域维度 | `region_dim` | 区域层级 | 区域编号、上级区域、区域名称 |
| 热力聚合 | `heatmap_aggregate` | 聚合窗口 | 窗口编号、网格层级、时间粒度 |
| 起止流量 | `od_matrix` | 流量单元 | 单元编号、起点网格、终点网格 |
| 路径还原 | `route_restore` | 还原路段 | 路段编号、分段编号、路段序列 |
| 密度估算 | `density_estimate` | 密度网格 | 估算编号、网格编号、带宽参数 |
| 轨迹质量 | `geo_quality` | 质量报告 | 报告编号、批次编号、漂移点数 |
| 点位归并 | `dedup_merge` | 归并批次 | 归并编号、批次编号、去重键列 |
| 分析快照 | `geo_snapshot` | 版本快照 | 快照编号、分析口径、网格层级 |
| 历史回补 | `geo_backfill` | 回补任务 | 回补编号、目标数据集、起始日期 |
| 结果导出 | `geo_export` | 导出任务 | 导出编号、快照编号、导出格式 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 空间来源配对（网格索引 ↔ 热力聚合）

网格索引与热力聚合共用 `backend/app/services/spatial_inputs.py` 中的输入解析：
定位点优先读坐标转换结果、缺失回退采集原始点，格与点必须属于同一来源版本。
落格与热点算法仍分别留在两个领域服务里，稳定输入的口径不变。

### 网格发布端

| 能力 | 服务方法 | 接口 |
| --- | --- | --- |
| 输入就绪核对 | `check_input_readiness` | `POST /api/grid_index/readiness` |
| 登记就绪状态 | `record_input_readiness` | `POST /api/grid_index/readiness/record` |
| 发布完整配对（格 + 点 + 就绪凭据） | `publish_grid_index` | `POST /api/grid_index/publish` |
| 撤回已发布配对 | `withdraw_grid_index` | `POST /api/grid_index/withdraw` |
| 查看版本与配对 | `get_publication` / `get_published_pair` | `GET /api/grid_index/versions/{batch_key}`、`GET /api/grid_index/pair/{batch_key}` |

- 生成网格时归档当次输入点快照并登记生成清单；发布登记整份"格 + 点"，
  写入就绪凭据 `readiness_token`。
- 坐标已更新但对应格尚未重建时，分区状态为 `待重建`，发布被拒绝，
  不会仅凭新坐标文件存在就标成可发布。
- 撤回只取消配对资格，归档文件保持可读；常规热力取数不再放行，
  已交接的旧结果仍可按归档配对复盘。

### 热力取数端

| 能力 | 服务方法 | 接口 |
| --- | --- | --- |
| 取数资格核对 | `check_data_eligibility` | `GET /api/heatmap_aggregate/eligibility/{batch_key}` |
| 独立核实分区就绪 | `verify_partition_inputs` | — |
| 启动未确认重算 | `start_recompute` | `POST /api/heatmap_aggregate/recompute/start` |
| 执行未确认重算 | `run_recompute` | `POST /api/heatmap_aggregate/recompute/run` |
| 确认重算并交接 | `confirm_recompute` | `POST /api/heatmap_aggregate/recompute/confirm` |
| 重算待办 | `list_pending_recomputes` | — |
| 交接复盘 | `replay_handover` | `POST /api/heatmap_aggregate/handover/replay` |

- 热力只读取已发布且输入就绪的完整配对；文件仍可读不等于仍可发布。
- 每次重算记录实际采用的格版本、当时采用的点摘要与就绪凭据；
  未确认重算会固定启动时的来源，新版本发布不会改变它。
- 两个已发布版本各自可用，但不能跨版本配对：切换版本必须显式选择。
- 每次交接留档，可按交接时的格与点复盘原值；若按旧版完成交接，
  会为更新的已发布版本留下新的重算待办。

领域：数据工程
子方向：数据分析与数据应用
