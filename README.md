# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限和审计。所有写操作在`BEGIN IMMEDIATE`事务内基于乐观版本号完成：并发提交测量/重分类时旧版本收到409冲突，望远镜与观测队的时段冲突在锁内复查，杜绝同一时段双开。

## 观测状态与复核

观测申请状态：`requested`（等待调度）→ `scheduled`（已占时段）→ `completed`；任一步可`withdraw`。

- 候选事件优先级因补测量或重分类变化时，所有未开始的观测（`requested`/`scheduled`）立即进入`review_pending`，并写`invalidate`审计。
- 复核期间由`scheduled`转入的观测仍占用望远镜和观测队时段（`holds_slot=true`），其他申请无法抢入。
- `confirm_review`（coordinator/supervisor/admin）释放时段，申请回到`requested`，等待队列在同一事务内按候选最新优先级（其次按创建时间）重新竞争：同一望远镜或同一观测队同一时段只放行一个，落选申请保留`requested`可重试，并记录`schedule_deferred`审计。
- `withdraw`同样释放时段并触发重新竞争；已完成的观测不受优先级变化影响。
- 调度冲突或版本冲突时申请数据原样保留，可携带最新`expected_version`重试。
- 可手动触发某台望远镜的重新竞争：`POST /api/telescopes/<id>/dispatch`。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并和排程冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
