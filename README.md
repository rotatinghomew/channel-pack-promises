# 多渠道商品规格与销售承诺后端

这是一个仅依赖 Python 3.11 标准库、可反复接入的后端。它把四类事实严格分开：

1. **门店上报**：经营权、门店身份 `local`、条码、包装件数和半开生效区间。
2. **运营校对**：渠道关联候选、整批更正、冲突认领、明确确认、暂停/恢复。
3. **渠道读写**：渠道商品/发布号 `push`、发布回执、对外发布适配器。
4. **接单交付**：按顾客所见 `push` 固化订单承诺、规格明确的换货/取消、顾客确认、配货与交付事实。

商品名和条码**不会**凭相似性决定订单内容。跨门店、跨时间、跨渠道的相同条码都只作为证据；最终必须落到精确的 `(store, local)` 与 `(channel, listing, push)` 标识链。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

## 载入样本并启动 HTTP 服务

```bash
python3 -m exchange load-samples --samples samples --state /tmp/exchange-state.json
EXCHANGE_STATE=/tmp/exchange-state.json python3 app.py
# 或
python3 -m exchange serve --host 127.0.0.1 --port 8080 --state /tmp/exchange-state.json
```

服务提供 JSON API；使用 `--state` 时，成功写入会原子保存到 JSON 状态文件，重复提交按业务键或内容哈希幂等处理。

也可以直接在 Python 中接入：

```python
from exchange import ExchangeService

svc = ExchangeService()
svc.ingest_samples("samples")

print(svc.resolve_catalog(
    "s1", "2026-11-27T20:00:00+08:00", barcode="S119"
))
print(svc.preview_channel("ch-a", at="2026-11-27T19:45:00+08:00"))
print(svc.trace_order("order-18"))
```

## 核心业务规则

### 1. 时间、门店和条码都是精确上下文

所有区间均为半开区间 `[from, until)`，时间必须是带偏移的 ISO 8601。样本中：

- 旧包装 `case-old`：`until=20:00`
- 新包装 `case-new`：`from=20:00`

因此 `20:00` 整归新包装，`19:59:59...` 仍归旧包装。`s2` 的同一条码 `S119` 则独立对应 `case-other`，不会与 `s1` 合并。

查询：

```http
GET /v1/explain?store=s1&barcode=S119&at=2026-11-27T18:50:00%2B08:00
GET /v1/explain?channel=ch-a&listing=A7&push=tx-a1&at=2026-11-27T20:09:00%2B08:00
```

### 2. 候选关联和渠道发布明确分离

- `GET /v1/candidates`：返回运营/资料侧的候选或已校对关联，记录类型为 `candidate_association`。
- `GET /v1/publications`：返回渠道回执，记录类型为 `channel_publication`。
- `POST /v1/publications`：通过可选渠道适配器对外发布候选。
- `GET /v1/preview`：合并两类证据并给出渠道预览状态。

预览状态包括：

- `sellable_now`：当前处于有效区间，关联、门店包装和可见发布回执均无冲突。
- `reservable_future`：未来生效且规格链可验证，可预约。
- `under_review`：存在重叠包装、缺字段、规格不一致或发布回执未确认。
- `paused`：有权运营人员已暂停。
- `unavailable` / `ended`：明确解除、渠道不可见或已过有效期。

### 3. 整批更正、缺字段和显式清空

`POST /v1/amendments` 按 `batch` 幂等接收整批更正：

- `changes` 中未出现的字段表示“本批无意见”，保留旧值。
- `clear: [{"field": "local", ...}]` 才会显式移除关系字段。
- 若更正造成同一 `(channel, listing)` 在重叠时间存在两种不同的有效包装，响应会包含受影响的渠道、商品、门店、候选 `push` 和重叠窗口。
- 不在受影响作用域内的门店/渠道仍保留在 `unaffected_sellable` 中，可继续销售。

样本第一批 `m-11` 把 `tx-a2.from` 提前到 `19:30`，与 `tx-a1` 的有效区间形成 `19:30–20:00` 冲突；第二批 `m-12` 未提 `from`，所以 `19:30` 保留，同时显式清空 `local`，结束 `tx-a2` 的可用关联。

### 4. 冲突由有权负责该门店的人员处理

经营权记录也是半开区间。显式动作通过 `X-Operator-Id` 或 body 中的 `operator` 指定人员：

```http
POST /v1/conflicts/channel-listing:ch-a:A7/claim
POST /v1/conflicts/channel-listing:ch-a:A7/confirm
POST /v1/pauses
POST /v1/resumptions
```

新运营方只能确认其接管时点之后的窗口；尝试把关联确认窗口回填到经营权开始前会得到 `backdated_operator_action`。

暂停是持久销售开关。渠道回执只是证据，因此：

- 重复的旧 `visible` 回执不会恢复已暂停商品；
- 回执不会创建或改写商品关联；
- `ch-a` 的暂停不会影响 `ch-b` 上独立确认的 `A7`。

### 5. 订单固化顾客所见的承诺

`POST /v1/purchases` 按 `order` 幂等接单。订单通过精确的 `(channel, listing, push)` 找到承诺，并保存不可变快照：

- `store`
- `local`
- 承诺的 `pack`
- 购买份数 `units`
- 数量含义，例如 `2 份 × 3 件/份 = 6 件`
- listing revision、amendment、catalog evidence 等来源链
- 接单时经营权归属 `managing_operator_id`

之后条码改绑新规格、更正到达或运营方更换，都不会覆盖 `accepted_commitment`。

样本 `order-18` 在 `20:09` 购买了旧 `push=tx-a1`。即使此时 `S119` 在 `s1` 已唯一对应新的 2 件装，直接按条码配货仍会被拒绝：

```text
same_barcode_different_spec
required: case-old / 3 件
actual:   case-new / 2 件
```

### 6. 换货、取消和配货交付

门店只能由接单时归属的运营方处理原订单：

```http
POST /v1/orders/{id}/replacements
POST /v1/orders/{id}/cancellation-requests
POST /v1/orders/{id}/customer-decision
POST /v1/orders/{id}/fulfillment
POST /v1/orders/{id}/delivery
POST /v1/orders/{id}/mismatch-reports
GET  /v1/orders/{id}/trace
```

换货必须给出精确的新 `local`、`pack` 和 `units`；仅给条码或相似商品名不够。同一条码的新规格可以作为**显式换货候选**，但必须由订单上的顾客确认。原始承诺仍保留在追溯结果中，确认后的方案位于 `agreed_resolution`。

合作门店转营时：

- 接管前接受的订单仍由原运营方处置；
- 新运营方对这些订单的配货、换货、取消请求会得到 `order_operator_transition_forbidden`；
- 新运营方只能处理接管时点之后的关联、暂停和销售；
- 配货/交付事实只追加，不提供覆盖修改。

## 主要 HTTP 路由

| 用途 | 方法与路径 |
| --- | --- |
| 健康检查 | `GET /health` |
| 单品/承诺解释 | `GET /v1/explain` |
| 渠道预览 | `GET /v1/preview` |
| 候选关联 | `GET /v1/candidates` |
| 渠道回执 | `GET /v1/publications` |
| 对外发布记录 | `GET /v1/outbound-publications` |
| 冲突列表 | `GET /v1/conflicts` |
| 接收任一类资料 | `POST /v1/ingest/{kind}`，kind 可取 `stores`、`catalog`、`listings`、`amendments`、`channel_receipts`、`purchases` |
| 整批更正 | `POST /v1/amendments` |
| 渠道回执 | `POST /v1/receipts` |
| 对外发布 | `POST /v1/publications` |
| 暂停/恢复 | `POST /v1/pauses`、`POST /v1/resumptions` |
| 认领冲突 | `POST /v1/conflicts/{conflict_id}/claim` |
| 确认关联 | `POST /v1/conflicts/{conflict_id}/confirm` |
| 接单 | `POST /v1/purchases` |
| 订单列表/详情/追溯 | `GET /v1/orders`、`GET /v1/orders/{id}`、`GET /v1/orders/{id}/trace` |
| 配货 | `POST /v1/orders/{id}/fulfillment` |
| 交付 | `POST /v1/orders/{id}/delivery` |
| 换货申请 | `POST /v1/orders/{id}/replacements` |
| 取消申请 | `POST /v1/orders/{id}/cancellation-requests` |
| 顾客确认 | `POST /v1/orders/{id}/customer-decision` |
| 规格不符记录 | `POST /v1/orders/{id}/mismatch-reports` |

## 目录结构

```text
app.py                  # python app.py 入口
main.py                 # python main.py ... 入口
solution.py             # 常见接入名兼容别名
exchange/
  service.py            # 核心领域服务与幂等状态
  httpapi.py            # 标准库 JSON HTTP 适配
  channels.py           # 渠道网关契约和内存适配器
  errors.py             # 稳定错误码
  timeutil.py           # ISO 时间与半开区间
samples/                # 合成交换资料
tests/                  # 样例契约和完整业务场景测试
```

真实连锁渠道可实现 `exchange.channels.ChannelGateway` 的 `fetch_listings` / `publish_listing`，加入签名、重试和渠道侧幂等号；核心领域规则无需改动。
