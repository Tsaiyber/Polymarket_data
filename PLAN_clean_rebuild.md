# Polymarket Crypto Up/Down 数据管道重建规划

## 一、市场背景

### 目标市场

只维护 **BTC/ETH/SOL/XRP/BNB/DOGE** 的涨跌预测市场：

| 维度 | 内容 |
|---|---|
| 资产 | BTC, ETH, SOL, XRP, BNB, DOGE |
| 周期 | 5min, 15min, hourly, 240min, daily（及其变体） |
| 合约来源 | CTF_EXCHANGE + NEGRISK_CTF_EXCHANGE（Polygon） |
| 数据起点 | 区块 **69,002,025**（2025-03-13 UTC） |
| 当前链头 | 区块 ~84,948,227（2026-04-06） |
| Token 数量 | ~351,556 个（来自 175,778 个市场） |

### 为何重建

现有数据库是全市场数据（94GB），UP/DOWN 市场数据从 2025-03 才开始，
只需约 15M 个区块的扫描范围，新库预计 **5-15GB**，架构更清晰、维护成本更低。

---

## 二、架构设计

### 总体原则

> **在源头过滤，而非事后清洗。**
> fetch 时只写入目标 token 的事件，orderfilled.parquet 天然干净。

### 数据流

```
Polygon RPC (Alchemy)
        │
        │  eth_getLogs  [CTF_EXCHANGE, NEGRISK_CTF_EXCHANGE]
        │  过滤条件：maker_asset_id 或 taker_asset_id ∈ updown_tokens
        ▼
orderfilled.parquet          ← 只含 up/down 市场事件，预计 5-15 GB
        │
        │  process-historical（分批，流式写入）
        ▼
trades.parquet               ← 每条交易：价格/金额/方向/市场信息
        │
        ├──► quant.parquet   ← 量化用：时间序列，按市场+时间聚合
        └──► users.parquet   ← 用户维度：地址/交易次数/盈亏
```

### 状态管理（state.json）

```json
{
  "fetch_onchain": {
    "last_block": 84948227,        // 下次增量从这里继续
    "updated_at": "..."
  },
  "process_historical": {
    "last_total_rows": 12000000,   // orderfilled 已处理行数，增量基准
    "resume_batch": null,          // 非 null = 有中断未完成的批次
    "resume_session_ts": null,
    "updated_at": "..."
  }
}
```

### 容错机制

| 场景 | 处理方式 |
|---|---|
| fetch 中断 | `state.last_block` 记录断点，`--continue` 续传 |
| fetch 失败区块 | 写入 `pending_blocks`，下次 sync 自动重试（自适应批次 10→5→1） |
| process 中断 | `state.resume_batch` 记录断点，`--continue` 续传 |
| orderfilled merge | 流式写入临时文件后原子替换，不整体读入内存 |
| session 文件残留 | sync Step 4 自动扫描合并所有 `*_session_*.parquet` |

---

## 三、实现

### 3.1 项目结构

```
polymarket_updown/
├── pyproject.toml
├── .env                        # ALCHEMY_API_KEY
├── data/
│   ├── updown_markets.parquet  # 从旧项目复制过来
│   ├── state.json
│   ├── dataset/
│   │   └── orderfilled.parquet
│   └── data_clean/
│       ├── trades.parquet
│       ├── quant.parquet
│       └── users.parquet
└── polymarket/
    ├── config.py
    ├── cli/
    │   └── main.py
    ├── fetchers/
    │   └── rpc.py
    └── processors/
        └── trades.py
```

### 3.2 核心改动：fetch 时过滤

现有代码在 `cmd_fetch_onchain` 里把所有事件写入 session 文件，过滤在 process 时发生。
新架构改为写入前过滤：

```python
# cmd_fetch_onchain 内，写 session 文件之前加：
crypto_tokens = load_updown_tokens()   # 从 updown_markets.parquet 读取所有 token1/token2

if crypto_tokens:
    formatted = [
        e for e in formatted
        if e.get('maker_asset_id') in crypto_tokens
        or e.get('taker_asset_id') in crypto_tokens
    ]
```

`load_updown_tokens()` 返回 `set[str]`，从 `updown_markets.parquet` 的 token1/token2 列构建。

### 3.3 全历史初始化（一次性）

```bash
# 需要 Alchemy API Key（公共 RPC 每批10块，太慢）
# 起始块 = updown 市场最早区块 69,002,025
# 当前链头 ≈ 84,948,227
# 总计 ~16M 个区块，Alchemy 2000块/批 ≈ 8000次请求，约 30-60 分钟

uv run polymarket fetch-onchain \
    --range 69000000 84948227 \
    --alchemy
```

Alchemy 需要把 `BLOCKS_PER_BATCH` 提升到 2000，或加 `--batch-size` 参数。

### 3.4 全量 process（初始化后跑一次）

```bash
uv run polymarket process-historical
# continue_from=False → 从头覆盖，直接写主文件
# 预计数据量小（5-15GB），速度快
```

### 3.5 日常 sync（增量，每天跑）

```bash
uv run polymarket sync --alchemy
```

内部流程：
```
Step 1  fetch-markets + update-markets + 重建 updown_markets.parquet
Step 2  重试 pending_blocks
Step 3  fetch-onchain --continue（从 last_block 拉新区块，写前过滤）
Step 4  merge session 文件 → orderfilled.parquet（流式，带去重）
Step 5  process-historical --continue（增量，只处理新增行）
        → session 文件 → 流式合并到 trades/quant/users
```

### 3.6 需要调整的配置参数

```python
# config.py
BLOCKS_PER_BATCH = 2000        # Alchemy 支持，从 10 提升
REQUEST_DELAY = 0.05           # Alchemy 限速更宽松，从 0.2 降低

# 起始区块（updown 市场数据起点）
UPDOWN_START_BLOCK = 69_000_000
```

---

## 四、验证

### 4.1 数据完整性

```python
# 验证 orderfilled 只含 updown token
import pandas as pd, pyarrow.parquet as pq

updown = pd.read_parquet('data/updown_markets.parquet', columns=['token1','token2'])
tokens = set(updown.token1.dropna()) | set(updown.token2.dropna())

pf = pq.ParquetFile('data/dataset/orderfilled.parquet')
for batch in pf.iter_batches(batch_size=1_000_000):
    df = batch.to_pandas()
    # 所有行的 maker 或 taker 必须在 tokens 中
    bad = df[~(df.maker_asset_id.isin(tokens) | df.taker_asset_id.isin(tokens))]
    assert len(bad) == 0, f"发现 {len(bad)} 条非目标 token 数据"

print("orderfilled 过滤验证通过")
```

### 4.2 区块连续性

```python
import json
import pyarrow.parquet as pq
import pandas as pd

# 1. state.json 检查
state = json.load(open('data/state.json'))
last_block = state['fetch_onchain']['last_block']
pending = state.get('pending_blocks', [])
print(f"last_block: {last_block:,}")
print(f"pending_blocks: {len(pending)} 个未解决区块")
if pending:
    for p in pending:
        print(f"  {p['start']}-{p['end']}  attempts={p['attempts']}")

# 2. orderfilled 区块空洞检测
# 将 block_number 按批次读取，找出相邻区块差值 > 阈值的空洞
pf = pq.ParquetFile('data/dataset/orderfilled.parquet')
prev_max = None
GAP_THRESHOLD = 50000  # 差值超过 5万块（约1天）视为空洞

for batch in pf.iter_batches(batch_size=2_000_000, columns=['block_number']):
    blocks = batch.column('block_number').to_pylist()
    batch_min = min(blocks)
    batch_max = max(blocks)

    if prev_max is not None and (batch_min - prev_max) > GAP_THRESHOLD:
        print(f"  ⚠ 空洞: {prev_max:,} → {batch_min:,}，缺失 {batch_min - prev_max:,} 个区块")

    prev_max = batch_max

print(f"区块范围: {prev_max:,}（最大）")
print("连续性检查完成")
```

### 4.3 数量级验证

基于旧库数据（799M行全市场，其中 updown 约占 X%）估算期望值：

```
旧库：orderfilled 约 6% 为 updown（按干跑结果 62.7% 为加密全市场，updown 是其子集）
起始区块 69M，旧库从 35.9M 开始，updown 只有后半段
预期新库 orderfilled 行数：100M ～ 200M 行
```

如果新库结果远低于 50M 或高于 500M，需要检查过滤逻辑。

### 4.4 增量幂等性测试

```bash
# 第一次 sync
uv run polymarket sync --alchemy
# 记录 orderfilled 行数 N1

# 第二次 sync（无新数据时）
uv run polymarket sync --alchemy
# 记录 orderfilled 行数 N2

# 断言 N1 == N2（或 N2 略大，仅新增真实新区块）
```

### 4.5 价格合理性抽样

```python
import pandas as pd

trades = pd.read_parquet('data/data_clean/trades.parquet')
# 价格应在 [0.01, 0.99]
assert trades.price.between(0.001, 0.999).mean() > 0.99, "价格分布异常"
# 抽查某个已知市场
btc = trades[trades.question.str.contains('BTC', na=False)]
print(btc[['datetime','price','usd_amount','question']].tail(10))
```

---

## 五、项目路径

新项目根目录：`../Polymarket_data_infra`

旧项目根目录：`../Polymarket_data`

### 可直接复用的数据文件

| 文件 | 旧项目路径 | 说明 |
|---|---|---|
| updown_markets.parquet | `../Polymarket_data/data/updown_markets.parquet` | 175,778 个目标市场，含 asset/period 列 |
| markets.parquet | `../Polymarket_data/data/dataset/markets.parquet` | 全市场 token→market_id 映射 |

### 可直接复用的代码模块

| 模块 | 旧项目路径 | 说明 |
|---|---|---|
| RPC 拉取 + 解码 | `../Polymarket_data/polymarket/fetchers/rpc.py` | LogFetcher, EventDecoder，含自适应重试 |
| 流式合并 parquet | `../Polymarket_data/polymarket/tools/merge_parquet.py` | 滑动窗口去重，500K行一批流式写入 |
| trades 提取 | `../Polymarket_data/polymarket/processors/trades.py` | OrderFilled → trades DataFrame |
| 数据清洗 | `../Polymarket_data/polymarket/processors/clean.py` | trades → quant/users |
| Gamma API | `../Polymarket_data/polymarket/api/gamma.py` | 拉取市场元数据 |
| 配置 | `../Polymarket_data/polymarket/config.py` | 合约地址、事件签名、路径常量 |
| CLI 入口 | `../Polymarket_data/polymarket/cli/main.py` | 所有命令实现，sync/process-historical 逻辑 |

### 需要新增/修改的部分（旧项目没有的）

1. **`cmd_fetch_onchain` 写入前加 token 过滤**（旧项目在 process 时才过滤）
2. **`BLOCKS_PER_BATCH = 2000`**（旧项目是 10，新项目面向 Alchemy）
3. **`UPDOWN_START_BLOCK = 69_000_000`** 作为全历史初始化的起始块

---

## 六、迁移清单

- [ ] 复制 `data/updown_markets.parquet` 到新项目
- [ ] 复制 `data/dataset/markets.parquet` 到新项目（token 映射需要）
- [ ] 配置 `.env`：`ALCHEMY_API_KEY=xxx`
- [ ] 修改 `BLOCKS_PER_BATCH = 2000`
- [ ] 在 `cmd_fetch_onchain` 写入前加 token 过滤
- [ ] 初始化：`fetch-onchain --range 69000000 <head> --alchemy`
- [ ] 初始化：`process-historical`
- [ ] 验证数据量和价格合理性
- [ ] 设置定时任务：`sync --alchemy`（每日或每小时）
