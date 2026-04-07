"""
交易数据提取

只需要从两个交易所合约提取 OrderFilled 事件
支持 market_id/condition_id 关联和缺失 token 补全

参考 poly_data-main 的处理逻辑:
1. 找出非USDC的 asset_id (maker_asset_id 或 taker_asset_id 中不为"0"的)
2. 用非USDC的 asset_id join markets 表获取 market_id, condition_id, side(token1/token2)
3. 金额除以 10^6 (USDC 是 6 位小数)
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

import pandas as pd

from ..config import MARKETS_FILE, CRYPTO_MARKET_IDS_FILE

logger = logging.getLogger(__name__)

# USDC 精度
USDC_DECIMALS = 10 ** 6

# 两个交易所合约名
EXCHANGE_CONTRACTS = {'CTF_EXCHANGE', 'NEGRISK_CTF_EXCHANGE'}


def extract_trades(
    events: List[Dict[str, Any]],
    token_mapping: Optional[Dict[str, Dict]] = None,
    crypto_only: bool = False,
) -> pd.DataFrame:
    """
    从事件中提取交易数据

    只保留:
    - 两个交易所合约 (CTF_EXCHANGE, NEGRISK_CTF_EXCHANGE) 的
    - OrderFilled 事件

    Args:
        events: 解码后的事件列表
        token_mapping: token_id -> {market_id, condition_id, side, question} 映射
        crypto_only: 为 True 时跳过无法匹配到 market_id 的交易（即非加密市场）
    """
    trades = []

    for event in events:
        # 如果有 event_name 字段，检查是否为 OrderFilled
        # （orderfilled.parquet 已经是过滤后的数据，可能没有这个字段）
        event_name = event.get('event_name')
        if event_name is not None and event_name != 'OrderFilled':
            continue

        # 只要两个交易所合约
        contract = event.get('contract', '')
        if contract not in EXCHANGE_CONTRACTS:
            continue

        # 解析交易
        trade = _parse_order_filled(event, token_mapping)
        if trade:
            # crypto_only 模式：跳过未匹配到市场的交易（市场不在加密过滤集内）
            if crypto_only and not trade.get('market_id'):
                continue
            trades.append(trade)

    logger.info(f"提取 {len(trades)} 条交易{'（仅加密市场）' if crypto_only else ''}")

    if not trades:
        return pd.DataFrame()

    return pd.DataFrame(trades)


def _parse_order_filled(event: Dict, token_mapping: Optional[Dict[str, Dict]] = None) -> Optional[Dict]:
    """解析 OrderFilled 事件"""
    maker_id = str(event.get('maker_asset_id', '0'))
    taker_id = str(event.get('taker_asset_id', '0'))

    # 金额可能是字符串，需要转换
    try:
        maker_amt = float(event.get('maker_amount_filled', 0))
        taker_amt = float(event.get('taker_amount_filled', 0))
    except (ValueError, TypeError):
        maker_amt = 0
        taker_amt = 0

    # 找出非USDC的 asset_id (关键！)
    # USDC 的 asset_id 是 "0"
    if maker_id != '0':
        nonusdc_asset_id = maker_id
        usdc_amt = taker_amt
        token_amt = maker_amt
        # maker 提供 token，taker 提供 USDC -> maker 卖，taker 买
        maker_dir, taker_dir = 'SELL', 'BUY'
        maker_asset = 'token'  # 待填充为 token1/token2
        taker_asset = 'USDC'
    elif taker_id != '0':
        nonusdc_asset_id = taker_id
        usdc_amt = maker_amt
        token_amt = taker_amt
        # maker 提供 USDC，taker 提供 token -> maker 买，taker 卖
        maker_dir, taker_dir = 'BUY', 'SELL'
        maker_asset = 'USDC'
        taker_asset = 'token'  # 待填充为 token1/token2
    else:
        # 两边都是 0，跳过
        return None

    # 金额除以 10^6 (USDC 精度)
    usdc_amt_normalized = usdc_amt / USDC_DECIMALS
    token_amt_normalized = token_amt / USDC_DECIMALS

    # 计算价格
    price = usdc_amt_normalized / token_amt_normalized if token_amt_normalized > 0 else 0

    # 关联 market 信息
    market_id = ''
    condition_id = ''
    side = ''  # token1 或 token2
    question = ''
    event_id = ''
    event_slug = ''
    event_title = ''

    if token_mapping and nonusdc_asset_id:
        market_info = token_mapping.get(nonusdc_asset_id, {})
        market_id = str(market_info.get('market_id', ''))
        condition_id = str(market_info.get('condition_id', ''))
        side = str(market_info.get('side', ''))
        question = str(market_info.get('question', ''))
        event_id = str(market_info.get('event_id', ''))
        event_slug = str(market_info.get('event_slug', ''))
        event_title = str(market_info.get('event_title', ''))

        # 更新 asset 标签为 token1/token2
        if side:
            if maker_asset == 'token':
                maker_asset = side
            if taker_asset == 'token':
                taker_asset = side

    return {
        'timestamp': event.get('timestamp'),
        'datetime': event.get('datetime', ''),
        'block_number': event.get('block_number'),
        'transaction_hash': event.get('transaction_hash'),
        'contract': event.get('contract', ''),
        # Event 信息
        'event_id': event_id,
        'event_slug': event_slug,
        'event_title': event_title,
        # Market 信息
        'market_id': market_id,
        'condition_id': condition_id,
        'question': question,
        'nonusdc_side': side,  # token1 或 token2 (和 poly_data-main 一致)
        # 交易双方
        'maker': event.get('maker'),
        'taker': event.get('taker'),
        'maker_asset': maker_asset,
        'taker_asset': taker_asset,
        'maker_direction': maker_dir,
        'taker_direction': taker_dir,
        # 金额 (已标准化，除以10^6)
        'price': round(price, 6),
        'usd_amount': round(usdc_amt_normalized, 2),
        'token_amount': round(token_amt_normalized, 2),
        # 原始数据
        'asset_id': nonusdc_asset_id,
        'order_hash': event.get('order_hash', '')
    }


def load_crypto_market_ids(crypto_ids_file=None) -> Optional[Set[str]]:
    """
    读取加密市场 ID 集合。文件不存在时返回 None（不过滤）。
    """
    if crypto_ids_file is None:
        crypto_ids_file = CRYPTO_MARKET_IDS_FILE
    if not Path(crypto_ids_file).exists():
        return None
    with open(crypto_ids_file) as f:
        ids = {line.strip() for line in f if line.strip()}
    logger.info(f"加密市场过滤已启用：{len(ids):,} 个市场")
    return ids


def load_token_mapping(markets_file=None, crypto_ids: Optional[Set[str]] = None) -> Dict[str, Dict]:
    """
    从 markets parquet 加载 token 映射

    Args:
        markets_file: markets.parquet 路径
        crypto_ids: 如果提供，只加载这些 market_id 的 token（加密货币过滤）

    返回: token_id -> {market_id, condition_id, side, question}
    """
    if markets_file is None:
        markets_file = MARKETS_FILE

    if not markets_file.exists():
        logger.warning(f"市场文件不存在: {markets_file}")
        return {}

    try:
        # 用 pyarrow 读取，避免 pandas 兼容性问题
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(markets_file)
        df = pf.read().to_pandas()
        mapping = {}

        for _, row in df.iterrows():
            if crypto_ids is not None and str(row.get('id', '')) not in crypto_ids:
                continue
            market_id = str(row.get('id', ''))
            condition_id = str(row.get('condition_id', ''))
            question = str(row.get('question', ''))[:100]  # 截断过长的问题
            token1 = str(row.get('token1', ''))
            token2 = str(row.get('token2', ''))
            # Event 信息
            event_id = str(row.get('event_id', ''))
            event_slug = str(row.get('event_slug', ''))
            event_title = str(row.get('event_title', ''))[:100]

            if token1:
                mapping[token1] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token1',
                    'question': question,
                    'event_id': event_id,
                    'event_slug': event_slug,
                    'event_title': event_title,
                }
            if token2:
                mapping[token2] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token2',
                    'question': question,
                    'event_id': event_id,
                    'event_slug': event_slug,
                    'event_title': event_title,
                }

        logger.info(f"加载 {len(mapping)} 个 token 映射")
        return mapping
    except Exception as e:
        logger.error(f"加载市场文件失败: {e}")
        return {}


def find_missing_tokens(trades_df: pd.DataFrame, token_mapping: Dict[str, Dict]) -> Set[str]:
    """找出交易中没有映射的 token"""
    if trades_df.empty or 'asset_id' not in trades_df.columns:
        return set()

    all_tokens = set(trades_df['asset_id'].dropna().astype(str).unique())
    all_tokens.discard('')
    all_tokens.discard('0')

    missing = all_tokens - set(token_mapping.keys())
    if missing:
        logger.info(f"发现 {len(missing)} 个未映射的 token")
    return missing


def save_preview_csv(trades_df: pd.DataFrame, output_file, n_rows: int = 1000):
    """保存最新 N 条交易为 CSV 预览（保留所有字段）"""
    if trades_df.empty:
        return

    # 保存所有字段，只限制行数
    preview_df = trades_df.tail(n_rows)
    preview_df.to_csv(output_file, index=False)
    logger.info(f"保存 {len(preview_df)} 条交易预览到 {output_file}")


# 保留旧的类接口以兼容
class TradeBuilder:
    """交易构建器 (兼容旧接口)"""

    def __init__(self, token_mapping: Optional[Dict[str, Dict]] = None):
        self.token_mapping = token_mapping

    def build_from_events(self, events: List[Dict]) -> List[Dict]:
        df = extract_trades(events, self.token_mapping)
        return df.to_dict('records') if not df.empty else []

    def to_dataframe(self, trades: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(trades) if trades else pd.DataFrame()


class TokenMapper:
    """Token 映射器"""

    def __init__(self, markets_file=None):
        self.token_map = load_token_mapping(markets_file)

    def get_market(self, token_id: str) -> Optional[Dict]:
        return self.token_map.get(str(token_id))

    def add_markets(self, markets: List[Dict]):
        """添加新市场到映射"""
        for m in markets:
            market_id = str(m.get('id', ''))
            condition_id = str(m.get('condition_id', ''))
            question = str(m.get('question', ''))[:100]

            if m.get('token1'):
                self.token_map[m['token1']] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token1',
                    'question': question
                }
            if m.get('token2'):
                self.token_map[m['token2']] = {
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'side': 'token2',
                    'question': question
                }
