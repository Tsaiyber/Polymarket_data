"""
过滤 orderfilled.parquet，只保留加密市场的交易。

原理：
  1. 从 markets.parquet 提取加密市场的所有 token1/token2
  2. 流式读取 orderfilled.parquet（批次处理，不占满内存）
  3. 保留 maker_asset_id 或 taker_asset_id 在加密 token 集合中的行
  4. 写入临时文件后替换原文件

用法：
    uv run python -m polymarket.tools.filter_crypto_orderfilled
    uv run python -m polymarket.tools.filter_crypto_orderfilled --dry-run
"""

import argparse
import logging
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pandas as pd

from ..config import DATASET_DIR, CRYPTO_MARKET_IDS_FILE, DATA_DIR

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

ORDERFILLED_FILE = DATASET_DIR / 'orderfilled.parquet'
MARKETS_FILE = DATASET_DIR / 'markets.parquet'
BATCH_SIZE = 2_000_000


def load_crypto_tokens() -> set:
    """从 markets.parquet 提取加密市场的所有 token ID。"""
    if not CRYPTO_MARKET_IDS_FILE.exists():
        raise FileNotFoundError(
            f"crypto_market_ids.txt 不存在: {CRYPTO_MARKET_IDS_FILE}\n"
            "请先运行: uv run polymarket build-crypto-filter"
        )
    if not MARKETS_FILE.exists():
        raise FileNotFoundError(f"markets.parquet 不存在: {MARKETS_FILE}")

    with open(CRYPTO_MARKET_IDS_FILE) as f:
        crypto_ids = {line.strip() for line in f if line.strip()}
    logger.info(f"加载 {len(crypto_ids):,} 个加密市场 ID")

    markets = pd.read_parquet(MARKETS_FILE, columns=['id', 'token1', 'token2'])
    crypto_markets = markets[markets['id'].isin(crypto_ids)]
    logger.info(f"匹配到 {len(crypto_markets):,} 个加密市场（共 {len(markets):,} 个）")

    tokens = set()
    tokens.update(crypto_markets['token1'].dropna().tolist())
    tokens.update(crypto_markets['token2'].dropna().tolist())
    tokens.discard('')
    logger.info(f"加密市场 token 集合大小: {len(tokens):,}")
    return tokens


def filter_orderfilled(dry_run: bool = False) -> None:
    if not ORDERFILLED_FILE.exists():
        raise FileNotFoundError(f"orderfilled.parquet 不存在: {ORDERFILLED_FILE}")

    crypto_tokens = load_crypto_tokens()
    token_array = pa.array(list(crypto_tokens), type=pa.string())

    pf = pq.ParquetFile(ORDERFILLED_FILE)
    total_rows = pf.metadata.num_rows
    logger.info(f"orderfilled.parquet: {total_rows:,} 行")

    if dry_run:
        # 只统计匹配行数，不写文件
        kept = 0
        for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE)):
            mask = pc.or_(
                pc.is_in(batch.column('maker_asset_id'), value_set=token_array),
                pc.is_in(batch.column('taker_asset_id'), value_set=token_array),
            )
            kept += pc.sum(mask).as_py()
            logger.info(f"  批次 {i+1}: 已扫描 {min((i+1)*BATCH_SIZE, total_rows):,} 行，保留 {kept:,} 行")
        logger.info(f"[dry-run] 预计保留 {kept:,} / {total_rows:,} 行 ({kept/total_rows*100:.1f}%)")
        return

    tmp_file = ORDERFILLED_FILE.with_suffix('.crypto_tmp.parquet')
    writer = None
    kept = 0

    try:
        for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE)):
            mask = pc.or_(
                pc.is_in(batch.column('maker_asset_id'), value_set=token_array),
                pc.is_in(batch.column('taker_asset_id'), value_set=token_array),
            )
            filtered = batch.filter(mask)
            kept += len(filtered)

            if writer is None:
                writer = pq.ParquetWriter(tmp_file, filtered.schema, compression='snappy')
            writer.write_batch(filtered)

            scanned = min((i+1)*BATCH_SIZE, total_rows)
            logger.info(f"  批次 {i+1}: {scanned:,}/{total_rows:,} 行已处理，保留 {kept:,} 行")

        if writer:
            writer.close()

        logger.info(f"过滤完成：{kept:,} / {total_rows:,} 行保留 ({kept/total_rows*100:.1f}%)")

        # 备份原文件后替换
        backup = ORDERFILLED_FILE.with_suffix('.full_backup.parquet')
        logger.info(f"备份原文件 → {backup.name}")
        shutil.move(str(ORDERFILLED_FILE), str(backup))
        shutil.move(str(tmp_file), str(ORDERFILLED_FILE))
        logger.info(f"✓ orderfilled.parquet 已替换为加密专属版本")
        logger.info(f"  原始备份保留在: {backup.name}（确认无误后可手动删除）")

    except Exception:
        if writer:
            writer.close()
        if tmp_file.exists():
            tmp_file.unlink()
        raise


def main():
    parser = argparse.ArgumentParser(description='过滤 orderfilled.parquet 为加密市场专属')
    parser.add_argument('--dry-run', action='store_true', help='只统计，不写文件')
    args = parser.parse_args()
    filter_orderfilled(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
