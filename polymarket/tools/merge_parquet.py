#!/usr/bin/env python3
"""
合并多个 parquet 文件

nohup python scripts/merge_parquet.py \
  data/dataset/orderfilled.parquet \
  data/dataset/orderfilled_refetched_20251230_055516.parquet \
  -o data/dataset/orderfilled_merged.parquet \
  -y \
  > logs/merge_orderfilled.log 2>&1 &

支持指定输入文件列表和输出文件，按顺序合并
"""
import sys
import argparse
from pathlib import Path
from collections import deque
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def merge_parquet_files(input_files, output_file, dry_run=False, auto_yes=False, dedup=False):
    """
    合并多个 parquet 文件

    Args:
        input_files: 输入文件列表（按顺序）
        output_file: 输出文件路径
        dry_run: 是否只显示信息不实际合并
        auto_yes: 自动确认覆盖
        dedup: 是否去重（基于 transaction_hash + log_index）
    """
    # 验证输入文件
    valid_files = []
    total_rows = 0

    logger.info("=== 检查输入文件 ===")
    for i, file_path in enumerate(input_files, 1):
        p = Path(file_path)
        if not p.exists():
            logger.error(f"[{i}] 文件不存在: {file_path}")
            continue

        try:
            # 只读取元数据，不加载实际数据
            parquet_file = pq.ParquetFile(file_path)
            rows = parquet_file.metadata.num_rows
            size_mb = p.stat().st_size / 1024 / 1024
            total_rows += rows
            valid_files.append(file_path)
            logger.info(f"[{i}] {p.name}: {rows:,} 行, {size_mb:.1f} MB")
        except Exception as e:
            logger.error(f"[{i}] 读取失败 {file_path}: {e}")
            continue

    if not valid_files:
        logger.error("没有有效的输入文件")
        return False

    logger.info(f"\n总计: {len(valid_files)} 个文件, {total_rows:,} 行")

    if dry_run:
        logger.info(f"\n[DRY RUN] 将输出到: {output_file}")
        return True

    # 检查输出文件
    output_path = Path(output_file)
    if output_path.exists():
        logger.warning(f"输出文件已存在，将覆盖: {output_file}")
        if not auto_yes:
            response = input("确认覆盖？(yes/no): ")
            if response.lower() not in ['yes', 'y']:
                logger.info("取消操作")
                return False
        else:
            logger.info("自动确认覆盖（--yes）")

    # 合并文件（流式写入 + 分批读取）
    logger.info(f"\n=== 开始合并（流式写入 + 分批读取）===")
    if dedup:
        logger.info("去重模式已开启（基于 transaction_hash + log_index）")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 去重状态：滑动窗口，保留最近 200,000 行的键（覆盖约 1,200 个区块的重叠）
    DEDUP_COLS = ['transaction_hash', 'log_index']
    DEDUP_WINDOW_SIZE = 200_000
    seen_keys_queue: deque[str] = deque()
    seen_keys: set[str] = set()
    total_duplicates = 0

    def _make_key_series(df):
        """将 transaction_hash + log_index 合成为字符串键"""
        available = [c for c in DEDUP_COLS if c in df.columns]
        if not available:
            return None
        key = df[available[0]].astype(str)
        for col in available[1:]:
            key = key + '|' + df[col].astype(str)
        return key

    def _dedup_batch(table: pa.Table) -> pa.Table:
        """过滤掉已见过的行，并将新行加入滑动窗口"""
        nonlocal total_duplicates
        import pandas as pd
        df = table.to_pandas()
        key_series = _make_key_series(df)
        if key_series is None:
            return table  # 无法去重，跳过

        mask = ~key_series.isin(seen_keys)
        removed = int((~mask).sum())
        if removed:
            total_duplicates += removed
            logger.info(f"  去重: 移除 {removed:,} 条重复行")
            df = df[mask]
            key_series = key_series[mask]

        # 批量更新滑动窗口：先淘汰超出容量的旧键，再批量追加新键
        new_keys = key_series.tolist()
        overflow = len(seen_keys_queue) + len(new_keys) - DEDUP_WINDOW_SIZE
        if overflow > 0:
            for _ in range(min(overflow, len(seen_keys_queue))):
                seen_keys.discard(seen_keys_queue[0])
                seen_keys_queue.popleft()
        seen_keys_queue.extend(new_keys)
        seen_keys.update(new_keys)

        if df.empty:
            return None
        return pa.Table.from_pandas(df, schema=table.schema, preserve_index=False)

    try:
        writer = None
        target_schema = None
        total_rows_written = 0
        batch_size = 500000  # 每批读取 50 万行，减少内存占用

        for i, file_path in enumerate(valid_files, 1):
            logger.info(f"[{i}/{len(valid_files)}] 处理 {Path(file_path).name}")

            parquet_file = pq.ParquetFile(file_path)
            file_rows = 0

            # 第一次创建 writer（需要读取第一批数据获取 schema）
            if writer is None:
                batch_iter = parquet_file.iter_batches(batch_size=batch_size)
                first_batch = next(batch_iter)
                target_schema = first_batch.schema
                writer = pq.ParquetWriter(output_file, target_schema, compression='snappy')

                # 处理第一批
                table = pa.Table.from_batches([first_batch])
                if dedup:
                    table = _dedup_batch(table)
                if table is not None and len(table) > 0:
                    writer.write_table(table)
                    file_rows += len(table)
                    total_rows_written += len(table)
                logger.info(f"  已写入第 1 批: {len(table) if table is not None else 0:,} 行")

                batch_num = 2
                for batch in batch_iter:
                    table = pa.Table.from_batches([batch])
                    if dedup:
                        table = _dedup_batch(table)
                    if table is not None and len(table) > 0:
                        writer.write_table(table)
                        file_rows += len(table)
                        total_rows_written += len(table)
                    logger.info(f"  已写入第 {batch_num} 批: {len(table) if table is not None else 0:,} 行（累计 {total_rows_written:,} 行）")
                    batch_num += 1
            else:
                # 后续文件：统一 schema 后分批写入
                batch_num = 1
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    table = pa.Table.from_batches([batch])
                    try:
                        table = table.cast(target_schema)
                    except Exception:
                        import pandas as pd
                        df = table.to_pandas()
                        for field in target_schema:
                            if field.name in df.columns:
                                if pa.types.is_string(field.type):
                                    df[field.name] = df[field.name].astype(str)
                                elif pa.types.is_integer(field.type):
                                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce').fillna(0).astype('int64')
                        table = pa.Table.from_pandas(df, schema=target_schema, preserve_index=False)

                    if dedup:
                        table = _dedup_batch(table)
                    if table is not None and len(table) > 0:
                        writer.write_table(table)
                        file_rows += len(table)
                        total_rows_written += len(table)
                    if batch_num % 10 == 0 or (table is None or len(table) < batch_size):
                        logger.info(f"  已写入第 {batch_num} 批: {len(table) if table is not None else 0:,} 行（累计 {total_rows_written:,} 行）")
                    batch_num += 1

            logger.info(f"  ✓ 文件完成，共 {file_rows:,} 行")

        # 关闭 writer
        if writer:
            writer.close()

        output_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"\n=== 合并完成 ===")
        logger.info(f"输出文件: {output_file}")
        logger.info(f"总行数: {total_rows_written:,}")
        if dedup and total_duplicates:
            logger.info(f"去重移除: {total_duplicates:,} 条重复行")
        logger.info(f"文件大小: {output_size_mb:.1f} MB")

        return True

    except Exception as e:
        logger.error(f"合并失败: {e}")
        if writer:
            writer.close()
        return False


def main():
    parser = argparse.ArgumentParser(
        description='合并多个 parquet 文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 合并三个文件
  python scripts/merge_parquet.py \\
    data/dataset/orderfilled.parquet \\
    data/dataset/orderfilled_session_*.parquet \\
    data/dataset/orderfilled_refetched_*.parquet \\
    -o data/dataset/orderfilled_merged.parquet

  # 先查看信息不实际合并
  python scripts/merge_parquet.py file1.parquet file2.parquet -o output.parquet --dry-run
        """
    )

    parser.add_argument(
        'input_files',
        nargs='+',
        help='输入 parquet 文件（按顺序合并）'
    )

    parser.add_argument(
        '-o', '--output',
        required=True,
        help='输出文件路径'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='只显示信息，不实际合并'
    )

    parser.add_argument(
        '--log-file',
        help='日志文件路径（用于 nohup 运行）'
    )

    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='自动确认覆盖，不询问（用于 nohup 运行）'
    )

    parser.add_argument(
        '--dedup',
        action='store_true',
        help='去重：基于 transaction_hash + log_index 过滤重复行（用于合并断点续传的 session 文件）'
    )

    args = parser.parse_args()

    # 如果指定了日志文件，重新配置 logging
    if args.log_file:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(args.log_file),
                logging.StreamHandler()  # 同时输出到终端
            ],
            force=True
        )

    # 展开 glob pattern
    import glob
    input_files = []
    for pattern in args.input_files:
        matched = glob.glob(pattern)
        if matched:
            input_files.extend(sorted(matched))
        else:
            # 不是 pattern，直接添加
            input_files.append(pattern)

    if not input_files:
        logger.error("没有输入文件")
        sys.exit(1)

    success = merge_parquet_files(input_files, args.output, args.dry_run, args.yes, args.dedup)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
