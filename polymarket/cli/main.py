#!/usr/bin/env python3
"""
polymarket 命令行工具

使用方法:
    uv run polymarket fetch-onchain --blocks 1000
    uv run polymarket fetch-markets
    uv run polymarket process
    uv run polymarket update
"""

import argparse
import glob
import json
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import (
    DATA_DIR, LOG_DIR, STATE_FILE,
    DATASET_DIR, LATEST_RESULT_DIR, DATA_CLEAN_DIR,
    DECODED_EVENTS_FILE, MARKETS_FILE, MISSING_MARKETS_FILE,
    TRADES_OUTPUT_FILE, TRADES_PREVIEW_FILE,
    MARKETS_PREVIEW_FILE, ORDERFILLED_PREVIEW_FILE,
    USERS_CLEAN_FILE, QUANT_CLEAN_FILE,
    USERS_PREVIEW_FILE, QUANT_PREVIEW_FILE,
    CRYPTO_MARKET_IDS_FILE,
)
from ..fetchers import LogFetcher, GammaApiClient
from ..processors import (
    EventDecoder, extract_trades,
    load_token_mapping, load_crypto_market_ids,
    find_missing_tokens, save_preview_csv,
    clean_users, clean_trades, clean_users_df, clean_trades_df
)

logger = logging.getLogger(__name__)

# 加密市场 ID 集合（懒加载，None = 未初始化，set() = 空集合，有内容 = 已加载）
_crypto_ids_cache = None


def get_crypto_ids():
    """获取加密市场 ID 集合（懒加载，有缓存）。返回 None 表示未启用过滤。"""
    global _crypto_ids_cache
    if _crypto_ids_cache is None:
        _crypto_ids_cache = load_crypto_market_ids() or False  # False 表示文件不存在
    return _crypto_ids_cache if _crypto_ids_cache is not False else None


def setup_logging(verbose: bool = False):
    """设置日志，输出到控制台和文件"""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # 创建根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除已有处理器（避免重复）
    root_logger.handlers.clear()

    # 文件处理器（主要日志输出）
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / 'polymarket.log'
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # 控制台处理器（仅当不是 nohup 运行时）
    # 检查是否有交互式终端
    if sys.stdout.isatty():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(console_handler)


def get_last_block() -> int:
    """获取上次处理的区块

    优先级：
    1. STATE_FILE 中的 last_block（最准确，记录了实际处理到的区块）
    2. orderfilled.parquet 中的最大 block_number（备用）
    3. 返回 0（首次运行）
    """
    # 优先从 state 文件读取
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)

                # 新格式：fetch_onchain.last_block
                if 'fetch_onchain' in state:
                    last_block = state['fetch_onchain'].get('last_block', 0)
                    if last_block > 0:
                        return last_block

                # 旧格式兼容：直接的 last_block
                last_block = state.get('last_block', 0)
                if last_block > 0:
                    return last_block
        except (json.JSONDecodeError, IOError):
            pass

    # 备用：从 parquet 文件读取最大区块号
    if DECODED_EVENTS_FILE.exists():
        try:
            # 读取整个文件的 block_number 列找最大值
            df = pq.read_table(DECODED_EVENTS_FILE, columns=['block_number']).to_pandas()
            if len(df) > 0:
                return int(df['block_number'].max())
        except Exception:
            pass

    return 0


def save_last_block(block: int):
    """保存最后处理的区块到state.json"""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # 读取现有状态
    state = {}
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
        except:
            pass

    # 更新fetch_onchain状态
    state['fetch_onchain'] = {
        'last_block': block,
        'updated_at': datetime.now().isoformat()
    }

    # 保存
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_pending_blocks() -> list:
    """从 state.json 读取 pending_blocks 列表"""
    if not STATE_FILE.exists():
        return []
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
        return state.get('pending_blocks', [])
    except (json.JSONDecodeError, IOError):
        return []


def update_pending_blocks(newly_failed: list, newly_succeeded: list):
    """更新 state.json 中的 pending_blocks

    - 成功的删掉
    - 失败的 attempts+1（新失败的插入，attempts=1）
    - attempts > 1 说明跨多次 sync 仍未解决，属于历史遗留
    """
    state = {}
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    pending = state.get('pending_blocks', [])
    now = datetime.utcnow().isoformat()

    # 移除已成功的
    succeeded_set = {(s, e) for s, e in newly_succeeded}
    pending = [p for p in pending if (p['start'], p['end']) not in succeeded_set]

    # 新增/累加失败的
    existing = {(p['start'], p['end']): p for p in pending}
    for (s, e) in newly_failed:
        key = (s, e)
        if key in existing:
            existing[key]['attempts'] += 1
            existing[key]['last_tried'] = now
        else:
            pending.append({
                'start': s,
                'end': e,
                'attempts': 1,
                'first_failed': now,
                'last_tried': now,
            })

    state['pending_blocks'] = pending
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    if newly_failed:
        chronic = [p for p in pending if p['attempts'] > 1]
        logger.warning(f"pending_blocks: 共 {len(pending)} 个，其中 {len(chronic)} 个历史遗留（attempts>1）")


def cmd_fetch_onchain(args):
    """获取链上数据（增量模式）

    特性：
    - 使用 PyArrow 流式写入，不读取历史数据
    - 支持安全退出（Ctrl+C）
    - 支持断点续传（--continue）
    """
    import signal

    # 输入验证
    MAX_BLOCKS = 1_000_000  # 最大允许获取的区块数
    MIN_BLOCK = 1  # 最小区块号

    if args.blocks is not None:
        if args.blocks <= 0:
            logger.error(f"--blocks 必须为正整数，当前值: {args.blocks}")
            return
        if args.blocks > MAX_BLOCKS:
            logger.error(f"--blocks 超出限制，最大: {MAX_BLOCKS}，当前值: {args.blocks}")
            return

    if args.range is not None:
        start_r, end_r = args.range
        if start_r < MIN_BLOCK or end_r < MIN_BLOCK:
            logger.error(f"区块号必须 >= {MIN_BLOCK}")
            return
        if start_r > end_r:
            logger.error(f"起始区块 ({start_r}) 不能大于结束区块 ({end_r})")
            return
        if end_r - start_r + 1 > MAX_BLOCKS:
            logger.error(f"区块范围超出限制，最大: {MAX_BLOCKS}，当前范围: {end_r - start_r + 1}")
            return

    fetcher = LogFetcher(use_alchemy=args.alchemy)
    decoder = EventDecoder()

    # 确定区块范围
    if args.continue_from:
        start = get_last_block() + 1
        end = fetcher.get_latest_block()
    elif args.blocks:
        end = fetcher.get_latest_block()
        start = end - args.blocks + 1
    elif args.range:
        start, end = args.range
    else:
        logger.error("请指定: --blocks, --range, 或 --continue")
        return

    if start > end:
        logger.info("没有新区块")
        return

    logger.info(f"获取区块 {start} - {end} (共 {end - start + 1} 个区块)")

    # 创建目录
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # 加载 token 映射（用于生成 trades）
    crypto_ids = get_crypto_ids()
    token_mapping = load_token_mapping(MARKETS_FILE, crypto_ids=crypto_ids)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE, crypto_ids=crypto_ids))
    if crypto_ids:
        logger.info(f"加载 {len(token_mapping)} 个 token 映射（加密市场过滤已启用）")
    else:
        logger.info(f"加载 {len(token_mapping)} 个 token 映射")

    # 安全退出标志
    stop_requested = False
    last_saved_block = start - 1

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"收到退出信号 ({signum})，将在当前批次完成后安全退出...")
        stop_requested = True

    # 注册信号处理器
    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # PyArrow 流式 writers（追加模式）
    events_writer = None
    trades_writer = None
    quant_writer = None
    users_writer = None

    # 使用带时间戳的 session 文件名，避免覆盖已有数据
    session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    events_temp = DATASET_DIR / f'orderfilled_session_{session_ts}.parquet'
    trades_temp = DATASET_DIR / f'trades_session_{session_ts}.parquet'
    quant_temp = DATA_CLEAN_DIR / f'quant_session_{session_ts}.parquet'
    users_temp = DATA_CLEAN_DIR / f'users_session_{session_ts}.parquet'

    logger.info(f"本次会话文件: session_{session_ts}")

    def close_writers():
        """关闭所有 writers，确保数据写入磁盘"""
        nonlocal events_writer, trades_writer, quant_writer, users_writer
        for w in [events_writer, trades_writer, quant_writer, users_writer]:
            if w:
                try:
                    w.close()
                except:
                    pass
        events_writer = trades_writer = quant_writer = users_writer = None


    def merge_temp_files():
        """合并当前 session 文件到主文件（仅当前 session）"""
        for temp_file, main_file in [
            (events_temp, DECODED_EVENTS_FILE),
            (trades_temp, TRADES_OUTPUT_FILE),
            (quant_temp, QUANT_CLEAN_FILE),
            (users_temp, USERS_CLEAN_FILE)
        ]:
            if not temp_file.exists():
                continue
            if main_file.exists():
                # 合并
                temp_table = pq.read_table(temp_file)
                main_table = pq.read_table(main_file)
                combined = pa.concat_tables([main_table, temp_table])
                pq.write_table(combined, main_file, compression='snappy')
                temp_file.unlink()
                del temp_table, main_table, combined
            else:
                # 直接移动
                shutil.move(str(temp_file), str(main_file))

    # 分批获取，每 100 区块保存一次
    batch_size = 100
    checkpoint_interval = 10  # 每10批保存一次断点（约1000区块，33分钟数据）
    current = start
    total_events = 0
    total_trades = 0
    total_quant = 0
    total_users = 0
    batches_since_checkpoint = 0

    # 本次 session 新增的失败区块（将在结束时写入 state.json pending_blocks）
    session_newly_failed = []

    try:
        while current <= end:
            # 检查退出信号
            if stop_requested:
                logger.info("收到退出信号，保存进度并退出...")
                break

            batch_end = min(current + batch_size - 1, end)

            # 自适应批次重试：10→5→1，每档最多 3 次（指数退避）
            logs, still_failed = fetcher.fetch_with_adaptive_retry(current, batch_end)
            if still_failed:
                # 用尽所有策略仍失败，记入本次失败列表（session 结束后写入 pending_blocks）
                session_newly_failed.extend(still_failed)
                logger.warning(f"区块 {current}-{batch_end} 有 {len(still_failed)} 个子范围失败，记入 pending_blocks")
                # 即使部分失败，已成功获取的 logs 仍然保留处理

            if not logs:
                # 完全没拿到数据（可能全部失败，或该区块范围本来就没有事件）
                # 无论哪种情况，都必须推进 current，否则会无限循环
                current = batch_end + 1
                batches_since_checkpoint += 1
                continue

            # logs 非空，处理事件
            decoded = decoder.decode_batch(logs)
            formatted = decoder.format_batch(decoded)

            if formatted:
                # 新数据（只包含当前批次）
                new_df = pd.DataFrame(formatted)
                batch_events = len(new_df)

                # 1. 写入 orderfilled（流式追加到临时文件）
                events_table = pa.Table.from_pandas(new_df, preserve_index=False)
                if events_writer is None:
                    events_writer = pq.ParquetWriter(str(events_temp), events_table.schema, compression='snappy')
                events_writer.write_table(events_table)
                new_df.tail(1000).to_csv(ORDERFILLED_PREVIEW_FILE, index=False)

                # 2. 生成 trades
                events = new_df.to_dict('records')
                trades_df = extract_trades(events, token_mapping, crypto_only=crypto_ids is not None)
                batch_trades = 0
                batch_quant = 0
                batch_users = 0

                if not trades_df.empty:
                    batch_trades = len(trades_df)

                    # 写入 trades
                    trades_table = pa.Table.from_pandas(trades_df, preserve_index=False)
                    if trades_writer is None:
                        trades_writer = pq.ParquetWriter(str(trades_temp), trades_table.schema, compression='snappy')
                    trades_writer.write_table(trades_table)
                    trades_df.tail(1000).to_csv(TRADES_PREVIEW_FILE, index=False)

                    # 3. 生成 quant
                    quant_df = clean_trades_df(trades_df)
                    if not quant_df.empty:
                        batch_quant = len(quant_df)
                        quant_table = pa.Table.from_pandas(quant_df, preserve_index=False)
                        if quant_writer is None:
                            quant_writer = pq.ParquetWriter(str(quant_temp), quant_table.schema, compression='snappy')
                        quant_writer.write_table(quant_table)
                        quant_df.tail(1000).to_csv(QUANT_PREVIEW_FILE, index=False)

                    # 4. 生成 users
                    users_df = clean_users_df(trades_df)
                    if not users_df.empty:
                        batch_users = len(users_df)
                        users_table = pa.Table.from_pandas(users_df, preserve_index=False)
                        if users_writer is None:
                            users_writer = pq.ParquetWriter(str(users_temp), users_table.schema, compression='snappy')
                        users_writer.write_table(users_table)
                        users_df.tail(1000).to_csv(USERS_PREVIEW_FILE, index=False)

                total_events += batch_events
                total_trades += batch_trades
                total_quant += batch_quant
                total_users += batch_users

                logger.info(f"区块 {current}-{batch_end}: "
                           f"事件+{batch_events}, 交易+{batch_trades}, "
                           f"quant+{batch_quant}, users+{batch_users}")

            last_saved_block = batch_end
            current = batch_end + 1
            batches_since_checkpoint += 1

            # 定期保存断点（防止崩溃丢失进度）
            if batches_since_checkpoint >= checkpoint_interval:
                save_last_block(last_saved_block)
                logger.info(f"  ✓ 断点已保存 (区块 {last_saved_block})")
                batches_since_checkpoint = 0

        # 关闭 writers
        close_writers()

        # 根据参数决定是否合并
        if total_events > 0 and getattr(args, 'merge', False):
            logger.info("合并数据到主文件...")
            merge_temp_files()
        elif total_events > 0:
            logger.info(f"数据已保存到临时文件，使用 --merge 参数合并到主文件")

        # 保存最后处理的区块
        save_last_block(last_saved_block)

        logger.info(f"链上数据获取完成, 新增: 事件 {total_events}, 交易 {total_trades}, "
                   f"quant {total_quant}, users {total_users}")

        # 将本次失败区块写入 state.json pending_blocks
        if session_newly_failed:
            update_pending_blocks(newly_failed=session_newly_failed, newly_succeeded=[])
            logger.warning(f"⚠️ 共 {len(session_newly_failed)} 个子区块范围记入 pending_blocks，下次 sync 自动重试")

    except Exception as e:
        logger.error(f"获取链上数据出错: {e}")
        close_writers()
        # 即使出错也保存进度
        if last_saved_block >= start:
            save_last_block(last_saved_block)
        raise

    finally:
        # 恢复原始信号处理器
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        close_writers()


def cmd_fetch_markets(args):
    """增量获取新市场（高频运行，如每小时）

    特性：
    - 增量获取新市场，不重复获取已有市场
    - 支持安全退出（Ctrl+C）
    - 支持断点续传（--continue）
    - 市场数据较小，直接内存操作后一次性保存
    """
    import signal

    client = GammaApiClient()

    if not client.test_connection():
        logger.error("API 连接失败")
        return

    # 创建目录
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # 加载已有市场ID集合（只加载ID，不加载全部数据）
    existing_ids = set()
    if MARKETS_FILE.exists():
        existing_df = pq.read_table(MARKETS_FILE, columns=['id']).to_pandas()
        existing_ids = set(existing_df['id'].astype(str).tolist())
        logger.info(f"已有 {len(existing_ids)} 个市场")

    # 安全退出标志
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"收到退出信号 ({signum})，将在当前批次完成后安全退出...")
        stop_requested = True

    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # 读取断点
    continue_from = getattr(args, 'continue_from', False)
    offset = 0
    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                markets_state = state.get('fetch_markets', {})
                offset = markets_state.get('last_offset', 0)
                if offset > 0:
                    logger.info(f"从 offset={offset} 继续获取")
        except Exception as e:
            logger.warning(f"读取断点失败: {e}")
            offset = 0

    logger.info("增量获取新市场...")
    new_markets = []  # 只存储新市场
    new_count = 0
    consecutive_existing = 0
    batch_size = 500

    try:
        while True:
            if stop_requested:
                logger.info("收到退出信号，保存进度并退出...")
                break

            logger.info(f"获取 offset={offset}...")
            markets = client.get_markets(limit=batch_size, offset=offset)

            if not markets:
                break

            batch_new = 0

            for market in markets:
                market_id = str(market['id'])
                if market_id not in existing_ids:
                    # 新市场
                    new_markets.append(market)
                    existing_ids.add(market_id)
                    batch_new += 1
                    new_count += 1
                    consecutive_existing = 0
                else:
                    consecutive_existing += 1

            if batch_new > 0:
                logger.info(f"  本批新增 {batch_new} 个市场")

            # 如果连续3批都是已存在的市场，停止
            if consecutive_existing >= batch_size * 3:
                logger.info("连续遇到已存在市场，增量同步完成")
                break

            if len(markets) < batch_size:
                break

            offset += len(markets)
            time.sleep(0.3)

        # 保存新市场（追加到主文件）
        if new_markets:
            logger.info(f"保存 {len(new_markets)} 个新市场...")
            new_df = pd.DataFrame(new_markets)

            if MARKETS_FILE.exists():
                # 追加到已有文件
                existing_table = pq.read_table(MARKETS_FILE)
                new_table = pa.Table.from_pandas(new_df, preserve_index=False)
                combined = pa.concat_tables([existing_table, new_table])
                pq.write_table(combined, MARKETS_FILE, compression='snappy')
                del existing_table, combined
            else:
                new_df.to_parquet(MARKETS_FILE, index=False)

            # 更新预览（从文件读取最新1000条）
            preview_df = pq.read_table(MARKETS_FILE).to_pandas().tail(1000)
            preview_df.to_csv(MARKETS_PREVIEW_FILE, index=False)

        # 保存断点
        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass

        state['fetch_markets'] = {
            'last_offset': offset,
            'total_markets': len(existing_ids),
            'new_count': new_count,
            'updated_at': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

        logger.info(f"完成! 共 {len(existing_ids)} 个市场 (新增 {new_count})")

    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def cmd_update_markets(args):
    """更新未closed市场的状态（低频运行，如每周）

    特性：
    - 只更新未closed的市场
    - 支持安全退出（Ctrl+C）
    - 支持断点续传（--continue）
    - 市场数据较小，需要加载全部用于更新

    注意：市场文件较小（~60MB），需要完整加载才能更新特定市场
    """
    import signal

    client = GammaApiClient()

    if not client.test_connection():
        logger.error("API 连接失败")
        return

    if not MARKETS_FILE.exists():
        logger.error(f"市场文件不存在: {MARKETS_FILE}")
        return

    # 加载已有数据（市场文件较小，~60MB，可以完整加载）
    df = pd.read_parquet(MARKETS_FILE)
    markets_dict = {str(row['id']): dict(row) for _, row in df.iterrows()}

    # 筛选未closed的市场
    unclosed_ids = []
    for mid, m in markets_dict.items():
        is_closed = m.get('closed', m.get('resolved', False))
        if not is_closed:
            unclosed_ids.append(mid)

    logger.info(f"共 {len(markets_dict)} 个市场，其中 {len(unclosed_ids)} 个未closed")

    if not unclosed_ids:
        logger.info("没有需要更新的市场")
        return

    # 安全退出标志
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"收到退出信号 ({signum})，将在当前市场完成后安全退出...")
        stop_requested = True

    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # 读取断点
    continue_from = getattr(args, 'continue_from', False)
    start_idx = 0
    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                update_state = state.get('update_markets', {})
                start_idx = update_state.get('last_index', 0)
                if start_idx > 0:
                    logger.info(f"从第 {start_idx} 个市场继续更新")
        except Exception as e:
            logger.warning(f"读取断点失败: {e}")
            start_idx = 0

    updated_count = 0
    closed_count = 0
    last_saved_idx = start_idx - 1

    def save_progress(idx):
        """保存进度和数据"""
        df_updated = pd.DataFrame(list(markets_dict.values()))
        pq.write_table(
            pa.Table.from_pandas(df_updated, preserve_index=False),
            MARKETS_FILE,
            compression='snappy'
        )
        df_updated.tail(1000).to_csv(MARKETS_PREVIEW_FILE, index=False)

        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass
        state['update_markets'] = {
            'last_index': idx + 1,
            'total_unclosed': len(unclosed_ids),
            'updated_count': updated_count,
            'closed_count': closed_count,
            'updated_at': datetime.now().isoformat()
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

    try:
        for idx, market_id in enumerate(unclosed_ids[start_idx:], start=start_idx):
            if stop_requested:
                logger.info("收到退出信号，保存进度并退出...")
                break

            logger.info(f"更新市场 {idx+1}/{len(unclosed_ids)}: {market_id[:20]}...")

            old_market = markets_dict[market_id]
            token_id = old_market.get('token1', '')

            if not token_id:
                logger.warning(f"市场 {market_id} 没有 token_id，跳过")
                last_saved_idx = idx
                continue

            new_market = client.get_market_by_token(token_id)

            if new_market and new_market['id'] == market_id:
                markets_dict[market_id] = new_market
                updated_count += 1

                if new_market.get('closed', False):
                    closed_count += 1
                    logger.info(f"  ✓ 市场已closed")

            last_saved_idx = idx

            # 每50个保存一次
            if (idx + 1) % 50 == 0:
                save_progress(idx)
                logger.info(f"进度: {idx+1}/{len(unclosed_ids)} (已更新 {updated_count}, 新closed {closed_count})")

            time.sleep(0.3)

        # 最终保存
        if last_saved_idx >= start_idx:
            save_progress(last_saved_idx)

        logger.info(f"完成! 更新 {updated_count} 个市场，其中 {closed_count} 个已closed")

    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def cmd_process_historical(args):
    """分批处理历史数据（用于大文件，避免内存溢出）

    使用方式：
        python3 run.py process-historical --batch-size 1000000
        python3 run.py process-historical --continue  # 从断点继续

    说明：
        - 分批读取 orderfilled.parquet
        - 使用 PyArrow 流式写入（不读取已有数据）
        - 支持断点续传，中断后可继续
        - 安全退出：Ctrl+C 完成当前批次后关闭writer保存
    """
    import signal
    import gc

    if not DECODED_EVENTS_FILE.exists():
        logger.error(f"事件文件不存在: {DECODED_EVENTS_FILE}")
        return

    batch_size = getattr(args, 'batch_size', 1000000)  # 默认每批100万条
    test_batches = getattr(args, 'test_batches', None)  # 测试模式
    continue_from = getattr(args, 'continue_from', False)  # 断点续传
    checkpoint_interval = 10  # 每10批保存进度

    # 读取上次进度（从state.json）
    start_batch = 0
    total_trades = 0
    total_quant = 0
    total_users = 0
    session_id = 0  # 用于区分不同运行session的文件

    if continue_from and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
                process_state = state.get('process_historical', {})
                start_batch = process_state.get('last_batch', -1) + 1
                total_trades = process_state.get('total_trades', 0)
                total_quant = process_state.get('total_quant', 0)
                total_users = process_state.get('total_users', 0)
                session_id = process_state.get('session_id', 0) + 1
                if start_batch > 0:
                    logger.info(f"从批次 {start_batch + 1} 继续处理 (session {session_id})")
                    logger.info(f"  已有数据: trades={total_trades:,}, quant={total_quant:,}, users={total_users:,}")
        except Exception as e:
            logger.warning(f"读取进度失败: {e}，从头开始")
            start_batch = 0

    if test_batches:
        logger.info(f"测试模式：处理批次 {start_batch + 1} 到 {start_batch + test_batches}，每批 {batch_size:,} 条")
    else:
        if start_batch > 0:
            logger.info(f"继续分批处理历史数据，从批次 {start_batch + 1} 开始，每批 {batch_size:,} 条")
        else:
            logger.info(f"开始分批处理历史数据，每批 {batch_size:,} 条")

    # 1. 加载 token 映射
    logger.info("加载 token 映射...")
    crypto_ids = get_crypto_ids()
    token_mapping = load_token_mapping(MARKETS_FILE, crypto_ids=crypto_ids)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE, crypto_ids=crypto_ids))
    if crypto_ids:
        logger.info(f"共 {len(token_mapping)} 个 token 映射（加密市场过滤已启用）")
    else:
        logger.info(f"共 {len(token_mapping)} 个 token 映射")

    # 2. 获取总行数
    parquet_file = pq.ParquetFile(DECODED_EVENTS_FILE)
    total_rows = parquet_file.metadata.num_rows
    total_batches = (total_rows + batch_size - 1) // batch_size
    logger.info(f"总计 {total_rows:,} 条事件，共 {total_batches} 批")

    if start_batch >= total_batches:
        logger.info("所有数据已处理完成")
        return

    # 确保目录存在
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # 安全退出标志
    stop_requested = False

    def signal_handler(signum, frame):
        nonlocal stop_requested
        logger.warning(f"收到退出信号 ({signum})，将在当前批次完成后安全退出...")
        stop_requested = True

    # 注册信号处理器
    original_sigint = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

    # 确定输出文件路径
    # 如果是续传，写入新的session文件；否则直接写主文件
    if start_batch > 0:
        trades_output = DATASET_DIR / f'trades_session_{session_id}.parquet'
        quant_output = DATA_CLEAN_DIR / f'quant_session_{session_id}.parquet'
        users_output = DATA_CLEAN_DIR / f'users_session_{session_id}.parquet'
    else:
        # 从头开始，删除旧文件
        trades_output = TRADES_OUTPUT_FILE
        quant_output = QUANT_CLEAN_FILE
        users_output = USERS_CLEAN_FILE
        for f in [trades_output, quant_output, users_output]:
            if f.exists():
                f.unlink()

    # PyArrow 流式 writers
    trades_writer = None
    quant_writer = None
    users_writer = None

    def save_progress(batch_idx, final=False):
        """保存进度到 state.json"""
        state = {}
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    state = json.load(f)
            except:
                pass

        state['process_historical'] = {
            'last_batch': batch_idx,
            'total_batches': total_batches,
            'total_trades': total_trades,
            'total_quant': total_quant,
            'total_users': total_users,
            'session_id': session_id,
            'updated_at': datetime.now().isoformat()
        }

        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

    def close_writers():
        """关闭所有writers"""
        nonlocal trades_writer, quant_writer, users_writer
        if trades_writer:
            trades_writer.close()
            trades_writer = None
        if quant_writer:
            quant_writer.close()
            quant_writer = None
        if users_writer:
            users_writer.close()
            users_writer = None

    def merge_session_files():
        """合并所有session文件到主文件"""
        for main_file, pattern, output_dir in [
            (TRADES_OUTPUT_FILE, 'trades_session_*.parquet', DATASET_DIR),
            (QUANT_CLEAN_FILE, 'quant_session_*.parquet', DATA_CLEAN_DIR),
            (USERS_CLEAN_FILE, 'users_session_*.parquet', DATA_CLEAN_DIR)
        ]:
            session_files = sorted(glob.glob(str(output_dir / pattern)))
            if not session_files:
                continue

            # 收集所有文件（主文件 + session文件）
            all_files = []
            if main_file.exists():
                all_files.append(str(main_file))
            all_files.extend(session_files)

            if len(all_files) <= 1:
                # 只有一个文件，如果是session文件就重命名为主文件
                if session_files and not main_file.exists():
                    shutil.move(session_files[0], str(main_file))
                continue

            # 合并所有文件
            logger.info(f"合并 {len(all_files)} 个文件到 {main_file.name}...")
            tables = [pq.read_table(f) for f in all_files]
            combined = pa.concat_tables(tables)
            pq.write_table(combined, main_file, compression='snappy')

            # 删除session文件
            for sf in session_files:
                Path(sf).unlink()

            del tables, combined
            gc.collect()

    try:
        last_completed_batch = start_batch - 1

        for batch_idx, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
            # 检查退出信号
            if stop_requested:
                logger.info("收到退出信号，关闭writers并保存进度...")
                close_writers()
                save_progress(last_completed_batch)
                break

            # 跳过已处理的批次
            if batch_idx < start_batch:
                continue

            # 测试模式：只处理指定数量的批次
            if test_batches and (batch_idx - start_batch) >= test_batches:
                logger.info(f"测试模式完成，已处理 {test_batches} 批")
                break

            batch_start_time = datetime.now()
            batch_df = batch.to_pandas()
            batch_rows = len(batch_df)
            progress_pct = (batch_idx + 1) * 100.0 / total_batches
            logger.info(f"处理批次 {batch_idx + 1}/{total_batches}: {batch_rows:,} 条事件 ({progress_pct:.1f}%)")

            # 生成 trades
            events = batch_df.to_dict('records')
            trades_df = extract_trades(events, token_mapping, crypto_only=crypto_ids is not None)

            batch_quant = 0
            batch_users = 0

            if not trades_df.empty:
                batch_trades = len(trades_df)
                total_trades += batch_trades

                # 写入 trades
                trades_table = pa.Table.from_pandas(trades_df, preserve_index=False)
                if trades_writer is None:
                    trades_writer = pq.ParquetWriter(str(trades_output), trades_table.schema, compression='snappy')
                trades_writer.write_table(trades_table)

                # 生成并写入 quant
                quant_df = clean_trades_df(trades_df)
                if not quant_df.empty:
                    batch_quant = len(quant_df)
                    total_quant += batch_quant
                    quant_table = pa.Table.from_pandas(quant_df, preserve_index=False)
                    if quant_writer is None:
                        quant_writer = pq.ParquetWriter(str(quant_output), quant_table.schema, compression='snappy')
                    quant_writer.write_table(quant_table)

                # 生成并写入 users
                users_df = clean_users_df(trades_df)
                if not users_df.empty:
                    batch_users = len(users_df)
                    total_users += batch_users
                    users_table = pa.Table.from_pandas(users_df, preserve_index=False)
                    if users_writer is None:
                        users_writer = pq.ParquetWriter(str(users_output), users_table.schema, compression='snappy')
                    users_writer.write_table(users_table)

                batch_elapsed = (datetime.now() - batch_start_time).total_seconds()
                logger.info(f"  → 交易+{batch_trades:,}, quant+{batch_quant:,}, users+{batch_users:,} ({batch_elapsed:.1f}s)")

                # 实时更新 CSV 预览（保存最新1000条）
                trades_df.tail(1000).to_csv(TRADES_PREVIEW_FILE, index=False)
                if not quant_df.empty:
                    quant_df.tail(1000).to_csv(QUANT_PREVIEW_FILE, index=False)
                if not users_df.empty:
                    users_df.tail(1000).to_csv(USERS_PREVIEW_FILE, index=False)

            # 标记此批次完成
            last_completed_batch = batch_idx

            # 每 N 批保存进度（只保存state，不关闭writer）
            batches_processed = batch_idx - start_batch + 1
            if batches_processed > 0 and batches_processed % checkpoint_interval == 0:
                save_progress(batch_idx)
                logger.info(f"  ✓ 进度已保存 (批次 {batch_idx + 1})")

            # 显式释放内存
            del batch_df, trades_df
            if 'quant_df' in locals():
                del quant_df
            if 'users_df' in locals():
                del users_df
            gc.collect()

        # 正常完成
        if not stop_requested:
            close_writers()
            save_progress(last_completed_batch)

            # 合并所有session文件
            if session_id > 0:
                logger.info("合并所有session文件...")
                merge_session_files()

            logger.info(f"历史数据处理完成!")
            logger.info(f"  总计: 交易 {total_trades:,}, quant {total_quant:,}, users {total_users:,}")

    except Exception as e:
        logger.error(f"处理出错: {e}")
        close_writers()
        if last_completed_batch >= start_batch:
            save_progress(last_completed_batch)
            logger.info(f"已保存进度到批次 {last_completed_batch + 1}")
        raise

    finally:
        # 恢复原始信号处理器
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)

        # 确保 writers 关闭
        close_writers()


def cmd_process(args):
    """处理交易数据（带 market_id 关联和缺失 token 补全）

    警告：此命令会一次性读取全部数据，仅适用于小数据集！
    对于大数据集（>1GB），请使用 process-historical 命令。
    """
    if not DECODED_EVENTS_FILE.exists():
        logger.error(f"事件文件不存在: {DECODED_EVENTS_FILE}")
        return

    # 1. 加载 token 映射
    logger.info("加载 token 映射...")
    crypto_ids = get_crypto_ids()
    token_mapping = load_token_mapping(MARKETS_FILE, crypto_ids=crypto_ids)

    # 也加载缺失市场文件
    if MISSING_MARKETS_FILE.exists():
        missing_mapping = load_token_mapping(MISSING_MARKETS_FILE, crypto_ids=crypto_ids)
        token_mapping.update(missing_mapping)
        logger.info(f"合并缺失市场映射，共 {len(token_mapping)} 个 token")

    # 2. 读取事件并提取交易
    logger.info("读取事件...")
    df = pd.read_parquet(DECODED_EVENTS_FILE)
    events = df.to_dict('records')

    logger.info("提取交易...")
    trades_df = extract_trades(events, token_mapping, crypto_only=crypto_ids is not None)

    if trades_df.empty:
        logger.info("没有交易数据")
        return

    # 3. 查找并补全缺失 token（crypto_only 模式下不需要，因为非加密 token 是有意跳过的）
    missing_tokens = set() if crypto_ids else find_missing_tokens(trades_df, token_mapping)
    if missing_tokens and not getattr(args, 'skip_missing', False):
        logger.info(f"补全 {len(missing_tokens)} 个缺失 token...")
        client = GammaApiClient()
        new_markets = client.fetch_missing_tokens(list(missing_tokens))

        if new_markets:
            # 保存到缺失市场文件
            new_df = pd.DataFrame(new_markets)
            MISSING_MARKETS_FILE.parent.mkdir(parents=True, exist_ok=True)

            if MISSING_MARKETS_FILE.exists():
                existing = pd.read_parquet(MISSING_MARKETS_FILE)
                new_df = pd.concat([existing, new_df], ignore_index=True)
                new_df = new_df.drop_duplicates(subset=['id'])

            new_df.to_parquet(MISSING_MARKETS_FILE, index=False)
            logger.info(f"保存 {len(new_markets)} 个缺失市场")

            # 更新映射并重新处理
            for m in new_markets:
                if m.get('token1'):
                    token_mapping[m['token1']] = {'market_id': m['id'], 'answer': m.get('answer1', 'YES')}
                if m.get('token2'):
                    token_mapping[m['token2']] = {'market_id': m['id'], 'answer': m.get('answer2', 'NO')}

            # 重新提取交易（带完整映射）
            logger.info("重新提取交易...")
            trades_df = extract_trades(events, token_mapping, crypto_only=crypto_ids is not None)

    # 4. 保存结果
    TRADES_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    trades_df.to_parquet(TRADES_OUTPUT_FILE, index=False)
    logger.info(f"保存 {len(trades_df)} 条交易到 {TRADES_OUTPUT_FILE}")

    # 5. 保存 CSV 预览（最新 1000 条）
    save_preview_csv(trades_df, TRADES_PREVIEW_FILE, n_rows=1000)

    # 6. 统计信息
    matched = (trades_df['market_id'] != '').sum()
    logger.info(f"market_id 匹配率: {matched}/{len(trades_df)} ({matched/len(trades_df)*100:.1f}%)")


def cmd_clean_users(args):
    """清洗用户数据"""
    if not TRADES_OUTPUT_FILE.exists():
        logger.error(f"交易文件不存在: {TRADES_OUTPUT_FILE}")
        logger.info("请先运行 fetch-onchain 和 process 命令获取交易数据")
        return

    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    try:
        stats = clean_users(
            input_path=TRADES_OUTPUT_FILE,
            output_path=USERS_CLEAN_FILE,
            batch_size=args.batch_size,
            test_rows=args.test
        )
        logger.info(f"用户数据已保存到: {USERS_CLEAN_FILE}")
    except Exception as e:
        logger.error(f"清洗用户数据失败: {e}")
        raise


def cmd_clean_trades(args):
    """清洗交易数据（量化用）"""
    if not TRADES_OUTPUT_FILE.exists():
        logger.error(f"交易文件不存在: {TRADES_OUTPUT_FILE}")
        logger.info("请先运行 fetch-onchain 和 process 命令获取交易数据")
        return

    DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    try:
        stats = clean_trades(
            input_path=TRADES_OUTPUT_FILE,
            output_path=QUANT_CLEAN_FILE,
            batch_size=args.batch_size,
            test_rows=args.test
        )
        logger.info(f"量化交易数据已保存到: {QUANT_CLEAN_FILE}")
    except Exception as e:
        logger.error(f"清洗交易数据失败: {e}")
        raise


def cmd_clean(args):
    """运行所有数据清洗"""
    logger.info("=== 清洗用户数据 ===")
    cmd_clean_users(args)

    logger.info("\n=== 清洗量化交易数据 ===")
    cmd_clean_trades(args)

    logger.info("\n数据清洗完成!")


def cmd_update(args):
    """全量更新"""
    logger.info("=== 更新市场数据 ===")
    cmd_fetch_markets(args)

    logger.info("\n=== 刷新已有市场状态 ===")
    cmd_update_markets(args)

    logger.info("\n=== 更新链上数据 ===")
    args.continue_from = True
    args.blocks = None
    args.range = None
    args.merge = True
    cmd_fetch_onchain(args)

    logger.info("\n=== 处理交易 ===")
    cmd_process(args)

    # 如果指定了 --clean，也运行数据清洗
    if getattr(args, 'with_clean', False):
        logger.info("\n=== 清洗数据 ===")
        args.batch_size = 5_000_000
        args.test = None
        cmd_clean(args)

    logger.info("\n全量更新完成!")


def cmd_build_crypto_filter(args):
    """
    从 markets.parquet 生成加密 Up/Down 市场列表，保存到 data/updown_markets.parquet。
    只保留 BTC/ETH/SOL/XRP/BNB/DOGE 的涨跌预测市场（5min/15min/hourly/4hr/daily）。
    文件存在后，所有命令（sync/process）自动只处理这些市场。
    """
    import re

    if not MARKETS_FILE.exists():
        logger.error(f"markets.parquet 不存在: {MARKETS_FILE}，请先运行 fetch-markets")
        return

    logger.info("=== 生成加密 Up/Down 市场过滤列表 ===")

    df = pq.read_table(
        MARKETS_FILE,
        columns=['id', 'event_slug', 'event_title', 'token1', 'token2', 'end_date', 'created_at']
    ).to_pandas()
    logger.info(f"总市场数: {len(df):,}")

    pat_updown = re.compile(r'\bup.or.down\b', re.IGNORECASE)
    pat_crypto = re.compile(
        r'\b(?:bitcoin|btc|ethereum|eth|solana|sol|xrp|ripple|bnb|doge|dogecoin)\b',
        re.IGNORECASE
    )

    slug = df['event_slug'].fillna('')
    title = df['event_title'].fillna('')
    updown_mask = slug.str.contains(pat_updown, regex=True) | title.str.contains(pat_updown, regex=True)
    crypto_mask = slug.str.contains(pat_crypto, regex=True) | title.str.contains(pat_crypto, regex=True)
    filtered_df = df[updown_mask & crypto_mask].copy()

    # 标注 asset
    def get_asset(t):
        t = t.lower()
        for asset, kws in [('BTC', ['bitcoin', 'btc']), ('ETH', ['ethereum', 'eth']),
                           ('SOL', ['solana', 'sol']), ('XRP', ['xrp', 'ripple']),
                           ('BNB', ['bnb']), ('DOGE', ['doge', 'dogecoin'])]:
            if any(k in t for k in kws):
                return asset
        return 'OTHER'

    # 标注 period
    def get_period(t):
        m = re.search(r'(\d+):(\d+)[AP]M-(\d+):(\d+)[AP]M', t, re.IGNORECASE)
        if m:
            diff = (int(m.group(3)) * 60 + int(m.group(4)) - int(m.group(1)) * 60 - int(m.group(2))) % (12 * 60)
            return f'{diff}min'
        if re.search(r'\d+[AP]M ET', t, re.IGNORECASE):
            return 'hourly'
        if re.search(r'on \w+ \d+', t, re.IGNORECASE):
            return 'daily'
        return 'other'

    filtered_df['asset'] = filtered_df['event_title'].apply(get_asset)
    filtered_df['period'] = filtered_df['event_title'].apply(get_period)

    if getattr(args, 'preview', False):
        logger.info(f"\n匹配到 {len(filtered_df):,} 个市场（预览模式，不写入文件）")
        from collections import Counter
        for (a, p), cnt in sorted(Counter(zip(filtered_df['asset'], filtered_df['period'])).items()):
            logger.info(f"  {a:5s} {p:8s}: {cnt:,}")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    filtered_df.to_parquet(CRYPTO_MARKET_IDS_FILE, index=False)

    logger.info(f"✓ 写入 {len(filtered_df):,} 个市场 → {CRYPTO_MARKET_IDS_FILE}")
    logger.info(f"  占全部市场的 {len(filtered_df)/len(df)*100:.1f}%")
    logger.info("过滤已启用，下次 sync/process 将自动只处理这些市场")

    global _crypto_ids_cache
    _crypto_ids_cache = None


def cmd_merge_sessions(args):
    """合并所有 session 文件到主文件"""
    import gc

    logger.info("=== 合并 session 文件 ===")

    for main_file, pattern, output_dir, file_type in [
        (DECODED_EVENTS_FILE, 'orderfilled_session_*.parquet', DATASET_DIR, 'orderfilled_session'),
        (DECODED_EVENTS_FILE, 'orderfilled_refetched_*.parquet', DATASET_DIR, 'orderfilled_refetched'),
        (DECODED_EVENTS_FILE, 'orderfilled_append.parquet', DATASET_DIR, 'orderfilled_append'),
        (TRADES_OUTPUT_FILE, 'trades_session_*.parquet', DATASET_DIR, 'trades_session'),
        (TRADES_OUTPUT_FILE, 'trades_refetched_*.parquet', DATASET_DIR, 'trades_refetched'),
        (TRADES_OUTPUT_FILE, 'trades_append.parquet', DATASET_DIR, 'trades_append'),
        (QUANT_CLEAN_FILE, 'quant_session_*.parquet', DATA_CLEAN_DIR, 'quant_session'),
        (QUANT_CLEAN_FILE, 'quant_refetched_*.parquet', DATA_CLEAN_DIR, 'quant_refetched'),
        (QUANT_CLEAN_FILE, 'quant_append.parquet', DATA_CLEAN_DIR, 'quant_append'),
        (USERS_CLEAN_FILE, 'users_session_*.parquet', DATA_CLEAN_DIR, 'users_session'),
        (USERS_CLEAN_FILE, 'users_refetched_*.parquet', DATA_CLEAN_DIR, 'users_refetched'),
        (USERS_CLEAN_FILE, 'users_append.parquet', DATA_CLEAN_DIR, 'users_append'),
    ]:
        session_files = sorted(glob.glob(str(output_dir / pattern)))
        if not session_files:
            continue

        logger.info(f"找到 {len(session_files)} 个 {file_type} 文件")

        # 收集所有文件（主文件 + session文件）
        all_files = []
        if main_file.exists():
            all_files.append(str(main_file))
        all_files.extend(session_files)

        if len(all_files) <= 1:
            # 只有一个文件，如果是session文件就重命名为主文件
            if session_files and not main_file.exists():
                shutil.move(session_files[0], str(main_file))
                logger.info(f"移动 {session_files[0]} -> {main_file}")
            continue

        # 流式合并（避免一次性读入大文件）
        logger.info(f"合并 {len(all_files)} 个文件到 {main_file.name}...")
        schema = pq.read_schema(all_files[0])
        temp_file = str(main_file) + '.tmp'
        total_rows = 0
        with pq.ParquetWriter(temp_file, schema, compression='snappy') as writer:
            for f in all_files:
                pf = pq.ParquetFile(f)
                for batch in pf.iter_batches(batch_size=200_000):
                    writer.write_batch(batch)
                    total_rows += batch.num_rows
        shutil.move(temp_file, str(main_file))
        logger.info(f"合并完成，共 {total_rows:,} 行")

        # 删除session文件
        for sf in session_files:
            Path(sf).unlink()
            logger.info(f"删除 {sf}")

        gc.collect()

    logger.info("所有 session 文件合并完成!")


def cmd_sync(args):
    """
    一条命令完成完整同步：
      1. 重试 state.json 中的 pending_blocks（自适应批次）
      2. 增量 fetch 新区块（chain head - last_block）
      3. merge session 文件到主文件（带去重）
    """
    from ..tools.merge_parquet import merge_parquet_files

    use_alchemy = args.alchemy
    fetcher = LogFetcher(use_alchemy=use_alchemy)
    decoder = EventDecoder()
    crypto_ids = get_crypto_ids()
    token_mapping = load_token_mapping(MARKETS_FILE, crypto_ids=crypto_ids)
    if MISSING_MARKETS_FILE.exists():
        token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE, crypto_ids=crypto_ids))
    if crypto_ids:
        logger.info(f"加密市场过滤已启用（{len(crypto_ids):,} 个市场）")

    # ── Step 1: 重试 pending_blocks ──────────────────────────────────────
    pending = load_pending_blocks()
    if pending:
        logger.info(f"=== Step 1: 重试 {len(pending)} 个 pending 区块 ===")
        chronic = [p for p in pending if p['attempts'] > 1]
        if chronic:
            logger.warning(f"  其中 {len(chronic)} 个为历史遗留（attempts > 1）")

        still_failing = []
        succeeded = []
        all_pending_formatted = []
        for item in pending:
            s, e = item['start'], item['end']
            logs, still_failed = fetcher.fetch_with_adaptive_retry(s, e)
            if still_failed:
                still_failing.extend(still_failed)
            else:
                succeeded.append((s, e))
                if logs:
                    decoded = decoder.decode_batch(logs)
                    formatted = decoder.format_batch(decoded)
                    if formatted:
                        all_pending_formatted.extend(formatted)

        # 所有 pending 数据写入单个文件（避免同秒多文件冲突）
        if all_pending_formatted:
            session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            out = DATASET_DIR / f'orderfilled_pending_{session_ts}.parquet'
            pd.DataFrame(all_pending_formatted).to_parquet(out, index=False, compression='snappy')
            logger.info(f"  pending 数据已保存: {out.name} ({len(all_pending_formatted)} 行)")

        update_pending_blocks(newly_failed=still_failing, newly_succeeded=succeeded)
        logger.info(f"  Step 1 完成: {len(succeeded)} 个成功，{len(still_failing)} 个仍失败")
    else:
        logger.info("=== Step 1: 无 pending 区块，跳过 ===")

    # ── Step 2: 增量 fetch 新区块 ────────────────────────────────────────
    logger.info("=== Step 2: 增量 fetch 新区块 ===")
    args.continue_from = True
    args.blocks = None
    args.range = None
    args.merge = False  # merge 在 Step 3 单独做
    cmd_fetch_onchain(args)

    # ── Step 3: merge session 文件 ───────────────────────────────────────
    logger.info("=== Step 3: merge session 文件 ===")
    main_file = DECODED_EVENTS_FILE  # orderfilled.parquet

    session_files = sorted(glob.glob(str(DATASET_DIR / 'orderfilled_session_*.parquet')))
    pending_files = sorted(glob.glob(str(DATASET_DIR / 'orderfilled_pending_*.parquet')))
    refetch_files = sorted(glob.glob(str(DATASET_DIR / 'orderfilled_refetched_*.parquet')))

    new_files = session_files + pending_files + refetch_files
    if not new_files:
        logger.info("  没有新的 session 文件，跳过 merge")
        return

    logger.info(f"  待合并文件: {len(new_files)} 个")
    out_file = str(main_file).replace('.parquet', '_synced.parquet')
    success = merge_parquet_files(
        [str(main_file)] + new_files,
        out_file,
        auto_yes=True,
        dedup=True,
    )
    if success:
        shutil.move(out_file, str(main_file))
        logger.info(f"  ✓ merge 完成，主文件已更新: {main_file.name}")
        # 删除已合并的 session 文件
        for f in new_files:
            Path(f).unlink(missing_ok=True)
        logger.info(f"  ✓ 已清理 {len(new_files)} 个 session 文件")
    else:
        logger.error("  merge 失败，session 文件保留")

    # 打印 pending 概况
    remaining = load_pending_blocks()
    if remaining:
        logger.warning(f"仍有 {len(remaining)} 个 pending 区块未解决（下次 sync 自动重试）")
        for p in remaining:
            logger.warning(f"  {p['start']}-{p['end']}  attempts={p['attempts']}  last_tried={p['last_tried']}")

    if getattr(args, 'no_process', False):
        logger.info("=== sync 完成（跳过 process/clean）===")
        return

    # ── Step 4: 更新市场元数据 + 重建过滤列表 ──────────────────────────────
    logger.info("=== Step 4: 更新市场元数据 ===")
    args.continue_from = True
    cmd_fetch_markets(args)
    cmd_update_markets(args)
    logger.info("=== Step 4b: 重建 updown_market_ids.txt ===")
    args.preview = False
    cmd_build_crypto_filter(args)

    # ── Step 5: process orderfilled → trades ────────────────────────────
    logger.info("=== Step 5: process-historical（orderfilled → trades）===")
    args.batch_size = getattr(args, 'batch_size', 1_000_000)
    args.test_batches = None
    args.continue_from = False
    cmd_process_historical(args)

    # ── Step 6: clean trades → quant + users ────────────────────────────
    logger.info("=== Step 6: clean（trades → quant + users）===")
    args.test = None
    cmd_clean(args)

    logger.info("=== sync 全流程完成 ===")


def main():
    parser = argparse.ArgumentParser(description='Polymarket 数据工具')
    parser.add_argument('-v', '--verbose', action='store_true')
    subparsers = parser.add_subparsers(dest='command')

    # fetch-onchain
    p1 = subparsers.add_parser('fetch-onchain', help='获取链上数据')
    p1.add_argument('-b', '--blocks', type=int, help='最近N个区块')
    p1.add_argument('-r', '--range', nargs=2, type=int, metavar=('START', 'END'))
    p1.add_argument('-c', '--continue', dest='continue_from', action='store_true')
    p1.add_argument('-a', '--alchemy', action='store_true')
    p1.add_argument('-m', '--merge', action='store_true',
                    help='完成后合并临时文件到主文件（默认不合并）')

    # sync（推荐日常使用）
    p_sync = subparsers.add_parser('sync', help='完整同步：市场元数据 + 链上数据 + process + clean')
    p_sync.add_argument('-a', '--alchemy', action='store_true', help='使用 Alchemy RPC')
    p_sync.add_argument('--no-process', action='store_true', help='只同步链上数据，跳过 process/clean')

    # fetch-markets
    p2 = subparsers.add_parser('fetch-markets', help='增量获取新市场')
    p2.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                    help='从上次断点继续获取')

    # update-markets
    p2b = subparsers.add_parser('update-markets', help='更新未resolved市场状态')
    p2b.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                     help='从上次断点继续更新')

    # process
    p3 = subparsers.add_parser('process', help='处理交易数据（小数据集）')
    p3.add_argument('--skip-missing', action='store_true', help='跳过缺失 token 补全')

    # process-historical
    p_hist = subparsers.add_parser('process-historical', help='分批处理历史大文件')
    p_hist.add_argument('-b', '--batch-size', type=int, default=1000000,
                        help='每批处理行数（默认100万）')
    p_hist.add_argument('-c', '--continue', dest='continue_from', action='store_true',
                        help='从上次断点继续处理')
    p_hist.add_argument('--test-batches', type=int, default=None,
                        help='测试模式：只处理前N批')

    # clean-users
    p4 = subparsers.add_parser('clean-users', help='清洗用户数据')
    p4.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='批处理大小')
    p4.add_argument('-t', '--test', type=int, default=None, help='测试模式：只处理前N行')

    # clean-trades
    p5 = subparsers.add_parser('clean-trades', help='清洗交易数据（量化用）')
    p5.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='批处理大小')
    p5.add_argument('-t', '--test', type=int, default=None, help='测试模式：只处理前N行')

    # clean (both)
    p6 = subparsers.add_parser('clean', help='运行所有数据清洗')
    p6.add_argument('-b', '--batch-size', type=int, default=5_000_000, help='批处理大小')
    p6.add_argument('-t', '--test', type=int, default=None, help='测试模式：只处理前N行')

    # update
    p7 = subparsers.add_parser('update', help='全量更新')
    p7.add_argument('-a', '--alchemy', action='store_true')
    p7.add_argument('--skip-missing', action='store_true', help='跳过缺失 token 补全')
    p7.add_argument('--clean', dest='with_clean', action='store_true', help='同时运行数据清洗')

    # merge-sessions
    p8 = subparsers.add_parser('merge-sessions', help='合并所有 session 文件到主文件')

    # build-crypto-filter
    p_crypto = subparsers.add_parser(
        'build-crypto-filter',
        help='生成加密市场 ID 列表（启用后 sync/process 只处理加密市场）'
    )
    p_crypto.add_argument(
        '--preview', action='store_true',
        help='只预览匹配结果，不写入文件'
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command == 'sync':
        cmd_sync(args)
    elif args.command == 'build-crypto-filter':
        cmd_build_crypto_filter(args)
    elif args.command == 'fetch-onchain':
        cmd_fetch_onchain(args)
    elif args.command == 'fetch-markets':
        cmd_fetch_markets(args)
    elif args.command == 'update-markets':
        cmd_update_markets(args)
    elif args.command == 'process':
        cmd_process(args)
    elif args.command == 'process-historical':
        cmd_process_historical(args)
    elif args.command == 'clean-users':
        cmd_clean_users(args)
    elif args.command == 'clean-trades':
        cmd_clean_trades(args)
    elif args.command == 'clean':
        cmd_clean(args)
    elif args.command == 'update':
        cmd_update(args)
    elif args.command == 'merge-sessions':
        cmd_merge_sessions(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
