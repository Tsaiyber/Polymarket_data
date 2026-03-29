#!/usr/bin/env python3
"""
持续获取最新区块数据的脚本

功能：
1. 持续监控区块链，获取最新的区块数据
2. 自动切换模式：批量获取历史区块 -> 实时跟踪新区块（2秒一个）
3. 流式追加写入到 4 个 parquet 文件：orderfilled, trades, users, quant
4. 优雅退出，确保文件完整性

用法：
    # 在后台运行
    nohup python scripts/continuous_fetch.py > logs/continuous_fetch.log 2>&1 &

    # 停止（会优雅退出）
    kill -SIGTERM <PID>

    # 自定义输出目录
    python scripts/continuous_fetch.py --output-dir data/realtime
"""
import os
import sys
from pathlib import Path
import time
import signal
import argparse
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from polymarket.fetchers.rpc import LogFetcher
from polymarket.processors import (
    EventDecoder,
    extract_trades,
    load_token_mapping,
    clean_trades_df,
    clean_users_df
)
from polymarket.config import MARKETS_FILE, MISSING_MARKETS_FILE
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ContinuousWriter:
    """持续追加写入 parquet 的管理类"""

    def __init__(self, output_dir, session_timestamp, preview_size=1000):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session_timestamp = session_timestamp
        self.preview_size = preview_size

        # 4个输出文件（带时间戳）
        self.files = {
            'orderfilled': self.output_dir / f'orderfilled_{session_timestamp}.parquet',
            'trades': self.output_dir / f'trades_{session_timestamp}.parquet',
            'quant': self.output_dir / f'quant_{session_timestamp}.parquet',
            'users': self.output_dir / f'users_{session_timestamp}.parquet',
        }

        # CSV 预览文件（固定名称，实时更新）
        preview_dir = self.output_dir.parent / 'latest_result'
        preview_dir.mkdir(parents=True, exist_ok=True)
        self.csv_files = {
            'orderfilled': preview_dir / 'orderfilled.csv',
            'trades': preview_dir / 'trades.csv',
            'quant': preview_dir / 'quant.csv',
            'users': preview_dir / 'users.csv',
        }

        # ParquetWriter 实例
        self.writers = {
            'orderfilled': None,
            'trades': None,
            'quant': None,
            'users': None,
        }

        # 记录行数
        self.row_counts = {
            'orderfilled': 0,
            'trades': 0,
            'quant': 0,
            'users': 0,
        }

        # 缓存最近的数据（用于 CSV 预览）
        self.recent_data = {
            'orderfilled': [],
            'trades': [],
            'quant': [],
            'users': [],
        }

        # 输出文件信息
        logger.info(f"本次会话文件:")
        for name, file_path in self.files.items():
            logger.info(f"  {name}: {file_path.name}")
        logger.info(f"CSV 预览: data/latest_result/ (最新 {preview_size} 条)")

    def write_batch(self, data_type, data):
        """追加写入一批数据"""
        if data is None or len(data) == 0:
            return

        # 转换为 DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            return

        if len(df) == 0:
            return

        try:
            # 转换为 Arrow Table
            table = pa.Table.from_pandas(df)

            # 初始化 writer（如果还没有）
            if self.writers[data_type] is None:
                file_path = self.files[data_type]

                # 创建新文件
                self.writers[data_type] = pq.ParquetWriter(
                    file_path,
                    table.schema,
                    compression='snappy',
                )
                logger.info(f"✓ 创建新文件 {data_type}: {file_path.name}")

            # 写入数据
            self.writers[data_type].write_table(table)
            self.row_counts[data_type] += len(df)

            # 更新缓存（保留最新的 preview_size 条）
            if isinstance(data, list):
                self.recent_data[data_type].extend(data)
            else:
                self.recent_data[data_type].extend(df.to_dict('records'))

            # 只保留最新的 N 条
            if len(self.recent_data[data_type]) > self.preview_size:
                self.recent_data[data_type] = self.recent_data[data_type][-self.preview_size:]

            # 更新 CSV 预览
            self._update_csv_preview(data_type)

        except Exception as e:
            logger.error(f"写入 {data_type} 失败: {e}")
            raise

    def _update_csv_preview(self, data_type):
        """更新 CSV 预览文件"""
        try:
            if len(self.recent_data[data_type]) > 0:
                df = pd.DataFrame(self.recent_data[data_type])
                csv_file = self.csv_files[data_type]
                df.to_csv(csv_file, index=False)
        except Exception as e:
            logger.warning(f"更新 CSV 预览失败 ({data_type}): {e}")

    def close_all(self):
        """关闭所有 writer"""
        logger.info("正在关闭所有文件...")
        for name, writer in self.writers.items():
            if writer is not None:
                try:
                    writer.close()
                    logger.info(f"  ✓ {name}: {self.row_counts[name]:,} 行")
                except Exception as e:
                    logger.error(f"  关闭 {name} 失败: {e}")
        logger.info("所有文件已关闭")


class ContinuousFetcher:
    """持续获取区块数据"""

    def __init__(self, output_dir, batch_size=100):
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size

        # 状态文件
        self.state_file = self.output_dir / 'continuous_state.json'

        # 生成本次会话的时间戳
        self.session_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Writer
        self.writer = ContinuousWriter(output_dir, self.session_timestamp)

        # 初始化 fetcher 和 decoder
        self.fetcher = LogFetcher()
        self.decoder = EventDecoder()

        # 加载 token 映射
        self.token_mapping = load_token_mapping(MARKETS_FILE)
        if MISSING_MARKETS_FILE.exists():
            self.token_mapping.update(load_token_mapping(MISSING_MARKETS_FILE))
        logger.info(f"加载 {len(self.token_mapping)} 个 token 映射")

        # 加载状态
        self.last_processed_block = self.load_state()

        # 信号处理
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """处理停止信号"""
        logger.info(f"\n收到停止信号 ({signum})，准备安全退出...")
        self.should_stop = True

    def load_state(self):
        """加载上次处理到的区块号"""
        if self.state_file.exists():
            import json
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    return state.get('last_block', None)
            except:
                return None
        return None

    def save_state(self, block_number):
        """保存当前处理到的区块号"""
        import json
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'last_block': block_number,
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, f, indent=2)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

    def get_latest_block(self):
        """获取链上最新区块号"""
        try:
            latest = self.fetcher.client.get_latest_block()
            return latest
        except Exception as e:
            logger.error(f"获取最新区块失败: {e}")
            return None

    def fetch_and_process_range(self, start_block, end_block):
        """获取并处理一个区块范围"""
        try:
            # 获取日志
            logs = self.fetcher.fetch_range_in_batches(start_block, end_block)
            if logs is None or len(logs) == 0:
                logger.info(f"区块 {start_block:,}-{end_block:,} 无数据")
                return True

            logger.info(f"  获取到 {len(logs)} 条日志")

            # 解码并格式化事件
            decoded = [self.decoder.decode(log) for log in logs]
            events = [self.decoder.format_event(e) for e in decoded]
            if len(events) == 0:
                logger.info(f"  解码后无数据")
                return True

            logger.info(f"  解码 {len(events)} 条事件")

            # 提取 trades
            trades = extract_trades(events, self.token_mapping)
            logger.info(f"  提取 {len(trades)} 条交易")

            # 写入 orderfilled 和 trades
            self.writer.write_batch('orderfilled', events)
            self.writer.write_batch('trades', trades)

            # 清洗数据（仅当有交易数据时）
            if len(trades) > 0:
                trades_df = pd.DataFrame(trades)
                quant_df = clean_trades_df(trades_df)
                users_df = clean_users_df(trades_df)
                self.writer.write_batch('quant', quant_df)
                self.writer.write_batch('users', users_df)

            logger.info(f"  ✓ 已写入所有数据")
            return True

        except Exception as e:
            logger.error(f"处理区块 {start_block}-{end_block} 失败: {e}")
            return False

    def run(self):
        """主循环：持续获取新区块"""
        logger.info("\n" + "="*60)
        logger.info("=== 持续获取模式启动 ===")
        logger.info("="*60)
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"批次大小: {self.batch_size} 个区块")
        logger.info("按 Ctrl+C 或发送 SIGTERM 信号优雅退出")
        logger.info("="*60 + "\n")

        # 确定起始区块
        if self.last_processed_block is None:
            latest_block = self.get_latest_block()
            if latest_block is None:
                logger.error("无法获取最新区块，退出")
                return
            # 从最新区块往前 100 个区块开始
            self.last_processed_block = latest_block - self.batch_size
            logger.info(f"首次运行，从区块 {self.last_processed_block:,} 开始\n")
        else:
            logger.info(f"继续从区块 {self.last_processed_block:,} 开始\n")

        consecutive_errors = 0
        max_errors = 10
        last_log_time = time.time()

        try:
            while not self.should_stop:
                try:
                    # 获取最新区块
                    latest_block = self.get_latest_block()
                    if latest_block is None:
                        consecutive_errors += 1
                        if consecutive_errors >= max_errors:
                            logger.error(f"连续 {max_errors} 次获取最新区块失败")
                            break
                        time.sleep(5)
                        continue

                    consecutive_errors = 0
                    next_block = self.last_processed_block + 1

                    # 检查是否有新区块
                    if next_block > latest_block:
                        # 已经是最新了，等待 2 秒
                        if time.time() - last_log_time > 30:
                            logger.info(f"[实时模式] 当前: {self.last_processed_block:,}, 最新: {latest_block:,}, 等待新区块...")
                            last_log_time = time.time()
                        time.sleep(2)
                        continue

                    # 计算要处理的范围
                    blocks_behind = latest_block - self.last_processed_block

                    if blocks_behind >= self.batch_size:
                        # 批量模式：一次处理 100 个区块
                        end_block = next_block + self.batch_size - 1
                        logger.info(f"[批量模式] 处理 {next_block:,} - {end_block:,} (落后 {blocks_behind:,} 个区块)")
                        success = self.fetch_and_process_range(next_block, end_block)

                        if success:
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.info(f"✓ 更新状态: {end_block:,}\n")
                        else:
                            time.sleep(5)
                            continue

                        # 继续下一批，不等待
                        time.sleep(0.5)
                    else:
                        # 实时模式：一次处理 1 个区块
                        end_block = next_block
                        logger.info(f"[实时模式] 处理区块 {next_block:,} (最新: {latest_block:,})")
                        success = self.fetch_and_process_range(next_block, end_block)

                        if success:
                            self.last_processed_block = end_block
                            self.save_state(end_block)
                            logger.info(f"✓ 更新状态: {end_block:,}\n")
                        else:
                            time.sleep(5)
                            continue

                        # 实时模式：等待 2 秒
                        last_log_time = time.time()
                        time.sleep(2)

                except Exception as e:
                    logger.error(f"循环错误: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_errors:
                        logger.error(f"连续 {max_errors} 次错误，退出")
                        break
                    time.sleep(5)

        finally:
            # 优雅退出
            logger.info("\n" + "="*60)
            logger.info("正在安全退出...")
            self.writer.close_all()
            logger.info("="*60)
            logger.info("=== 持续获取模式已安全退出 ===")
            logger.info("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(description='持续获取最新区块数据')
    parser.add_argument('--output-dir', type=str, default='data/continuous',
                       help='输出目录，默认 data/continuous')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='批量模式时每次获取的区块数，默认 100')

    args = parser.parse_args()

    fetcher = ContinuousFetcher(
        output_dir=args.output_dir,
        batch_size=args.batch_size
    )

    fetcher.run()


if __name__ == '__main__':
    main()
