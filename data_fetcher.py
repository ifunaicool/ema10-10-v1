# data_fetcher.py
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import requests
import warnings
import threading
from typing import Dict, List, Tuple, Optional, Union, Any, Callable
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stock_data_fetcher.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)


class MarketType(Enum):
    """市场类型枚举"""
    SHANGHAI = "sh"  # 上海
    SHENZHEN = "sz"  # 深圳
    BEIJING = "bj"   # 北京


class AdjustType(Enum):
    """复权类型枚举"""
    NO_ADJUST = ""      # 不复权
    QFQ = "qfq"         # 前复权
    HFQ = "hfq"         # 后复权


@dataclass
class StockDataConfig:
    """股票数据配置类"""
    request_interval: float = 0.8      # 请求间隔
    max_retries: int = 3               # 最大重试次数
    batch_interval: float = 3.0        # 批次间隔
    batch_size: int = 15               # 批次大小
    cache_enabled: bool = True         # 是否启用缓存
    timeout: int = 30                  # 请求超时时间
    max_workers: int = 5               # 最大线程数
    retry_delay_factor: float = 1.5    # 重试延迟因子


class StockDataFetcher:
    """股票数据获取器 - 生产环境稳定版本"""

    # 市场前缀映射
    MARKET_PREFIX_MAP = {
        '6': MarketType.SHANGHAI,     # 上海主板
        '900': MarketType.SHANGHAI,   # 上海B股
        '0': MarketType.SHENZHEN,     # 深圳主板
        '002': MarketType.SHENZHEN,   # 深圳中小板
        '3': MarketType.SHENZHEN,     # 深圳创业板
        '4': MarketType.BEIJING,      # 北京交易所
        '8': MarketType.BEIJING       # 北京交易所
    }

    # 标准列名（统一使用英文小写）
    STANDARD_COLUMNS = [
        'date', 'open', 'close', 'high', 'low',
        'volume', 'turnover', 'outstanding_share', 'amount'
    ]

    # 数值列
    NUMERIC_COLUMNS = [
        'open', 'close', 'high', 'low', 'volume',
        'turnover', 'outstanding_share', 'amount'
    ]

    def __init__(self, config: StockDataConfig = None):
        """
        初始化股票数据获取器

        Args:
            config: 配置对象，如果为None则使用默认配置
        """
        self.config = config or StockDataConfig()

        # 缓存系统
        self.cache = {}
        self.cache_expiry = {}  # 缓存过期时间

        # 请求统计
        self.request_count = 0
        self.request_times = []
        self.failed_requests = 0
        self.successful_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0

        # 创建会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        self.session.timeout = self.config.timeout

        # 请求锁，确保线程安全
        self.request_lock = threading.RLock()

        logger.info(f"StockDataFetcher初始化完成，配置: {self.config}")

    def _get_market_type(self, code: str) -> MarketType:
        """
        根据股票代码获取市场类型

        Args:
            code: 6位股票代码

        Returns:
            MarketType枚举
        """
        code_str = str(code).zfill(6)

        for prefix, market in self.MARKET_PREFIX_MAP.items():
            if code_str.startswith(prefix):
                return market

        # 默认返回深圳市场
        return MarketType.SHENZHEN

    def _normalize_code(self, code: Union[str, int]) -> str:
        """
        标准化股票代码

        Args:
            code: 股票代码

        Returns:
            6位标准化股票代码
        """
        if not code:
            raise ValueError("股票代码不能为空")

        code_str = str(code).strip()

        # 移除市场前缀
        if code_str.startswith(('sh', 'sz', 'bj')):
            code_str = code_str[2:]

        # 补全为6位
        code_str = code_str.zfill(6)

        # 验证格式
        if not code_str.isdigit() or len(code_str) != 6:
            raise ValueError(f"股票代码格式错误: {code}")

        return code_str

    def _create_symbol(self, code: str) -> str:
        """
        创建akshare需要的symbol格式

        Args:
            code: 标准化股票代码

        Returns:
            akshare格式的symbol
        """
        market = self._get_market_type(code)
        return f"{market.value}{code}"

    def _format_date(self, date_input: Any) -> str:
        """
        格式化日期字符串为YYYYMMDD格式

        Args:
            date_input: 日期输入，可以是datetime对象或字符串

        Returns:
            统一格式的日期字符串
        """
        if date_input is None:
            return ""

        # 如果是datetime对象
        if isinstance(date_input, datetime):
            return date_input.strftime("%Y%m%d")

        # 如果是日期字符串
        date_str = str(date_input).strip()

        # 如果是时间戳
        if date_str.isdigit() and len(date_str) > 8:
            try:
                dt = datetime.fromtimestamp(int(date_str[:10]))
                return dt.strftime("%Y%m%d")
            except:
                pass

        # 如果是YYYYMMDD格式
        if date_str.isdigit() and len(date_str) == 8:
            return date_str

        # 尝试解析各种格式
        date_formats = [
            "%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
            "%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y%m%d")
            except:
                continue

        # 如果无法解析，返回今天
        return datetime.now().strftime("%Y%m%d")

    def _normalize_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        标准化DataFrame格式

        Args:
            df: 原始DataFrame
            symbol: 股票标识符

        Returns:
            标准化后的DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()

        # 创建副本
        df = df.copy()

        # 标准化列名
        column_renames = {}
        for col in df.columns:
            col_str = str(col).strip().lower()

            # 日期列
            if col_str in ['date', '日期', 'datetime', 'time']:
                column_renames[col] = 'date'

            # 价格列
            elif col_str in ['open', '开盘']:
                column_renames[col] = 'open'
            elif col_str in ['close', '收盘']:
                column_renames[col] = 'close'
            elif col_str in ['high', '最高']:
                column_renames[col] = 'high'
            elif col_str in ['low', '最低']:
                column_renames[col] = 'low'

            # 交易量列
            elif col_str in ['volume', '成交量', 'vol']:
                column_renames[col] = 'volume'
            elif col_str in ['amount', '成交额', '成交金额']:
                column_renames[col] = 'amount'

            # 换手率和流通股本
            elif col_str in ['turnover', '换手率', 'turnoverrate', 'turnover_rate']:
                column_renames[col] = 'turnover'
            elif col_str in ['outstanding_share', '流通股本', 'float_share', 'floatshare']:
                column_renames[col] = 'outstanding_share'

        if column_renames:
            df = df.rename(columns=column_renames)

        # 确保有date列
        if 'date' not in df.columns and len(df.columns) > 0:
            # 尝试将第一列作为日期
            first_col = df.columns[0]
            if 'date' in str(first_col).lower():
                df = df.rename(columns={first_col: 'date'})
            else:
                # 添加默认日期列（使用索引）
                df.insert(0, 'date', pd.date_range(end=datetime.now(), periods=len(df), freq='B'))

        # 转换日期列
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # 删除无效日期
            df = df.dropna(subset=['date'])

        # 转换数值列
        for col in self.NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 修复换手率数据
        if 'turnover' in df.columns:
            # 检查换手率是否异常（如全部为0）
            if df['turnover'].max() < 0.1:  # 最大换手率小于0.1%
                # 尝试从成交量和流通股本计算换手率
                if 'volume' in df.columns and 'outstanding_share' in df.columns:
                    df['turnover'] = (df['volume'] / df['outstanding_share']) * 100

        # 排序并重置索引
        if 'date' in df.columns and len(df) > 0:
            df = df.sort_values('date').reset_index(drop=True)

        # 只保留标准列
        available_columns = [col for col in self.STANDARD_COLUMNS if col in df.columns]
        if available_columns:
            df = df[available_columns]

        return df

    def _validate_data(self, df: pd.DataFrame, code: str) -> bool:
        """
        验证数据有效性

        Args:
            df: 要验证的数据
            code: 股票代码

        Returns:
            是否有效
        """
        if df is None or df.empty:
            return False

        # 基本验证
        if 'date' not in df.columns:
            return False

        if 'close' not in df.columns:
            return False

        # 检查价格合理性
        if 'close' in df.columns:
            if (df['close'] <= 0).any():
                return False

        # 检查成交量
        if 'volume' in df.columns:
            if (df['volume'] < 0).any():
                return False

        return True

    def _get_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """
        从缓存获取数据

        Args:
            cache_key: 缓存键

        Returns:
            缓存的数据，如果不存在或过期则返回None
        """
        if not self.config.cache_enabled:
            return None

        with self.request_lock:
            if cache_key in self.cache:
                # 检查是否过期（缓存1小时）
                if cache_key in self.cache_expiry:
                    if datetime.now() < self.cache_expiry[cache_key]:
                        self.cache_hits += 1
                        return self.cache[cache_key].copy()
                    else:
                        # 缓存过期，删除
                        del self.cache[cache_key]
                        del self.cache_expiry[cache_key]
                else:
                    self.cache_hits += 1
                    return self.cache[cache_key].copy()

        self.cache_misses += 1
        return None

    def _save_to_cache(self, cache_key: str, data: pd.DataFrame):
        """
        保存数据到缓存

        Args:
            cache_key: 缓存键
            data: 要缓存的数据
        """
        if not self.config.cache_enabled:
            return

        with self.request_lock:
            # 设置缓存过期时间（1小时）
            expiry_time = datetime.now() + timedelta(hours=1)

            self.cache[cache_key] = data.copy()
            self.cache_expiry[cache_key] = expiry_time

            # 清理过期缓存（每100次请求清理一次）
            self.request_count += 1
            if self.request_count % 100 == 0:
                self._clean_cache()

    def _clean_cache(self):
        """清理过期缓存"""
        current_time = datetime.now()
        expired_keys = []

        with self.request_lock:
            for key, expiry in self.cache_expiry.items():
                if current_time > expiry:
                    expired_keys.append(key)

            for key in expired_keys:
                del self.cache[key]
                del self.cache_expiry[key]

    def _throttle_request(self):
        """请求限流"""
        with self.request_lock:
            if self.request_times:
                elapsed = time.time() - self.request_times[-1]
                if elapsed < self.config.request_interval:
                    sleep_time = self.config.request_interval - elapsed
                    time.sleep(sleep_time)

            self.request_times.append(time.time())

    def get_stock_historical_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: Optional[int] = None,
        adjust: Union[str, AdjustType] = AdjustType.HFQ,
        use_cache: bool = True,
        force_fetch: bool = False
    ) -> pd.DataFrame:
        """
        获取股票历史数据

        Args:
            code: 股票代码，如 '000001' 或 'sh000001'
            start_date: 开始日期，格式 'YYYYMMDD' 或 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYYMMDD' 或 'YYYY-MM-DD'
            days: 获取最近N天的数据
            adjust: 复权类型，可选 "hfq"(后复权), "qfq"(前复权), ""(不复权)
            use_cache: 是否使用缓存
            force_fetch: 是否强制从网络获取（忽略缓存）

        Returns:
            股票历史数据DataFrame，包含标准列
        """
        try:
            # 参数验证和标准化
            code = self._normalize_code(code)

            # 处理复权类型
            if isinstance(adjust, AdjustType):
                adjust = adjust.value
            elif adjust not in ['hfq', 'qfq', '']:
                adjust = 'hfq'

            # 处理日期参数
            if end_date is None:
                end_date_dt = datetime.now()
                end_date_str = self._format_date(end_date_dt)
            else:
                end_date_str = self._format_date(end_date)

            if days is not None:
                start_date_dt = datetime.now() - timedelta(days=days+5)
                start_date_str = self._format_date(start_date_dt)
            elif start_date is not None:
                start_date_str = self._format_date(start_date)
            else:
                start_date_dt = datetime.now() - timedelta(days=365)
                start_date_str = self._format_date(start_date_dt)

            # 创建缓存键
            cache_key = f"{code}_{start_date_str}_{end_date_str}_{adjust}"

            # 尝试从缓存获取（除非强制获取）
            if use_cache and not force_fetch:
                cached_data = self._get_from_cache(cache_key)
                if cached_data is not None:
                    logger.info(f"从缓存获取股票 {code} 数据，形状: {cached_data.shape}")
                    self.successful_requests += 1
                    return cached_data

            # 创建symbol
            symbol = self._create_symbol(code)
            logger.info(f"从网络获取股票 {code} 数据，时间范围: {start_date_str} 到 {end_date_str}, symbol: {symbol}")

            # 请求数据
            data = self._fetch_stock_data_with_retry(symbol, start_date_str, end_date_str, adjust)

            if data is None or data.empty:
                logger.warning(f"股票 {code} 数据获取失败")
                self.failed_requests += 1
                return pd.DataFrame()

            # 标准化数据
            data = self._normalize_dataframe(data, symbol)

            # 验证数据
            if not self._validate_data(data, code):
                logger.warning(f"股票 {code} 数据验证失败")
                self.failed_requests += 1
                return pd.DataFrame()

            # 缓存数据
            if use_cache:
                self._save_to_cache(cache_key, data)

            self.successful_requests += 1

            logger.info(f"股票 {code} 网络获取成功，形状: {data.shape}, "
                       f"日期范围: {data['date'].min().date()} 到 {data['date'].max().date()}")

            return data

        except ValueError as e:
            logger.error(f"参数错误[{code}]: {e}")
            self.failed_requests += 1
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取股票 {code} 数据失败: {str(e)}")
            self.failed_requests += 1
            return pd.DataFrame()

    def _fetch_stock_data_with_retry(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str
    ) -> Optional[pd.DataFrame]:
        """
        带重试机制的股票数据获取

        Args:
            symbol: akshare格式的symbol
            start_date: 开始日期
            end_date: 结束日期
            adjust: 复权类型

        Returns:
            股票数据DataFrame
        """
        for attempt in range(self.config.max_retries):
            try:
                # 请求限流
                self._throttle_request()

                # 使用akshare获取数据
                df = ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )

                if df is not None and not df.empty:
                    return df
                else:
                    logger.warning(f"第 {attempt+1} 次尝试获取 {symbol} 数据为空")

            except Exception as e:
                logger.error(f"第 {attempt+1} 次尝试获取 {symbol} 数据失败: {str(e)}")

                # 重试前等待
                if attempt < self.config.max_retries - 1:
                    wait_time = self.config.request_interval * (self.config.retry_delay_factor ** attempt)
                    logger.debug(f"等待 {wait_time:.1f} 秒后重试")
                    time.sleep(wait_time)

        logger.error(f"经过 {self.config.max_retries} 次尝试后仍无法获取 {symbol} 数据")
        return None

    def get_multiple_stocks_data_batch(
        self,
        codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: Optional[int] = None,
        adjust: Union[str, AdjustType] = AdjustType.HFQ,
        use_cache: bool = True,
        force_fetch: bool = False,
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票数据（批次处理）

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            days: 获取最近N天的数据
            adjust: 复权类型
            use_cache: 是否使用缓存
            force_fetch: 是否强制从网络获取（忽略缓存）
            progress_callback: 进度回调函数

        Returns:
            字典，键为股票代码，值为DataFrame
        """
        results = {}
        total = len(codes)

        logger.info(f"开始批量获取 {total} 只股票数据，批次大小: {self.config.batch_size}, 强制获取: {force_fetch}")

        # 清除重复的股票代码
        unique_codes = list(dict.fromkeys(codes))
        if len(unique_codes) != total:
            logger.info(f"去重后实际获取 {len(unique_codes)} 只不同股票的数据")
            total = len(unique_codes)

        # 分批处理
        for batch_start in range(0, total, self.config.batch_size):
            batch_end = min(batch_start + self.config.batch_size, total)
            batch_codes = unique_codes[batch_start:batch_end]

            logger.info(f"处理批次 {batch_start//self.config.batch_size + 1}/{(total-1)//self.config.batch_size + 1}: "
                       f"股票 {batch_start+1} 到 {batch_end}")

            # 处理当前批次
            batch_results = self._process_batch(
                batch_codes, start_date, end_date, days, adjust,
                use_cache, force_fetch, progress_callback,
                batch_start, total
            )

            results.update(batch_results)

            # 批次间隔
            if batch_end < total:
                logger.info(f"批次完成，等待 {self.config.batch_interval} 秒")
                time.sleep(self.config.batch_interval)

        success_count = len(results)
        logger.info(f"批量获取完成，成功获取 {success_count}/{total} 只股票数据")
        logger.info(f"请求统计: 成功 {self.successful_requests}, 失败 {self.failed_requests}, "
                   f"缓存命中 {self.cache_hits}, 缓存未命中 {self.cache_misses}")

        return results

    def _process_batch(
        self,
        batch_codes: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        days: Optional[int],
        adjust: Union[str, AdjustType],
        use_cache: bool,
        force_fetch: bool,
        progress_callback: Optional[Callable],
        offset: int,
        total: int
    ) -> Dict[str, pd.DataFrame]:
        """
        处理单个批次

        Returns:
            当前批次的股票数据字典
        """
        batch_results = {}

        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=min(len(batch_codes), self.config.max_workers)) as executor:
            # 提交任务
            future_to_code = {}
            for i, code in enumerate(batch_codes):
                future = executor.submit(
                    self.get_stock_historical_data,
                    code, start_date, end_date, days, adjust, use_cache, force_fetch
                )
                future_to_code[future] = (code, i + offset + 1)

            # 处理结果
            for future in as_completed(future_to_code):
                code, position = future_to_code[future]
                try:
                    data = future.result(timeout=self.config.timeout * 2)
                    if not data.empty:
                        batch_results[code] = data
                        logger.debug(f"股票 {code} 获取成功，数据形状: {data.shape}")

                    # 进度回调
                    if progress_callback:
                        progress_callback(position, total, code, not data.empty)

                except Exception as e:
                    logger.error(f"处理股票 {code} 时出错: {str(e)}")
                    if progress_callback:
                        progress_callback(position, total, code, False)

        return batch_results

    def get_stock_basic_info(self, code: str) -> Dict[str, Any]:
        """
        获取股票基本信息

        Args:
            code: 股票代码

        Returns:
            股票基本信息字典
        """
        try:
            code = self._normalize_code(code)

            # 获取最近5个交易日数据
            df = self.get_stock_historical_data(code, days=5)

            if df.empty:
                return {}

            # 提取最新数据
            latest = df.iloc[-1]

            # 计算统计信息
            info = {
                'code': code,
                'market': self._get_market_type(code).value.upper(),
                'latest_date': latest['date'].strftime('%Y-%m-%d') if pd.notna(latest['date']) else None,
                'close': float(latest.get('close', 0)),
                'open': float(latest.get('open', 0)),
                'high': float(latest.get('high', 0)),
                'low': float(latest.get('low', 0)),
                'volume': int(latest.get('volume', 0)),
                'amount': float(latest.get('amount', 0)),
                'turnover': float(latest.get('turnover', 0)),
                'outstanding_share': int(latest.get('outstanding_share', 0)),
                'avg_volume_5d': int(df['volume'].mean()) if 'volume' in df.columns else 0,
                'price_change_5d': float(((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0] * 100)
                                        if len(df) >= 2 else 0),
                'volatility_5d': float(df['close'].std() / df['close'].mean() * 100 if len(df) >= 2 else 0)
            }

            # 计算流通市值
            if info['close'] > 0 and info['outstanding_share'] > 0:
                info['float_market_cap'] = info['close'] * info['outstanding_share']
            else:
                info['float_market_cap'] = 0

            return info

        except Exception as e:
            logger.error(f"获取股票 {code} 基本信息失败: {str(e)}")
            return {}

    def clear_cache(self):
        """清空缓存"""
        with self.request_lock:
            self.cache.clear()
            self.cache_expiry.clear()
            self.cache_hits = 0
            self.cache_misses = 0
        logger.info("缓存已清空")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        total_cache_access = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / max(total_cache_access, 1) * 100

        return {
            'total_requests': self.request_count,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'cache_size': len(self.cache),
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': cache_hit_rate,
            'success_rate': self.successful_requests / max(self.request_count, 1) * 100
        }
