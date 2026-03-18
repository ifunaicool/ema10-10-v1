# config.py
"""
MA10扫描器配置文件
"""

# 策略参数
MA_PERIOD = 10          # MA周期
SMOOTH_WINDOW = 10      # 平滑窗口
YEARS = 1               # 历史数据年数

# 数据获取参数
REQUEST_INTERVAL = 0.8   # 请求间隔（秒）
MAX_RETRIES = 3          # 最大重试次数
BATCH_SIZE = 15          # 批次大小
BATCH_INTERVAL = 3.0     # 批次间隔
CACHE_ENABLED = True     # 启用缓存
TIMEOUT = 30             # 超时时间
MAX_WORKERS = 8          # 并发线程数

# 股票筛选
STOCK_PRICE_MIN = 0      # 最低股价
STOCK_PRICE_MAX = 1000   # 最高股价

# 输出文件
OUTPUT_HTML = "top20_signals.html"   # 生成的网页文件名
