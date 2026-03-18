# main.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场MA10平滑策略监控系统 - 最终整合版
功能：
1. 获取沪深所有A股近一年数据（剔除ST/*ST）
2. 计算MA10平滑指标并生成买卖信号
3. 找出离当前日期最近且买入信号最强的20只股票
4. 生成网页展示这20只股票的详细图表，带“下一个”按钮
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import time
import os
from typing import Dict, List, Tuple, Optional

# 导入自定义模块
from config import *
from data_fetcher import StockDataFetcher, StockDataConfig
from analyzer import IndicatorAnalyzer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scanner.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)


def get_all_stocks(exclude_st=True, max_retries=3):
    """
    获取沪深所有A股股票代码列表，可选择剔除ST/*ST股票
    使用 stock_info_a_code_name 接口（更稳定）
    """
    logger.info("正在获取沪深A股股票列表...")
    for attempt in range(max_retries):
        try:
            df = ak.stock_info_a_code_name()
            logger.info(f"获取到 {len(df)} 只股票")

            if exclude_st:
                # 剔除ST、*ST股票
                mask = ~df['name'].str.contains(r'ST|\*ST|退', na=False, regex=True)
                df = df[mask]
                logger.info(f"剔除ST/*ST后剩余 {len(df)} 只股票")

            stock_list = df['code'].tolist()
            logger.info(f"最终有效股票数量: {len(stock_list)}")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败 (尝试 {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 指数退避
                logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error("所有重试均失败，请检查网络连接或稍后再试。")
                return []


def analyze_stock_data(stock_code: str, df_en: pd.DataFrame, analyzer: IndicatorAnalyzer) -> Optional[Dict]:
    """
    分析单只股票数据，计算指标、信号，提取最近买入信号
    """
    if df_en.empty:
        return None

    # 将英文列名转换为中文（指标计算需要）
    rename_map = {
        'date': '日期',
        'open': '开盘',
        'close': '收盘',
        'high': '最高',
        'low': '最低',
        'volume': '成交量',
        'amount': '成交额'
    }
    df = df_en.rename(columns={k: v for k, v in rename_map.items() if k in df_en.columns})

    # 确保有必要的列
    required_cols = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
    if not all(col in df.columns for col in required_cols):
        logger.warning(f"股票 {stock_code} 缺少必要列，跳过")
        return None

    # 计算指标和信号
    df = analyzer.calculate_indicators(df)
    df = analyzer.generate_signals(df)

    # 提取最近一次买入信号
    signal_date, signal_strength, has_buy = analyzer.get_latest_buy_signal(df)
    if not has_buy:
        return None

    # 获取股票名称（使用akshare单独获取）
    try:
        # 尝试从 stock_info_a_code_name 获取名称（但我们已经有了df，不过为了准确，还是用akshare获取）
        # 简单起见，直接使用传入的stock_code作为名称的一部分，或者从之前的列表获取，但这里重新获取一次
        # 这里我们先用stock_code代替，稍后可以从之前的数据中获取，但为了简化，我们调用akshare获取
        name_df = ak.stock_info_a_code_name()
        name_row = name_df[name_df['code'] == stock_code]
        if not name_row.empty:
            stock_name = name_row.iloc[0]['name']
        else:
            stock_name = stock_code
    except:
        stock_name = stock_code

    # 返回结果（包含信号数据和DataFrame用于绘图）
    return {
        'code': stock_code,
        'name': stock_name,
        'signal_date': signal_date,
        'signal_strength': signal_strength,
        'df': df  # 包含指标和信号的DataFrame
    }


def main():
    logger.info("=" * 60)
    logger.info("开始执行全市场MA10扫描任务")
    logger.info("=" * 60)

    # 1. 配置数据获取器
    data_config = StockDataConfig(
        request_interval=REQUEST_INTERVAL,
        max_retries=MAX_RETRIES,
        batch_size=BATCH_SIZE,
        batch_interval=BATCH_INTERVAL,
        cache_enabled=CACHE_ENABLED,
        timeout=TIMEOUT,
        max_workers=MAX_WORKERS
    )
    fetcher = StockDataFetcher(data_config)

    # 2. 创建分析器
    analyzer = IndicatorAnalyzer(ma_period=MA_PERIOD, smooth_window=SMOOTH_WINDOW)

    # 3. 获取股票列表
    stock_codes = get_all_stocks(exclude_st=True)
    if not stock_codes:
        logger.error("没有获取到股票列表，退出")
        return

    # 4. 批量获取数据
    logger.info(f"开始获取 {len(stock_codes)} 只股票近 {YEARS} 年的数据...")
    days_needed = YEARS * 365 + 50  # 多取一些避免边界问题
    data_dict = fetcher.get_multiple_stocks_data_batch(
        codes=stock_codes,
        days=days_needed,
        adjust='hfq',
        use_cache=True,
        force_fetch=False
    )
    logger.info(f"成功获取 {len(data_dict)} 只股票的数据")

    # 5. 分析每只股票，收集有买入信号的结果
    buy_signals = []
    for code, df in data_dict.items():
        result = analyze_stock_data(code, df, analyzer)
        if result:
            buy_signals.append(result)

    logger.info(f"共发现 {len(buy_signals)} 只股票有买入信号")

    if not buy_signals:
        logger.warning("没有找到任何买入信号，退出")
        return

    # 6. 按信号强度排序，取前20（强度相同则按日期近优先）
    buy_signals.sort(key=lambda x: (x['signal_strength'], x['signal_date']), reverse=True)
    top20 = buy_signals[:20]

    logger.info("前20只买入信号最强的股票：")
    for i, item in enumerate(top20, 1):
        logger.info(f"{i:2d}. {item['code']} {item['name']} 强度: {item['signal_strength']:.6f} 日期: {item['signal_date'].strftime('%Y-%m-%d')}")

    # 7. 为前20只股票生成图表，并保存为HTML嵌入数据
    chart_data = []
    for idx, item in enumerate(top20, 1):
        fig = analyzer.create_comprehensive_chart(
            stock_code=item['code'],
            stock_name=item['name'],
            df_signals=item['df'],
            rank=idx
        )
        if fig:
            # 将图表转换为JSON，以便在HTML中渲染
            chart_json = fig.to_json()
            chart_data.append({
                'rank': idx,
                'code': item['code'],
                'name': item['name'],
                'json': chart_json
            })

    # 8. 生成HTML文件
    template_path = os.path.join('templates', 'chart_viewer.html')
    if not os.path.exists(template_path):
        logger.error(f"模板文件 {template_path} 不存在，请确保 templates 目录下包含 chart_viewer.html")
        return

    with open(template_path, 'r', encoding='utf-8') as f:
        html_template = f.read()

    # 将chart_data转换为JavaScript数组
    import json
    charts_js = json.dumps(chart_data, ensure_ascii=False)

    # 替换占位符
    html_content = html_template.replace('{{CHART_DATA}}', charts_js)

    output_file = OUTPUT_HTML
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    logger.info(f"HTML文件已生成: {output_file}")
    logger.info("任务完成！")


if __name__ == "__main__":
    main()
