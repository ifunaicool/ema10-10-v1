# analyzer.py
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class IndicatorAnalyzer:
    """技术指标分析器，用于计算MA10平滑指标、生成信号、评分和图表"""

    def __init__(self, ma_period=10, smooth_window=10):
        self.ma_period = ma_period
        self.smooth_window = smooth_window

    def calculate_indicators(self, df):
        """计算技术指标 - 10日窗口平滑MA10（要求DataFrame含中文列名）"""
        # 计算基础MA10
        df[f'MA{self.ma_period}'] = df['收盘'].rolling(window=self.ma_period).mean()

        # 对MA10进行二次平滑（10日移动平均）
        df[f'SmoothMA{self.ma_period}'] = df[f'MA{self.ma_period}'].rolling(
            window=self.smooth_window, min_periods=1
        ).mean()

        # 计算斜率
        df[f'SmoothMA{self.ma_period}_slope'] = df[f'SmoothMA{self.ma_period}'].diff()

        # 计算斜率变化幅度
        df[f'SmoothMA{self.ma_period}_slope_change'] = df[f'SmoothMA{self.ma_period}_slope'].abs()

        # 计算原始MA10的斜率
        df[f'MA{self.ma_period}_slope'] = df[f'MA{self.ma_period}'].diff()

        return df

    def generate_signals(self, df):
        """生成交易信号（要求DataFrame已包含指标）"""
        df['signal'] = None
        df['signal_type'] = None
        df['signal_strength'] = None

        smooth_ma_col = f'SmoothMA{self.ma_period}'

        for i in range(self.ma_period + self.smooth_window, len(df)):
            current_slope = df.iloc[i][f'{smooth_ma_col}_slope']
            prev_slope = df.iloc[i-1][f'{smooth_ma_col}_slope']
            slope_change = df.iloc[i][f'{smooth_ma_col}_slope_change']

            # 斜率由负转正且信号强度达标，生成买入信号
            if prev_slope <= 0 and current_slope > 0 and slope_change > 0.001:
                df.loc[df.index[i], 'signal'] = 'BUY'
                df.loc[df.index[i], 'signal_type'] = 1
                df.loc[df.index[i], 'signal_strength'] = slope_change

            # 斜率由正转负，生成卖出信号
            elif prev_slope >= 0 and current_slope < 0:
                df.loc[df.index[i], 'signal'] = 'SELL'
                df.loc[df.index[i], 'signal_type'] = -1
                df.loc[df.index[i], 'signal_strength'] = slope_change

        return df

    def get_latest_buy_signal(self, df):
        """
        从已生成信号的DataFrame中提取最近一次买入信号及其强度
        返回 (signal_date, signal_strength, 是否存在买入信号)
        """
        buy_signals = df[df['signal'] == 'BUY']
        if len(buy_signals) == 0:
            return None, None, False
        latest = buy_signals.iloc[-1]
        return latest['日期'], latest['signal_strength'], True

    def create_comprehensive_chart(self, stock_code, stock_name, df_signals, rank=None):
        """
        创建综合性图表（四合一）
        参数：
            stock_code: 股票代码
            stock_name: 股票名称
            df_signals: 包含指标和信号的DataFrame（必须含中文列名）
            rank: 排名（用于标题）
        返回 plotly.graph_objects.Figure
        """
        if df_signals is None or len(df_signals) == 0:
            return None

        smooth_ma_col = f'SmoothMA{self.ma_period}'

        # 创建子图
        fig = make_subplots(
            rows=4, cols=1,
            row_heights=[0.4, 0.2, 0.2, 0.2],
            subplot_titles=(
                f'#{stock_name} ({stock_code}) - 价格与MA趋势',
                '成交量变化',
                '斜率趋势分析',
                '信号强度评估'
            ),
            vertical_spacing=0.08
        )

        # 主图：K线图
        fig.add_trace(
            go.Candlestick(
                x=df_signals['日期'],
                open=df_signals['开盘'],
                high=df_signals['最高'],
                low=df_signals['最低'],
                close=df_signals['收盘'],
                name='K线',
                increasing_line_color='#ff5252',
                decreasing_line_color='#00e676'
            ),
            row=1, col=1
        )

        # 原始MA10（增加可见性，使用实线加粗）
        fig.add_trace(
            go.Scatter(
                x=df_signals['日期'],
                y=df_signals[f'MA{self.ma_period}'],
                mode='lines',
                name=f'原始MA{self.ma_period}',
                line=dict(color='#ffa726', width=2, dash='dash'),  # 修改为更明显的橙色虚线
                opacity=0.8
            ),
            row=1, col=1
        )

        # 平滑MA10
        fig.add_trace(
            go.Scatter(
                x=df_signals['日期'],
                y=df_signals[smooth_ma_col],
                mode='lines',
                name=f'平滑MA{self.ma_period}',
                line=dict(color='#2979ff', width=3)  # 加粗
            ),
            row=1, col=1
        )

        # 买卖信号：将信号点绘制在平滑MA10线上（而非收盘价）
        buy_signals = df_signals[df_signals['signal'] == 'BUY']
        sell_signals = df_signals[df_signals['signal'] == 'SELL']

        if len(buy_signals) > 0:
            fig.add_trace(
                go.Scatter(
                    x=buy_signals['日期'],
                    y=buy_signals[smooth_ma_col],  # 使用平滑MA10的值
                    mode='markers',
                    name='买入信号',
                    marker=dict(
                        symbol='triangle-up',
                        size=20,
                        color='#00e676',
                        line=dict(color='white', width=2)
                    ),
                    text=[f"买入<br>价格: {price:.2f}<br>强度: {strength:.4f}<br>日期: {date.strftime('%Y-%m-%d')}"
                          for price, strength, date in zip(buy_signals[smooth_ma_col], buy_signals['signal_strength'], buy_signals['日期'])],
                    hovertemplate='%{text}<extra></extra>'
                ),
                row=1, col=1
            )

        if len(sell_signals) > 0:
            fig.add_trace(
                go.Scatter(
                    x=sell_signals['日期'],
                    y=sell_signals[smooth_ma_col],  # 使用平滑MA10的值
                    mode='markers',
                    name='卖出信号',
                    marker=dict(
                        symbol='triangle-down',
                        size=20,
                        color='#ff5252',
                        line=dict(color='white', width=2)
                    ),
                    text=[f"卖出<br>价格: {price:.2f}<br>日期: {date.strftime('%Y-%m-%d')}"
                          for price, date in zip(sell_signals[smooth_ma_col], sell_signals['日期'])],
                    hovertemplate='%{text}<extra></extra>'
                ),
                row=1, col=1
            )

        # 成交量图
        colors = ['#ff5252' if close >= open_ else '#00e676'
                 for close, open_ in zip(df_signals['收盘'], df_signals['开盘'])]
        fig.add_trace(
            go.Bar(
                x=df_signals['日期'],
                y=df_signals['成交量'],
                name='成交量',
                marker_color=colors,
                opacity=0.7
            ),
            row=2, col=1
        )

        # 斜率图
        fig.add_trace(
            go.Scatter(
                x=df_signals['日期'],
                y=df_signals[f'{smooth_ma_col}_slope'],
                mode='lines',
                name='斜率趋势',
                line=dict(color='#7c4dff', width=2)
            ),
            row=3, col=1
        )

        # 斜率零轴
        fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)

        # 斜率信号点（同样使用平滑MA10值）
        if len(buy_signals) > 0:
            fig.add_trace(
                go.Scatter(
                    x=buy_signals['日期'],
                    y=buy_signals[f'{smooth_ma_col}_slope'],
                    mode='markers',
                    name='买入拐点',
                    marker=dict(
                        symbol='star',
                        size=12,
                        color='#00e676'
                    ),
                    showlegend=False
                ),
                row=3, col=1
            )

        # 信号强度图
        df_signals_filtered = df_signals[df_signals['signal_strength'].notna()]
        if len(df_signals_filtered) > 0:
            fig.add_trace(
                go.Bar(
                    x=df_signals_filtered['日期'],
                    y=df_signals_filtered['signal_strength'] * 100,
                    name='信号强度',
                    marker_color=['#00e676' if sig == 'BUY' else '#ff5252'
                               for sig in df_signals_filtered['signal']],
                    opacity=0.8
                ),
                row=4, col=1
            )

        # 更新布局
        title_prefix = f"TOP{rank} - " if rank else ""
        fig.update_layout(
            template='plotly_dark',
            height=1200,
            showlegend=True,
            hovermode='x unified',
            title=dict(
                text=f"{title_prefix}{stock_name} ({stock_code}) - MA{self.ma_period}平滑策略深度分析",
                x=0.5,
                xanchor='center',
                font=dict(size=18, color='white')
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        # 更新坐标轴
        fig.update_xaxes(title_text="日期", gridcolor='rgba(128,128,128,0.2)', row=4, col=1)
        fig.update_yaxes(title_text="价格", gridcolor='rgba(128,128,128,0.2)', row=1, col=1)
        fig.update_yaxes(title_text="成交量", gridcolor='rgba(128,128,128,0.2)', row=2, col=1)
        fig.update_yaxes(title_text="斜率", gridcolor='rgba(128,128,128,0.2)', row=3, col=1)
        fig.update_yaxes(title_text="信号强度(x100)", gridcolor='rgba(128,128,128,0.2)', row=4, col=1)

        return fig
