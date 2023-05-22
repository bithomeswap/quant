import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class QuantTrader:
    def __init__(self):
        # 获取当前.py文件的绝对路径
        file_path = os.path.abspath(__file__)
        # 获取当前.py文件所在目录的路径
        dir_path = os.path.dirname(file_path)
        # 获取数据所在目录的路径
        self.data_dir = os.path.join(os.path.dirname(
            os.path.dirname(dir_path)), 'data')
        if not os.path.exists(self.data_dir):
            raise FileNotFoundError(f"数据文件夹{self.data_dir}不存在")

        self.m = 0.001  # 手续费比例
        self.n = 6  # 持仓周期
        self.w = 1.0  # 资金贡献占比因子

    def get_data(self, name):
        """
        加载数据
        """
        file_name = f"{name}.csv"
        file_path = os.path.join(self.data_dir, file_name)
        return pd.read_csv(file_path)

    def preprocess_data(self, df):
        """
        数据预处理，计算相关指标
        """
        # 去掉噪音数据
        for n in range(1, 9):
            df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]

        # 计算排名
        df['昨日资金波动_rank'] = df['昨日资金波动'].rank(ascending=False)
        df['昨日资金贡献_rank'] = df['昨日资金贡献'].rank(ascending=False)

        return df

    def strategy(self, df):
        """
        选股策略
        """
        # 获取所有日期
        dates = df['日期'].unique()

        result_list = []
        for date in dates:
            # 取从n天前到当天的数据
            end_date = datetime.strptime(date, '%Y-%m-%d')
            start_date = (end_date - timedelta(days=self.n)
                          ).strftime('%Y-%m-%d')
            mask = (df['日期'] >= start_date) & (df['日期'] <= date)
            day_df = df.loc[mask].copy()

            # 进行策略选股
            # 资金波动排名在前w%的股票，且资金贡献排名在前2*w%的股票
            value = len(day_df)
            rank = min(value, np.round(value * self.w))
            day_df = day_df[(day_df['昨日资金波动_rank'] <= rank)].copy()
            day_df = day_df[(day_df['昨日资金贡献_rank'] <= 2*rank)].copy()

            # 计算日收益率
            if len(day_df) > 0:
                # 计算每支股票的当日涨跌幅，以市值加权平均作为组合的涨跌幅
                day_df['daily_return'] = day_df[f'{self.n}日后当日涨跌（未来函数）'] - 1
                day_df['weight'] = day_df['流通市值'] / day_df['流通市值'].sum()
                daily_return = (day_df['daily_return']
                                * day_df['weight']).sum()

                # 计算资金余额
                if len(result_list) == 0:
                    cash_balance = 1
                else:
                    cash_balance = result_list[-1]['盘中波动']
                cash_balance *= (1 + daily_return - self.m / self.n)

                # 保存结果
                result_list.append({'日期': date, f'盘中波动': cash_balance})

        return pd.DataFrame(result_list)

    def run(self, name):
        """
        运行策略
        """
        df = self.get_data(name)
        df = self.preprocess_data(df)
        result_df = self.strategy(df)

        # 输出交易细节和结果
        trade_detail_file = os.path.join(
            self.data_dir, f'{name}交易细节.csv')
        result_file = os.path.join(self.data_dir, f'{name}盘中波动.csv')
        df.to_csv(trade_detail_file, index=False)
        result_df.to_csv(result_file, index=False)
