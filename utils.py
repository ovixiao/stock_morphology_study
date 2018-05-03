# -*- encoding: utf-8 -*-
import tushare as ts


def get_data(beg_date, end_date):
    """
    获取起始日期到结束日期的股票数据

    :param beg_date: 起始日期, YYYY-MM-DD
    :param end_date: 结束日期, YYYY-MM-DD
    :return: 返回股票数据. code, pd
    """
    pd = ts.get_stock_basics()
    stocks = {}
    for code, values in pd.iterrows():
        name = values['name']
        stocks[code] = name

    for code, name in stocks.items():
        pd = ts.get_k_data(code, start=beg_date, end=end_date)
        yield code, pd


def get_trend(pd, length=3):
    """依据给定的 pd 数据, 计算是不是在涨跌趋势中

    :param pd: tushare 返回的 pandas 数据
    :param length: 计算涨跌的窗口大小, 默认为 3, 即今天, 昨天, 前天
    :return: 返回标定的涨跌情况 dict = {date: 涨跌情况}
             涨跌情况: 1 -> 上涨, -1 -> 下跌, 0 -> 震荡
    """

    def is_ascending(window):
        # 判断是不是增长趋势
        last_high, last_low = window[0][1], window[0][2]
        for _, high, low in window[1:]:
            if not(high > last_high and low > last_low):
                return False
            last_high = high
            last_low = low

        return True

    def is_descending(window):
        # 判断是不是下跌趋势
        last_high, last_low = window[0][1], window[0][2]
        for _, high, low in window[1:]:
            if not(high < last_high and low < last_low):
                return False
            last_high = high
            last_low = low

        return True

    price_list = []
    for _, values in pd.iterrows():
        date = values['date']
        high = values['high']
        low = values['low']
        price_list.append((date, high, low))
    price_list.sort(key=lambda x: x[0])

    trend_dict = {}
    for i in xrange(length + 1, len(price_list) + 1):
        window = price_list[i - length: i]
        last_date = window[-1][0]
        if is_ascending(window):
            trend_dict[last_date] = 1
        elif is_descending(window):
            trend_dict[last_date] = -1
        else:
            trend_dict[last_date] = 0

    return trend_dict


if __name__ == '__main__':
    iterator = get_data('2018-04-12', '2018-05-02')
    for code, pd in iterator:
        print pd
        trend_dict = get_trend(pd)
        print sorted(trend_dict.items(), key=lambda x: x[0])
        break
