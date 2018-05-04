# -*- encoding: utf-8 -*-
import tushare as ts
from data import StockData as sd


def _get_trend(pd, length=3):
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


def get_inflection_point(kdata):
    """
    从 kdata 中计算拐点. 拐点的定义为:

    顶点: 当前点的最高价比其前后两天的最高价高
    底点: 当前点的最低价比起前后两天的最低价低

    :param kdata: K 线数据, 已经按照时间排序, 最少需要3个
    :return: 返回拐点的日期列表, 格式为:
        [(date, type), ...]
    其中, type == 1 表示顶点, type == 0 表示底点
    """
    # (data, type)
    point_list = [(kdata[0], None)]
    interval = 3
    for i in xrange(1, len(kdata) - 1):
        prev = kdata[i - 1]
        curr = kdata[i]
        next = kdata[i + 1]

        # 是否是顶点
        if curr['high'] > prev['high'] and curr['high'] > next['high']:
            # 1 -> 顶点, 不同, 需要判断是否超过 1
            if point_list[-1][1] != 1 and interval >= 1:
                point_list.append((curr, 1))
                interval = 0
            elif point_list[-1][1] == 1:  # 相同
                last_point, type = point_list.pop()
                if curr['high'] >= last_point['high']:  # 替换为现在的
                    point_list.append((curr, 1))
                    interval = 0
                else:  # 还是存老的
                    point_list.append((last_point, type))
                    interval += 1
            else:
                interval += 1
        elif curr['low'] < prev['low'] and curr['low'] < next['low']:
            # 0 -> 底点, 不同, 需要判断是否超过 1
            if point_list[-1][1] != 0 and interval > 1:
                point_list.append((curr, 0))
                interval = 0
            elif point_list[-1][1] == 0:  # 相同
                last_point, type = point_list.pop()
                if curr['low'] <= last_point['low']:  # 替换为现在的
                    point_list.append((curr, 0))
                    interval = 0
                else:  # 还是存老的
                    point_list.append((last_point, type))
                    interval += 1
            else:
                interval += 1
        else:
            interval += 1

    return map(lambda x: (x[0]['date'], x[1]), point_list)


def is_hammer(data):
    """
    1. 上影很短, 不到实体的 一半
    2. 实体较短, 不到总体的 15%
    3. 下影很长, 达到实体的 2 倍, 达到今天开盘价的5%
    """
    top_shadow = data['high'] - max(data['open'], data['close'])
    box_size = abs(data['open'] - data['close'])
    bottom_shadow = min(data['open'], data['close']) - data['low']
    total = data['high'] - data['low']
    if top_shadow >= 0.5 * box_size:
        return False
    if bottom_shadow < 2 * box_size:
        return False
    if box_size > 0.15 * total:
        return False
    if bottom_shadow <= data['open'] * 0.04:
        return False

    return True


def get_hammer(kdata):

    def win():
        if (max([x['high'] for x in kdata[i + 2: i + 7]]) - kdata[i + 1]['close']) / kdata[i + 1]['close'] > 0.03:
            return 1
        else:
            return 0

    n = 5
    for i, data in enumerate(kdata):
        if is_hammer(data):
            '''
            # 当前价格的最高点是过往 n 天最高点的最大值
            high_list = map(lambda x: x['high'], kdata[i - n + 1: i + 1])
            low_list = map(lambda x: x['low'], kdata[i - n + 1: i + 1])
            if len(high_list) == n and max(high_list) == data['high']:
                yield data['date']
            elif len(low_list) == n and min(low_list) == data['low']:
                yield data['date']
            '''
            try:
                # 顶
                #if data['high'] > kdata[i - 1]['high'] < kdata[i - 2]['low'] \
                #        and data['close'] > max(kdata[i - 1]['close'], kdata[i - 1]['open']) \
                #        and kdata[i + 1]['close'] < kdata[i + 1]['open']:
                #    yield data['date'], win()
                # 底
                if data['low'] < kdata[i - 1]['low'] < kdata[i - 2]['low'] \
                        and data['close'] > min(kdata[i - 1]['close'], kdata[i - 1]['open']) \
                        and kdata[i + 1]['close'] > kdata[i + 1]['open'] \
                        and kdata[i + 1]['close'] > max(data['open'], data['close']):
                    yield data['date'], win()
            except:
                continue




if __name__ == '__main__':
    kdata_iter = sd.get_all_kdata('2000-01-01', '2018-05-04')
    win_count, total_count = 0, 0
    for code, kdata in kdata_iter:
        hammer = list(get_hammer(kdata))
        win_count += sum([x[1] for x in hammer])
        total_count += len(hammer)
        for date, w in hammer:
            print code, w, date
        '''
        if hammer:
            point_list = get_inflection_point(kdata)
            point_date_set = set([x[0] for x in point_list])
            print code
            print hammer
            print point_list
            print code, len(hammer & point_date_set), len(hammer), len(point_date_set), len(hammer & point_date_set) / float(len(hammer))
            print "========" * 20
        '''
    print win_count, total_count, win_count / float(total_count)
