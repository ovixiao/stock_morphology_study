# -*- encoding: utf-8 -*-
import sys
import pymongo
from pymongo.errors import DuplicateKeyError
import tushare as ts


BEG_DATE = '2000-01-01'
END_DATE = '2018-05-03'
DB_HOSTNAME = '127.0.0.1'
DB_PORT = 27017


class StockData(object):

    cli = pymongo.MongoClient(DB_HOSTNAME, DB_PORT)
    db = cli.sms  # stock_morphology_study
    # stock: 存储所有的股票名称/代号等
    db.stock.create_index('code', unique=True)
    # kdata: K 线数据, 记录从 beg_date 到 end_date 的 K 线数据
    db.kdata.create_index([('code', 1), ('date', 1)], unique=True)

    @classmethod
    def save_data_to_db(cls, beg_date=BEG_DATE, end_date=END_DATE):
        """
        将 tushare 的数据保存 mongodb, 方便查看

        :param beg_date: 起始日期, 默认 2000-01-01
        :param end_date: 结束日期, 默认 2018-05-03
        """

        # 获取并存储股票名称/代号等信息
        pd = ts.get_stock_basics()
        for code, values in pd.iterrows():
            try:
                values = values.to_dict()
                values['code'] = code
                cls.db.stock.insert_one(values)
            except DuplicateKeyError:
                pass

            # 获取并存储 K 线数据
            try:
                name = values['name']
                pd = ts.get_k_data(code, start=beg_date, end=end_date)
                for _, data in pd.iterrows():
                    try:
                        data = data.to_dict()
                        data['code'] = code
                        data['name'] = name
                        cls.db.kdata.insert_one(data)
                    except DuplicateKeyError:
                        continue
            except Exception:
                print >> sys.stderr, code

    @classmethod
    def get_all_kdata(cls, beg_date=BEG_DATE, end_date=END_DATE):
        """
        获取所有的 K 线数据

        :param beg_date: 起始日期, 默认 2000-01-01
        :param end_date: 结束日期, 默认 2018-05-03
        :return: 返回一个生成器, 每个元素为 code, kdata
        """
        stocks = cls.db.stock.find()
        for stock in stocks:
            code = stock['code']
            # 查找 K 线数据
            kdata = cls.db.kdata.find({
                'date': {
                    '$gte': beg_date,
                    '$lt': end_date,
                },
                'code': code,
            })
            kdata = list(kdata)
            kdata.sort(key=lambda x: x['date'])
            yield code, kdata


if __name__ == '__main__':
    iterator = StockData.get_all_kdata('2018-01-01', '2018-01-10')
    for code, kdata in iterator:
        print code
        for item in kdata:
            print item
        break
