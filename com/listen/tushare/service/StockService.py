# coding: utf-8
import datetime

import tushare as ts
from com.listen.tushare.util.Utils import Utils
import pprint
import sys
import traceback

class StockService():
    table_tushare_stock_hist_data = 'tushare_stock_hist_data'
    table_field_names = ['security_code', 'the_date', 'open', 'low', 'high', 'close', 'volume', 'amount',
                         'close_change', 'close_change_percent', 'high_change', 'high_change_percent',
                         'low_change', 'low_change_percent', 'amount_change', 'amount_change_percent',
                         'volume_change', 'volume_change_percent',
                         'price_average_1', 'price_average_change_1', 'price_average_change_percent_1',
                         'price_average_3', 'price_average_change_3', 'price_average_change_percent_3',
                         'price_average_5', 'price_average_change_5', 'price_average_change_percent_5',
                         'price_average_10', 'price_average_change_10', 'price_average_change_percent_10',
                         'week_day', 'close_change_diff', 'high_change_diff', 'low_change_diff',
                         'amount_change_diff', 'volume_change_diff', 'price_average_change_diff_1',
                         'price_average_change_diff_3', 'price_average_change_diff_5', 'price_average_change_diff_10']

    def __init__(self, security_code, db_service, is_reset=False, real_time=False):
        self.security_code = security_code
        self.is_reset = is_reset
        self.db_service = db_service
        self.pp = pprint.PrettyPrinter(indent=4)
        self.real_time = real_time

    def process(self):
        if self.real_time:
            the_date = datetime.date(year=2017, month=8, day=11)
            self.process_real_time_data(the_date)
            # 下面是真实代码，上面是模拟的
            # the_date = datetime.date.today()
            # ticks_data = ts.get_today_ticks(self.security_code)
            # print('drop before', ticks_data)
            # describe_data = ticks_data.describe()
            # sum_data = ticks_data.sum()
            # indexes_values = ticks_data.index.values
            # open_idx = indexes_values[len(indexes_values) - 1]
            # open = None
            # time_093000 = "09:30:00"
            # time_093000_split = time_093000.split(":")
            # for i in range(len(indexes_values)):
            #     idx = indexes_values[len(indexes_values) - (i+1)]
            #     time = ticks_data.at[idx, 'time']
            #     time_split = time.split[":"]
            #     if int(time_split[0]) <= int(time_093000_split[0]) \
            #             and int(time_split[1]) <= int(time_093000_split[1]) \
            #             and int(time_split[2]) < int(time_093000_split[2]):
            #         ticks_data.drop(idx)
            #     else:
            #         open_idx = idx
            #         open = Utils.base_round_zero(ticks_data.at[open_idx, 'price'], 2)
            #         break
            # print('drop after', ticks_data)
            #
            # if open is not None:
            #     close_idx = indexes_values[0]
            #     close = Utils.base_round_zero(ticks_data.at[close_idx, 'price'], 2)
            #     amount = sum_data['amount']
            #     volume = sum_data['volume']
            #     high = Utils.base_round_zero(describe_data.at['max', 'price'], 2)
            #     low = Utils.base_round_zero(describe_data.at['min', 'price'], 2)
            #     dict_data = {'security_code': Utils.quotes_surround(self.security_code)}
            #     dict_data['the_date'] = Utils.quotes_surround(Utils.format_to_date(the_date))
            #     dict_data['open'] = open
            #     dict_data['high'] = high
            #     dict_data['low'] = low
            #     dict_data['close'] = close
            #     dict_data['amount'] = amount
            #     dict_data['volume'] = volume
            #     self.db_service.upsert(dict_data, self.table_tushare_stock_hist_data, ['security_code', 'the_date'])
            #     self.process_real_time_data(the_date)
            # else:
            #     print("没有发现 09:30:00 对应的开盘价，所以不计算")

        else:
            self.process_h_data()

    def get_start_the_date(self, table_name, limit_size):
        sql = ""
        try:
            sql = "select the_date from " \
                  + table_name \
                  + " where security_code = {security_code} order by the_date desc limit {limit_size}"
            sql = sql.format(security_code=Utils.quotes_surround(self.security_code), limit_size=limit_size)
            print(sql)
            tuple_datas = self.db_service.query(sql)
            if tuple_datas is not None and len(tuple_datas) > 0:
                min_tuple_data = tuple_datas[len(tuple_datas) - 1]
                max_tuple_data = tuple_datas[0]
                min_the_date = None
                max_the_date = None
                if min_tuple_data is not None and len(min_tuple_data) > 0:
                    min_the_date = min_tuple_data[0]
                if max_tuple_data is not None and len(max_tuple_data) > 0:
                    max_the_date = max_tuple_data[0]
                return (min_the_date, max_the_date)
            else:
                return (None, None)
        except Exception:
            traceback.print_exc()
            print('sql error:', sql)
            return None

    def process_h_data(self):
        try:

            (min_the_date, max_the_date) = self.get_start_the_date(self.table_tushare_stock_hist_data, 10)
            start_date = '2015-01-01'
            if min_the_date is not None:
                start_date = Utils.format_to_date(min_the_date)
            print('start_date', start_date)
            h_data = ts.get_h_data(self.security_code, start=start_date, autype=None)
            size = len(h_data.index)
            self.pp.pprint(type(h_data))
            h_data = h_data.sort_index()
            self.pp.pprint(h_data)
            pre_dict_data = None

            for i in range(size):
                if max_the_date is not None and i < 9:
                    print("###属于增量更新，最大时间", max_the_date, "不为空，i =", i)
                    continue
                idx = h_data.index.values[i]
                the_date = idx.astype('M8[ms]').astype('O')
                # TODO 查询数据库是否有前一天的数据，如果有则赋值给pre_dict_data，如果没有则默认
                if pre_dict_data is None:
                    where_sql = "where security_code = {security_code} and the_date < {the_date} order by the_date desc limit 1"
                    where_sql = where_sql.format(security_code=Utils.quotes_surround(self.security_code),
                                                 the_date=Utils.quotes_surround(Utils.format_to_date(the_date)))
                    pre_dict_datas = self.db_service.query_table(self.table_tushare_stock_hist_data, self.table_field_names, where_sql)
                    if pre_dict_datas is not None and len(pre_dict_datas) > 0:
                        pre_dict_data = pre_dict_datas[0]
                    else:
                        pre_dict_data = self.init_derive_zero()
                open = Utils.base_round_zero(h_data.at[idx, 'open'], 2)
                high = Utils.base_round_zero(h_data.at[idx, 'high'], 2)
                close = Utils.base_round_zero(h_data.at[idx, 'close'], 2)
                low = Utils.base_round_zero(h_data.at[idx, 'low'], 2)
                volume = Utils.base_round_zero(h_data.at[idx, 'volume'], 2)
                amount = Utils.base_round_zero(h_data.at[idx, 'amount'], 2)
                # print(the_date, type(the_date), open, type(open), high, type(high), close, type(close), low, type(low),
                #       volume, type(volume), amount, type(amount))
                dict_data = self.init_derive_zero()
                dict_data['security_code'] = self.security_code
                dict_data['the_date'] = Utils.quotes_surround(Utils.format_to_date(the_date))
                dict_data['open'] = open
                dict_data['high'] = high
                dict_data['close'] = close
                dict_data['low'] = low
                dict_data['volume'] = volume
                dict_data['amount'] = amount
                self.process_day_change(dict_data, pre_dict_data)
                self.process_section_data(i, h_data, dict_data, pre_dict_data)
                dict_data['week_day'] = Utils.format_week_day(the_date)
                print("##################", dict_data['security_code'], dict_data['the_date'])
                self.pp.pprint(dict_data)
                pre_dict_data = dict_data
                self.db_service.upsert(dict_data, self.table_tushare_stock_hist_data, ['security_code', 'the_date'])
        except Exception:
            traceback.print_exc()

    def process_day_change(self, dict_data, pre_dict_data):
        """
        :param dict_data:
        :param pre_dict_data:
        :return:
        """
        try:
            for name in ['close', 'high', 'low', 'amount', 'volume']:
                pre_name_val = pre_dict_data[name]
                if name == 'high' or name == 'low':
                    pre_name_val = pre_dict_data['close']
                name_val = dict_data[name]
                name_change = Utils.base_round_zero(name_val - pre_name_val, 2)
                name_change_percent = Utils.base_round_zero(Utils.division_zero(name_change, pre_name_val) * 100, 2)
                pre_name_change = pre_dict_data[name+'_change']
                name_change_diff = Utils.base_round_zero(name_change - pre_name_change, 2)
                dict_data[name+'_change'] = name_change
                dict_data[name+'_change_percent'] = name_change_percent
                dict_data[name+'_change_diff'] = name_change_diff
        except Exception:
            traceback.print_exc()

    def process_section_data(self, i, h_data, dict_data, pre_dict_data):
        try:

            pice_h_data = h_data.iloc[:i+1]
            for ma in [1, 3, 5, 10]:
                section = pice_h_data.tail(ma)
                self.pp.pprint(section)
                section_sum = section.sum()
                volume_sum = section_sum['volume']
                amount_sum = section_sum['amount']
                # print('section_sum', section_sum)
                # print('volume_sum', volume_sum, "amount_sum", amount_sum)
                pre_price_average_ma = pre_dict_data['price_average_' + str(ma)]
                price_average_ma = Utils.base_round_zero(Utils.division_zero(amount_sum, volume_sum), 2)
                price_average_change_ma = Utils.base_round_zero(price_average_ma - pre_price_average_ma, 2)
                price_average_change_percent_ma = Utils.base_round_zero(
                    Utils.division_zero(price_average_change_ma, pre_price_average_ma) * 100, 2)
                pre_price_average_change_ma = pre_dict_data['price_average_change_'+str(ma)]
                price_average_change_diff_ma = Utils.base_round_zero(price_average_change_ma - pre_price_average_change_ma, 2)
                dict_data['price_average_' + str(ma)] = price_average_ma
                dict_data['price_average_change_' + str(ma)] = price_average_change_ma
                dict_data['price_average_change_percent_' + str(ma)] = price_average_change_percent_ma
                dict_data['price_average_change_diff_'+str(ma)] = price_average_change_diff_ma
                # print('volume_sum', volume_sum, 'amount_sum', amount_sum,
                #       'pre_price_average_'+str(ma), pre_price_average_ma,
                #       'price_average_'+str(ma), price_average_ma,
                #       'price_average_change_'+str(ma), price_average_change_ma,
                #       'price_average_change_percent_' + str(ma), price_average_change_percent_ma)
        except Exception:
            traceback.print_exc()

    def calculate_real_time_ma(self, dict_list, dict_data, pre_dict_data):
        for ma in [1, 3, 5, 10]:
            pice_ma_list = dict_list[0:ma]
            volume_sum = Utils.sum([for_data['volume'] for for_data in pice_ma_list])
            amount_sum = Utils.sum([for_data['amount'] for for_data in pice_ma_list])

            pre_price_average_ma = pre_dict_data['price_average_' + str(ma)]
            price_average_ma = Utils.base_round_zero(Utils.division_zero(amount_sum, volume_sum), 2)
            price_average_change_ma = Utils.base_round_zero(price_average_ma - pre_price_average_ma, 2)
            price_average_change_percent_ma = Utils.base_round_zero(
                Utils.division_zero(price_average_change_ma, pre_price_average_ma) * 100, 2)
            pre_price_average_change_ma = pre_dict_data['price_average_change_' + str(ma)]
            price_average_change_diff_ma = Utils.base_round_zero(price_average_change_ma - pre_price_average_change_ma,
                                                                 2)
            dict_data['price_average_' + str(ma)] = price_average_ma
            dict_data['price_average_change_' + str(ma)] = price_average_change_ma
            dict_data['price_average_change_percent_' + str(ma)] = price_average_change_percent_ma
            dict_data['price_average_change_diff_' + str(ma)] = price_average_change_diff_ma
        dict_data['the_date'] = Utils.quotes_surround(Utils.format_to_date(dict_data['the_date']))
        dict_data['security_code'] = Utils.quotes_surround(self.security_code)
        self.db_service.upsert(dict_data, self.table_tushare_stock_hist_data, ['security_code', 'the_date'])

    def process_real_time_data(self, the_date):
        dict_list = self.get_hist_data(Utils.format_to_date(the_date), "<=", "desc", 10)
        if dict_list is not None and len(dict_list) == 10:
            dict_data = dict_list[0]
            pre_dict_data = dict_list[1]
            self.process_day_change(dict_data, pre_dict_data)
            self.calculate_real_time_ma(dict_list, dict_data, pre_dict_data)
        else:
            print("$$$$$$$$$$实时行情计算失败")


    def get_hist_data(self, the_date, relation, direction, limit_size):
        try:
            where_sql = "where security_code = {security_code} and the_date " \
                        + relation \
                        + " {the_date} order by the_date " + direction + " limit " + str(limit_size)
            where_sql = where_sql.format(security_code=Utils.quotes_surround(self.security_code),
                                        the_date=Utils.quotes_surround(the_date))
            dict_list = self.db_service.query_table(self.table_tushare_stock_hist_data, self.table_field_names, where_sql)
            return dict_list
        except Exception:
            traceback.print_exc()
            return None

    @staticmethod
    def init_derive_zero():
        dict_data = {}
        dict_data['open'] = 0
        dict_data['high'] = 0
        dict_data['close'] = 0
        dict_data['low'] = 0
        dict_data['volume'] = 0
        dict_data['amount'] = 0
        dict_data['close_change'] = 0
        dict_data['close_change_percent'] = 0
        dict_data['high_change'] = 0
        dict_data['high_change_percent'] = 0
        dict_data['low_change'] = 0
        dict_data['low_change_percent'] = 0
        dict_data['amount_change'] = 0
        dict_data['amount_change_percent'] = 0
        dict_data['volume_change'] = 0
        dict_data['volume_change_percent'] = 0
        dict_data['price_average_1'] = 0
        dict_data['price_average_change_1'] = 0
        dict_data['price_average_change_percent_1'] = 0
        dict_data['price_average_3'] = 0
        dict_data['price_average_change_3'] = 0
        dict_data['price_average_change_percent_3'] = 0
        dict_data['price_average_5'] = 0
        dict_data['price_average_change_5'] = 0
        dict_data['price_average_change_percent_5'] = 0
        dict_data['price_average_10'] = 0
        dict_data['price_average_change_10'] = 0
        dict_data['price_average_change_percent_10'] = 0
        dict_data['close_change_diff'] = 0
        dict_data['high_change_diff'] = 0
        dict_data['low_change_diff'] = 0
        dict_data['amount_change_diff'] = 0
        dict_data['volume_change_diff'] = 0
        dict_data['price_average_change_diff_1'] = 0
        dict_data['price_average_change_diff_3'] = 0
        dict_data['price_average_change_percent_5'] = 0
        dict_data['price_average_change_diff_10'] = 0
        return dict_data