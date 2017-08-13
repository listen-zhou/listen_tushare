# conding: utf-8

import decimal
from decimal import Decimal
from decimal import getcontext
import copy

import datetime


class Utils():

    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERRO"

    @staticmethod
    def format_log_message(list):
        if list is not None and len(list) > 0:
            length = len(list)
            i = 0
            message = ''
            while i < length:
                message += '{0[' + str(i) + ']} '
                i += 1
            message = message.format(list)
            return message
        else:
            return None

    @staticmethod
    def print_log(list):
        print(Utils.get_now_datetime(), Utils.format_log_message(list))

    @staticmethod
    def quotes_surround(str):
        if str is not None:
            return "'" + str + "'"
        return str

    @staticmethod
    def base_round(val, n):
        if val is not None:
            val = Decimal(val, getcontext())
            return val.__round__(n)
        return None
    @staticmethod
    def base_round_zero(val, n):
        if val is not None:
            val = Decimal(val, getcontext())
        else:
            val = Decimal(0, getcontext())
        return val.__round__(n)

    @staticmethod
    def division(divisor, dividend):
        if divisor is not None and dividend is not None and dividend != 0 and dividend != Decimal(0):
            return divisor / dividend
        return None

    @staticmethod
    def division_zero(divisor, dividend):
        if divisor is not None and dividend is not None and dividend != 0 and dividend != Decimal(0):
            return divisor / dividend
        return Decimal(0)

    @staticmethod
    def sum(list):
        if list is not None and len(list) > 0:
            total = Decimal(0)
            for item in list:
                if item is not None:
                    total += item
            return total
        return None

    @staticmethod
    def sum_zero(list):
        if list is not None and len(list) > 0:
            total = Decimal(0)
            for item in list:
                if item is not None:
                    total += item
            return total
        return Decimal(0)

    @staticmethod
    def average(list):
        if list is not None and len(list) > 0:
            total = Utils.sum(list)
            if total is not None:
                average = total / Decimal(len(list))
                return average
        return None

    @staticmethod
    def average_zero(list):
        if list is not None and len(list) > 0:
            total = Utils.sum(list)
            if total is not None:
                average = total / Decimal(len(list))
                return average
        return Decimal(0)

    @staticmethod
    def get_now_datetime():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def diff_days(date):
        if isinstance(date, datetime.date):
            date = datetime.datetime.now().replace(year=date.year, month=date.month, day=date.day)
            today = datetime.datetime.now()
            days = (today - date).days
            if days is None :
                return 0
            return days
        return None

    @staticmethod
    def format_to_date(date):
        if date is not None and (isinstance(date, datetime.datetime) or (isinstance(date, datetime.date))):
            return date.strftime('%Y-%m-%d')
        else:
            return date

    @staticmethod
    def format_to_datetime(date):
        if date is not None and (isinstance(date, datetime.datetime) or (isinstance(date, datetime.date))):
            return date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return date

    @staticmethod
    def format_week_day(val):
        if isinstance(val, datetime.date) or isinstance(val, datetime.datetime):
            return val.weekday() + 1
        return val


