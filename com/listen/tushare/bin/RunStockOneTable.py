# coding: utf-8

import multiprocessing
import time
from com.listen.tushare.database.DbService import DbService
from com.listen.tushare.service.StockService import StockService

if __name__ == '__main__':
    real_time = True
    securty_codes = ['601988', '601288', '601818', '601398', '601328', '601939', '601169']
    while True:
        if securty_codes is not None:
            print('security_codes', len(securty_codes))
            for securty_code in securty_codes:
                service = StockService(securty_code, DbService(), False, real_time)
                service.process()
                time.sleep(20)
            print('done')
            real_time = True
        time.sleep(10)