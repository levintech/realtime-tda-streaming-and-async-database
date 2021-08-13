# import packages
from enum import unique
from os import sep
import sys
import threading
import pytz

import json
import asyncio
import pandas as pd
from datetime import datetime

from tda import utils
from tda.auth import easy_client
from tda.client import Client
from tda.streaming import StreamClient

from utils import *
from data import *
from database import *

# import global contents
import global_content as gl_content 
gl_content.initialize()


class TimeClock(threading.Thread):

    analysis_system = None
    keep_running = True

    def __init__(self, system):
        threading.Thread.__init__(self)

        self.analysis_system = system
        self.current_time = datetime.now(pytz.timezone('US/Eastern'))
        self.analysis_system.current_time = self.current_time
        self.keep_running = True

    def run(self):
        while self.keep_running:
            # get the current time as EST timezone
            new_time = datetime.now(pytz.timezone('US/Eastern'))

            if self.current_time.minute != new_time.minute:
                self.current_time = new_time
                self.analysis_system.new_minute = True
                self.analysis_system.start_new_minute()

class AnalysisSystem(threading.Thread):

    time_clock = None
    current_time = None
    trade_time = None
    market_type = None
    pd_symbols = None
    new_minute = True
    minute_data = None
    db_helper = None
    result_file = None
    kill_async_task = False
    second_count = None
    runner_check_list = None
    runner_data = None
    log_file = None

    def __init__(self):
        threading.Thread.__init__(self)

        # create the time clock thread
        # self.time_clock = TimeClock(self)
        self.second_count = 0
        self.runner_data = []
        self.runner_check_list = []
        self.minute_data = OneMinuteData()
        self.db_helper = DatabaseHelper(self)
        self.current_time = datetime.now(pytz.timezone('US/Eastern'))
        self.log_file = "log.txt"

        client = easy_client(
            api_key=gl_content.TDA_CONSUMER_KEY,
            redirect_uri='https://localhost',
            token_path='token')
        self.stream_client = StreamClient(client, account_id=gl_content.TDA_ACCOUNT)

    def check_runner(self, runner_data, market_type, minute_mode):
        
        stock_key = 'symbol' if minute_mode else 'key'
        volume_key = 'volume' if minute_mode else 'LAST_SIZE'
        stock_list = []
        [stock_list.append(item[stock_key]) for item in runner_data if item[stock_key] not in stock_list]

        for stock in stock_list:
            runner_value = sum([item[volume_key] for item in runner_data if item[stock_key] == stock])
            if runner_value < 1000:
                continue
            if is_runner(stock, runner_value, market_type, self.db_helper, minute_mode):
                if minute_mode:
                    # save to runner table
                    price = [item['price'] for item in runner_data if item[stock_key] == stock][-1]
                    self.db_helper.save_runner_info(stock, self.current_time, runner_value, price, self.market_type)
                else:
                    sys.stdout.write('\a')
                    sys.stdout.flush()
                    # beepy.beep(sound="ping")

    def check_gapper(self, gapper_data, market_type):
        
        for stock in gapper_data.get_result():
            
            # check given stock is gapper or not
            gapper, (symbol, last_price, price_change, price_percent) \
                = is_gapper(stock, market_type, self.db_helper)

            if gapper:
                # save gapper info to database
                self.db_helper.save_gapper_info(
                    symbol, last_price, stock['open'], 
                    price_change, price_percent
                )

    def start_new_minute(self):
        # save the previous minute data to database
        if self.minute_data.length > 0:
            if self.result_file is not None:
                self.result_file.write('==================================================\n')
                self.result_file.write('total volume : {}\n'.format(
                    sum([item['volume'] for item in self.minute_data.sequence])
                ))
                self.result_file.close()
                self.result_file = None

            # check runners
            self.check_runner(self.minute_data.sequence, self.market_type, True)            

            # # check gapper
            # self.check_gapper(self.minute_data, self.market_type)

            print("{0:02d} : {1:02d} is ended".format(self.current_time.hour, self.current_time.minute))
            tic = time.perf_counter()
            # save the minute data to database
            thread = threading.Thread(target=self.db_helper.save_one_minute_data,
                        args=(self.current_time, self.market_type, self.minute_data,))
            thread.start()
            toc = time.perf_counter()
            print(f"Saving the minute data in {toc - tic:0.4f} seconds")

        # make new object to contain currrent min data
        self.minute_data = OneMinuteData()

        # update current time
        # self.current_time = self.time_clock.current_time
        self.current_time = self.trade_time

        # check the stock market is opened or closed now
        err, self.market_type = get_current_market(self.current_time)
        if err is None:
            print("Market Type : ", gl_content.market_label[self.market_type])

            self.minute_data = OneMinuteData()
            print("{0:02d} : {1:02d} is started".format(self.current_time.hour, self.current_time.minute))
            time_text = "{0:02d}_{1:02d}".format(self.current_time.hour, self.current_time.minute)
            self.result_file = open('min_log/{}.txt'.format(time_text), 'w')
            
        else:
            print(err)
            self.kill_async_task = True
            # thread = threading.Thread(target=self.end_of_day,)
            # thread.start()

    def parse_stream(self, msg):
        # tic = time.perf_counter()
        # self.minute_data.append_stream(msg)
        # toc = time.perf_counter()
        # print(f"Appending new stream data in {toc - tic:0.4f} seconds")

        try:
            assert self.kill_async_task == False
        except AssertionError as e:
            self.end_of_day()

        for item in msg["content"]:
            # get the trade time on EST
            self.trade_time = datetime.fromtimestamp(
                item['TRADE_TIME'] / 1000,
                tz=pytz.timezone('US/Eastern'))

            # check the new minute
            current_time = self.current_time.strftime("%Y-%m-%d %H:%M:%S")
            trade_time = self.trade_time.strftime("%Y-%m-%d %H:%M:%S")
            if trade_time < current_time:
                continue

            self.second_count = self.trade_time.second
            if self.second_count % gl_content.runners_checkout == gl_content.runners_checkout - 1 \
                    and self.second_count != 0 \
                    and self.second_count not in self.runner_check_list:

                print("Runner check [{} ~ {}] -> length of ticker : {}".format(
                    self.second_count - gl_content.runners_checkout + 1, 
                    self.second_count, len(self.runner_data)
                ))

                # tic = time.perf_counter()
                # runner_thread = threading.Thread(target=self.check_runner,
                #             args=(self.runner_data, self.market_type, False,))
                # runner_thread.start()
                # toc = time.perf_counter()
                # print(f"Checking the runners in {toc - tic:0.4f} seconds")
                self.check_runner(self.runner_data, self.market_type, False)

                self.runner_data = []
                self.runner_check_list.append(self.second_count)
            else:
                self.runner_data.append(item)

            # detect new minute
            if self.current_time.minute != self.trade_time.minute:
                self.start_new_minute()
                self.second_count = 0
                self.runner_check_list = []

            # add ticker info to minute data
            self.minute_data.append_ticker(item)

            if self.result_file is not None:
                # output stream data to file
                self.result_file.write('--------------------------------------------------\n')

                # write the log on txt file
                self.result_file.write(
                    "symbol : {}, volume : {}, last_seq : {}, seq : {}, trade_time : {}\n"
                    .format(item['key'], item['LAST_SIZE'], item['LAST_SEQUENCE'], 
                            item['seq'], self.trade_time))


    async def read_stream(self):
        """Read the websocket streaming as Asynchronous

        Args:
            stream_client (StreamClient): reference object of streaming client
        """

        await self.stream_client.login()
        await self.stream_client.quality_of_service(StreamClient.QOSLevel.FAST)

        # Always add handlers before subscribing because many streams start sending
        # data immediately after success, and messages with no handlers are dropped.
        
        # self.stream_client.add_timesale_equity_handler(
        #         lambda msg: print(json.dumps(msg, indent=4)))
        self.stream_client.add_timesale_equity_handler(
                lambda msg: self.parse_stream(msg))

        # await self.stream_client.timesale_equity_subs(['DTRC'])
        await self.stream_client.timesale_equity_subs(self.pd_symbols['Symbol'])
        
        while True:
            await self.stream_client.handle_message()
                
    def end_of_day(self):
        print("End of day")
        with open(self.log_file, "a") as logger:
            logger.write("\n")
            logger.write("End of day")

        # # stop the time clock
        # self.time_clock.keep_running = False

        # generate the 'ticker_volume_by_day'
        self.db_helper.generate_ticker_volume_by_day(self.current_time)
        print("Update ticker_volume_by_day successfully")

        # generate the 'ticker_average_by_minute'
        self.db_helper.generate_ticker_average_by_minute()
        print("Update ticker_average_by_minute successfully")

        # exit the application
        sys.exit()

    def run(self):

        # ---- Prepare receive streaming ----- #
        # load the symbols
        # self.pd_symbols = pd.read_csv('symbols.csv', sep=':')
        self.pd_symbols = pd.read_csv('stocks_old.csv')

        # check the stock market is opend or closed now
        # waiting for market to open
        while True:
            err, self.market_type = get_current_market()

            if err is None:
                break
            else:
                print(err)
                time.sleep(1)

        print("Market Type : ", gl_content.market_label[self.market_type])

        # start the receving streaming
        asyncio.run(self.read_stream())

if __name__ == '__main__':

    main_system = AnalysisSystem()
    main_system.start()
