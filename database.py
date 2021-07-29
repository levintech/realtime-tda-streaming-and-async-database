# import packages
import mysql.connector
from mysql.connector import errorcode
import asyncio
import json
import pandas as pd

from mysql.connector import cursor

import global_content as gl_content 

class DatabaseHelper():
    
    analysis_system = None
    cnx = None
    cursor = None

    def __init__(self, system) -> None:
        
        # receive the context of analysis system
        self.analysis_system = system

        # define constant
        self.one_min_table = [
            "1min_ticker_premarket", 
            "1min_ticker_market", 
            "1min_ticker_postmarket"
        ]

        self.configuration()
    
    def configuration(self):

        config = {
            'user': gl_content.DB_USER,
            'password': gl_content.DB_PASS,
            'host': gl_content.DB_HOST,
        }

        # create the cursor object
        self.cnx = mysql.connector.connect(**config)
        self.cursor = self.cnx.cursor(buffered=True)

        # create database if doesn't exist
        self.cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(gl_content.DB_NAME))
        self.cursor.execute('USE {}'.format(gl_content.DB_NAME))

        # crate tables if doesn't exist
        # self.cursor.execute('''DROP TABLE IF EXISTS 1min_ticker_premarket''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS 1min_ticker_premarket
          (symbol varchar(255), date_time varchar(50), open float, high float, low float, close float, volume int)''')

        # self.cursor.execute('''DROP TABLE IF EXISTS 1min_ticker_market''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS 1min_ticker_market
          (symbol varchar(255), date_time varchar(50), open float, high float, low float, close float, volume int)''')

        # self.cursor.execute('''DROP TABLE IF EXISTS 1min_ticker_postmarket''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS 1min_ticker_postmarket
          (symbol varchar(255), date_time varchar(50), open float, high float, low float, close float, volume int)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_volume_by_day
          (symbol varchar(255), date varchar(50), open_price float, high_price float, low_price float, close_price float, 
          total_volume_day_premarket int, total_volume_day_market int, total_volume_day_postmarket int, total_volume_day int)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_average_by_minute
          (symbol varchar(255), last_date varchar(50), latest_close float, average_volume_premarket int, 
          average_volume_market int, average_volume_postmarket int)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS runners_day
          (symbol varchar(255), time_of_day varchar(50), avg_min float, last_price float, volume int, price float, 
          change_volume float, change_price float)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS gappers_day
          (symbol varchar(255), last_price float, price_day float, price_change float, 
          change_percent float)''')

    def get_average_volume(self, market_type, symbol):
        """get the average volume of specific market type

        Args:
            market_type (int): market type. [pre-market, market, post-market]
        Return:
            avg_volume (int): average volume of specific market
        """

        field_name = ['average_volume_premarket', 'average_volume_market', 'average_volume_postmarket']
        sql = "SELECT {} from ticker_average_by_minute WHERE symbol='{}';".format(
            field_name[market_type], symbol)
        
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
        except Exception as e:
            result = None

        if result is None:
            return result

        return result[0]

    def get_last_price(self, symbol):
        """Get the last price of specific stock

        Args:
            symbol (string): the name of the stock
        
        Returns:
            the last price of given stock
        """

        sql = "SELECT latest_close from ticker_average_by_minute WHERE symbol='{}'".format(
            symbol)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
        except Exception as e:
            result = None

        if result is None:
            return result
        return result[0]
    
    def get_stock_1min_data(self, stock, market_type):
        
        sql = "SELECT * from {} WHERE symbol='{}'".format(
            self.one_min_table[market_type], stock
        )

        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def keep_daily_tickers(self):

        date_string = '{0:04d}_{1:02d}_{2:02d}'.format(
            self.analysis_system.current_time.year, 
            self.analysis_system.current_time.month, 
            self.analysis_system.current_time.day)
        
        # clone the tables for 1min ticker
        self.cursor.execute(
            "CREATE TABLE {}_premarket SELECT * FROM 1min_ticker_premarket".format(
                date_string))
        self.cursor.execute(
            "CREATE TABLE {}_market SELECT * FROM 1min_ticker_market".format(
                date_string))
        self.cursor.execute(
            "CREATE TABLE {}_postmarket SELECT * FROM 1min_ticker_postmarket".format(
                date_string))
        
        # output data to csv file
        results = pd.read_sql_query("SELECT * FROM 1min_ticker_premarket", self.cnx)
        results.to_csv("daily_report/{}_premarket.csv".format(date_string), index=False)
        results = pd.read_sql_query("SELECT * FROM 1min_ticker_market", self.cnx)
        results.to_csv("daily_report/{}_market.csv".format(date_string), index=False)
        results = pd.read_sql_query("SELECT * FROM 1min_ticker_postmarket", self.cnx)
        results.to_csv("daily_report/{}_postmarket.csv".format(date_string), index=False)

    def clear_tables(self):

        # clear the 1min_ticker tables of pre-market, market, post-market
        self.cursor.execute("DELETE FROM 1min_ticker_premarket")
        self.cursor.execute("DELETE FROM 1min_ticker_market")
        self.cursor.execute("DELETE FROM 1min_ticker_postmarket")

        self.cnx.commit()

    def save_runner_info(self, stock, current_time, volume, price, market_type):
        time_string = '{0:02d}/{1:02d}/{2:02d}:{3:02d}'.format(
            current_time.month, current_time.day, current_time.hour, current_time.minute)

        print('-------------------------------------------------')
        print("stock : {0}, time : {1}".format(stock, time_string))
        print('-------------------------------------------------')

        # get the average volume of stock
        avg_min = self.get_average_volume(market_type, stock)
        if (avg_min == None) and (market_type in [0, 2]):
            avg_min = self.get_average_volume(1, stock)

        # get last price of stock
        sql = '''SELECT latest_close from ticker_average_by_minute WHERE symbol='{}'
        '''.format(stock)
        self.cursor.execute(sql)
        last_price = self.cursor.fetchone()[0]

        # calculate the volume change
        if avg_min == 0:
            change_volume = volume
        else:
            change_volume = float(volume) / avg_min * 100
        # calculate the volume change
        change_price = float(price) / last_price * 100

        # insert to database
        sql = ("INSERT INTO runners_day "
              "(symbol, time_of_day, avg_min, last_price, volume, price, change_volume, change_price) "
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

        self.cursor.execute(sql, (stock, time_string, avg_min, last_price, volume, price, change_volume, change_price))
        self.cnx.commit()

    def save_gapper_info(self, stock, last_price, price_day, price_change, price_percent):

        sql = ("INSERT INTO gappers_day "
              "(symbol, last_price, price_day, price_change, change_percent)"
              "VALUES (%s, %s, %s, %s, %s)")

        try:
            self.cursor.execute(sql, (stock, last_price, price_day, price_change,price_percent))        
            self.cnx.commit()
        except Exception as e:
            print(e)

    def save_one_minute_data(self, current_time, market_type, minute_data):
        
        data_list = minute_data.get_result()
        for index in range(len(data_list)):
            data_list[index]['date_time'] = '{0:02d}:{1:02d}'.format(current_time.hour, current_time.minute)

        # insert to database
        sql = "INSERT INTO `{}` ( `symbol`, `date_time`, `open`, `high`, `low`, `close`, `volume` ) \
                VALUES ( %(symbol)s, %(date_time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s ) \
            ".format(self.one_min_table[market_type])

        try:
            self.cursor.executemany(sql, data_list)
            self.cnx.commit()
        except Exception as e:
            print(e)
            # print("The issue is occurred when saving the minute data to DB")
    
    def generate_ticker_volume_by_day(self, current_time):
        
        volume_by_day = {}
        date_string = '{0:04d}/{1:02d}/{2:02d}'.format(current_time.year, current_time.month, current_time.day)

        # ----- 1. get the pre-market info ----- #
        # get the high, low, total_volume
        sql = "select symbol, MAX(high) high, MIN(low) low, SUM(volume) volume from 1min_ticker_premarket GROUP BY symbol ORDER BY symbol"
        # sql = "select * from 1min_ticker_premarket"
        self.cursor.execute(sql)

        for (symbol, high, low, volume) in self.cursor:
            temp_dict = {}
            temp_dict['symbol'] = symbol
            temp_dict['date'] = date_string
            temp_dict['open_price'] = None
            temp_dict['high_price'] = high
            temp_dict['low_price'] = low
            temp_dict['close_price'] = None
            temp_dict['total_volume_day_premarket'] = volume
            temp_dict['total_volume_day_market'] = 0
            temp_dict['total_volume_day_postmarket'] = 0
            temp_dict['total_volume_day'] = volume

            volume_by_day[symbol] = temp_dict
            
        # get the open price
        sql = '''
            SELECT 1min_ticker_premarket.symbol, 1min_ticker_premarket.open from 1min_ticker_premarket, 
            (SELECT symbol, Min(date_time) as open_time from 1min_ticker_premarket GROUP BY symbol) as open_ticker
            WHERE 1min_ticker_premarket.symbol = open_ticker.symbol
            and 1min_ticker_premarket.date_time = open_ticker.open_time '''
        self.cursor.execute(sql)

        for (symbol, open) in self.cursor:
            volume_by_day[symbol]['open_price'] = open
                
        # get the open price
        sql = '''
            SELECT 1min_ticker_premarket.symbol, 1min_ticker_premarket.close from 1min_ticker_premarket, 
            (SELECT symbol, Max(date_time) as close_time from 1min_ticker_premarket GROUP BY symbol) as close_ticker
            WHERE 1min_ticker_premarket.symbol = close_ticker.symbol
            and 1min_ticker_premarket.date_time = close_ticker.close_time '''
        self.cursor.execute(sql)

        for (symbol, close) in self.cursor:
            volume_by_day[symbol]['close_price'] = close

        # ----- 2. get the market info ----- #
        # get the high, low, total_volume
        sql = "select symbol, MAX(high) high, MIN(low) low, SUM(volume) volume from 1min_ticker_market GROUP BY symbol ORDER BY symbol"
        # sql = "select * from 1min_ticker_premarket"
        self.cursor.execute(sql)

        for (symbol, high, low, volume) in self.cursor:
            if symbol in volume_by_day.keys():
                if high > volume_by_day[symbol]['high_price']:
                    volume_by_day[symbol]['high_price'] = high
                if low < volume_by_day[symbol]['low_price']:
                    volume_by_day[symbol]['low_price'] = low

                volume_by_day[symbol]['total_volume_day_market'] = volume    
                volume_by_day[symbol]['total_volume_day'] += volume
            else:
                temp_dict = {}
                temp_dict['symbol'] = symbol
                temp_dict['date'] = date_string
                temp_dict['open_price'] = None
                temp_dict['high_price'] = high
                temp_dict['low_price'] = low
                temp_dict['close_price'] = None
                temp_dict['total_volume_day_premarket'] = 0
                temp_dict['total_volume_day_market'] = volume
                temp_dict['total_volume_day_postmarket'] = 0
                temp_dict['total_volume_day'] = volume

                volume_by_day[symbol] = temp_dict
            
        # get the open price
        sql = '''
            SELECT 1min_ticker_market.symbol, 1min_ticker_market.open from 1min_ticker_market, 
            (SELECT symbol, Min(date_time) as open_time from 1min_ticker_market GROUP BY symbol) as open_ticker
            WHERE 1min_ticker_market.symbol = open_ticker.symbol
            and 1min_ticker_market.date_time = open_ticker.open_time '''
        self.cursor.execute(sql)

        for (symbol, open) in self.cursor:
            if volume_by_day[symbol]['open_price'] is None:
                volume_by_day[symbol]['open_price'] = open
                
        # get the open price
        sql = '''
            SELECT 1min_ticker_market.symbol, 1min_ticker_market.close from 1min_ticker_market, 
            (SELECT symbol, Max(date_time) as close_time from 1min_ticker_market GROUP BY symbol) as close_ticker
            WHERE 1min_ticker_market.symbol = close_ticker.symbol
            and 1min_ticker_market.date_time = close_ticker.close_time '''
        self.cursor.execute(sql)

        for (symbol, close) in self.cursor:
            volume_by_day[symbol]['close_price'] = close

        # ----- 3. get the post-market info ----- #
        # get the high, low, total_volume
        sql = "select symbol, MAX(high) high, MIN(low) low, SUM(volume) volume from 1min_ticker_postmarket GROUP BY symbol ORDER BY symbol"
        # sql = "select * from 1min_ticker_premarket"
        self.cursor.execute(sql)

        for (symbol, high, low, volume) in self.cursor:
            if symbol in volume_by_day.keys():
                if high > volume_by_day[symbol]['high_price']:
                    volume_by_day[symbol]['high_price'] = high
                if low < volume_by_day[symbol]['low_price']:
                    volume_by_day[symbol]['low_price'] = low

                volume_by_day[symbol]['total_volume_day_postmarket'] = volume
                volume_by_day[symbol]['total_volume_day'] += volume
            else:
                temp_dict = {}
                temp_dict['symbol'] = symbol
                temp_dict['date'] = date_string
                temp_dict['open_price'] = None
                temp_dict['high_price'] = high
                temp_dict['low_price'] = low
                temp_dict['close_price'] = None
                temp_dict['total_volume_day_premarket'] = 0
                temp_dict['total_volume_day_market'] = 0
                temp_dict['total_volume_day_postmarket'] = volume
                temp_dict['total_volume_day'] = volume

                volume_by_day[symbol] = temp_dict
            
        # get the open price
        sql = '''
            SELECT 1min_ticker_postmarket.symbol, 1min_ticker_postmarket.open from 1min_ticker_postmarket, 
            (SELECT symbol, Min(date_time) as open_time from 1min_ticker_postmarket GROUP BY symbol) as open_ticker
            WHERE 1min_ticker_postmarket.symbol = open_ticker.symbol
            and 1min_ticker_postmarket.date_time = open_ticker.open_time '''
        self.cursor.execute(sql)

        for (symbol, open) in self.cursor:
            if volume_by_day[symbol]['open_price'] is None:
                volume_by_day[symbol]['open_price'] = open
                
        # get the open price
        sql = '''
            SELECT 1min_ticker_postmarket.symbol, 1min_ticker_postmarket.close from 1min_ticker_postmarket, 
            (SELECT symbol, Max(date_time) as close_time from 1min_ticker_postmarket GROUP BY symbol) as close_ticker
            WHERE 1min_ticker_postmarket.symbol = close_ticker.symbol
            and 1min_ticker_postmarket.date_time = close_ticker.close_time '''
        self.cursor.execute(sql)

        for (symbol, close) in self.cursor:
            volume_by_day[symbol]['close_price'] = close

        sql = '''
                INSERT INTO `{}` 
                ( `symbol`, `date`, `open_price`, `high_price`, `low_price`, `close_price`, `total_volume_day_premarket`, 
                `total_volume_day_market`, `total_volume_day_postmarket`, `total_volume_day` ) \
                VALUES ( %(symbol)s, %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(total_volume_day_premarket)s, 
                %(total_volume_day_market)s, %(total_volume_day_postmarket)s, %(total_volume_day)s ) \
            '''.format('ticker_volume_by_day')

        self.cursor.executemany(sql, list(volume_by_day.values()))
        self.cnx.commit()

    def generate_ticker_average_by_minute(self):

        average_value = []
        # extract info from ticker by day
        sql = '''
                SELECT ticker_volume_by_day.symbol, date, close_price, 
            	(last_info.average_volume_premarket / num_date / 150) as average_volume_premarket,
	            (last_info.average_volume_market / num_date / 390) as average_volume_market, 
                (last_info.average_volume_postmarket / num_date / 240) as average_volume_postmarket
                from ticker_volume_by_day, 
                (SELECT symbol, MAX(date) as last_date, 
                        SUM(total_volume_day_premarket) as average_volume_premarket, 
                        SUM(total_volume_day_market) as average_volume_market, 
                        SUM(total_volume_day_postmarket) as average_volume_postmarket,
                        COUNT(symbol) as num_date
                    from ticker_volume_by_day GROUP BY symbol) as last_info
                WHERE ticker_volume_by_day.symbol = last_info.symbol
                AND ticker_volume_by_day.date = last_info.last_date

        '''
        self.cursor.execute(sql)
        for symbol, date, close, avg_pre, avg_market, avg_post in self.cursor:
            temp_dict = {}
            temp_dict['symbol'] = symbol
            temp_dict['last_date'] = date
            temp_dict['latest_close'] = close
            temp_dict['average_volume_premarket'] = int(avg_pre) if avg_pre is not None else avg_pre
            temp_dict['average_volume_market'] = int(avg_market) if avg_market is not None else avg_market
            temp_dict['average_volume_postmarket'] = int(avg_post) if avg_post is not None else avg_post

            average_value.append(temp_dict)

        # save info to database
        sql = '''
                INSERT INTO `{}` 
                ( `symbol`, `last_date`, `latest_close`, `average_volume_premarket`, `average_volume_market`, `average_volume_postmarket`)
                VALUES ( %(symbol)s, %(last_date)s, %(latest_close)s, %(average_volume_premarket)s, %(average_volume_market)s, %(average_volume_postmarket)s )
            '''.format('ticker_average_by_minute')

        self.cursor.executemany(sql, average_value)
        self.cnx.commit()
        
        # output data to json
        with open('average_min.json', 'w') as outfile:
            json.dump(average_value, outfile)

        # keep data
        self.keep_daily_tickers()

        # clear tables to ready for next day
        self.clear_tables()