# import packages

from datetime import datetime, timedelta
import calendar
from threading import current_thread
import time
from typing_extensions import ParamSpecArgs
from pandas.io import sql
import pytz
import json

# import global contents
import global_content as gl_content 

def is_market_opened(current_time = None):
    """Check the stock market is opened or closed
    """

    if current_time is None:
        current_time = datetime.now(pytz.timezone('US/Eastern'))

    start_time_string = current_time.strftime("%Y-%m-%d") + " " + "07:00:00"
    start_time = time.strptime(start_time_string, "%Y-%m-%d %H:%M:%S")
    start_time = datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern')) \
                    + timedelta(seconds=calendar.timegm(start_time))

    end_time_string = current_time.strftime("%Y-%m-%d") + " " + "20:00:00"
    end_time = time.strptime(end_time_string, "%Y-%m-%d %H:%M:%S")
    end_time = datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern')) \
                    + timedelta(seconds=calendar.timegm(end_time))

    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    print(current_time)
    print(start_time)

    if current_time > start_time and current_time < end_time:
        return True
    else:
        return False


def get_current_market(current_time = None):
    """Get the current stock marget type
    Returns:
        string: Error message if the market is closed
        int: current stock market type (PRE_MARKET, MARKET, POST_MARKET).
             -1 if the stock market is closed
    """
    
    if current_time is None:
        current_time = datetime.now(pytz.timezone('US/Eastern'))

    # check the stock market is opened or not
    if is_market_opened(current_time):
        market_time_string = current_time.strftime("%Y-%m-%d") + " " + "09:31:00"
        market_time = time.strptime(market_time_string, "%Y-%m-%d %H:%M:%S")
        market_time = datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern')) \
                        + timedelta(seconds=calendar.timegm(market_time))
        post_market_time_string = current_time.strftime("%Y-%m-%d") + " " + "16:01:00"
        post_market_time = time.strptime(post_market_time_string, "%Y-%m-%d %H:%M:%S")
        post_market_time = datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern')) \
                        + timedelta(seconds=calendar.timegm(post_market_time))

        current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        market_time = market_time.strftime("%Y-%m-%d %H:%M:%S")
        post_market_time = post_market_time.strftime("%Y-%m-%d %H:%M:%S")

        if current_time < market_time:
            return None, gl_content.PRE_MARKET

        if current_time < post_market_time:
            return None, gl_content.MARKET

        return None, gl_content.POST_MARKET

    else:
        return "The stock market is closed now.", -1

def is_runner(stock, total_volume, market_type, db_helper, minute_mode=True):
    """Check stock's ticker is runner or not

    Args:
        stock (string): the name of stock
        total_volume (int) : total volume of ticker
        market_type (int): type of market of current time 
                            [0 : pre-market, 1 : market, 2 : post-market]
        db_helper (DatabaseHelper) : Helper class object to interact with DB
        minute_mode (bool, optional): Check runners for minute data or not. 
                                        [True : minute data]
                                        Defaults to True.

    Returns:
        True if ticker info is runner, False in othercase
    """

    # get the average volume of specific stock from database
    avg_volume = db_helper.get_average_volume(market_type, stock)

    # if there is no previous data of stock, return False
    if avg_volume is None:
        if market_type in [0, 2]:
            avg_volume = db_helper.get_average_volume(1, stock)
            if avg_volume is None:
                return False
        else:
            return False

    # calculate the limit volume which used for check runner or not
    limit_volume = avg_volume * pow(2, gl_content.runners_rate[market_type])
    # limit_volume = avg_volume *  gl_content.runners_rate[market_type]

    # calculate limit volume for specific timeout (not minute)
    if not minute_mode:
        limit_volume = limit_volume / (60 / gl_content.runners_checkout)

    if total_volume > limit_volume:
        return True

    return False

def is_gapper(stock_info, market_type, db_helper):
    """Check the given stock trade is gapper or not

    Args:
        stock_info (dict): trade info of given stock
        market_type (int): current stock market type [PRE_MARKET, MARKET, POST_MARKET]
        db_helper (DatabaseHelper): Helper class object to interact with DB

    Returns:
        (Boolean): True if trade is gapper, False in otherwise
    """

    # check there is any trade of stock on today
    previous_data = db_helper.get_stock_1min_data(stock_info['symbol'], market_type)    
    if len(previous_data) > 0:
        return False, (None, None, None, None)

    last_price = db_helper.get_last_price(stock_info['symbol'])
    if last_price is None:
        pass
        price_change = stock_info['open']
        price_percent = price_change * 100
    else:
        price_change = stock_info['open'] - last_price
        price_percent = price_change / last_price * 100

    if abs(price_percent) > gl_content.gapper_rate:
        return True, (stock_info['symbol'], last_price, price_change, price_percent)
    else:
        return False, (None, None, None, None)

