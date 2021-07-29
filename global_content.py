
def initialize():

    # Market type
    global MARKET
    global PRE_MARKET
    global POST_MARKET
    global market_type
    global market_label

    PRE_MARKET = 0
    MARKET = 1
    POST_MARKET = 2
    market_type = -1
    market_label = ["Pre-Market", "Market", "Post-Market"]

    # Api
    global TDA_CONSUMER_KEY
    global TDA_ACCOUNT

    TDA_CONSUMER_KEY = 'IVXFC1GUFWWQ9SYFDICZCQDDBMKYFSH6@AMER.OAUTHAP'
    TDA_ACCOUNT = 489617046

    # database
    global DB_NAME
    global DB_USER
    global DB_PASS
    global DB_HOST

    DB_NAME = "stock_market"
    DB_USER = "root"
    DB_PASS = "root"
    DB_HOST = "localhost"

    # Runners
    global runners_rate
    global runners_checkout

    runners_rate = [4, 7, 4]
    runners_checkout = 20

    # Gapper
    global gapper_rate

    gapper_rate = 5
