# import packages


from collections import defaultdict
from typing import Sequence


class OneMinuteData:
    """Play with the stream data for one minute period
    """

    @property
    def length(self):
        return len(self.sequence_data)

    @property
    def sequence(self):
        return self.sequence_data

    def __init__(self):

        self.sequence_data = []
        

    def append_stream(self, stream):
        """parse the stream data and append to sequence data list

        Args:
            stream (dict): received stream from client
        """

        for item in stream["content"]:
            self.sequence_data.append(
                {
                    "symbol": item['key'], 
                    "price": item['LAST_PRICE'],
                    "volume": item['LAST_SIZE']
                })

    def append_ticker(self, ticker):
        """append one ticker info to sequence data list

        Args:
            ticker (dict): received one ticker value from client
        """

        self.sequence_data.append(
            {
                "symbol": ticker['key'], 
                "price": ticker['LAST_PRICE'],
                "volume": ticker['LAST_SIZE']
            })

    def get_result(self):

        result = []

        # extract the unique symbols which is received in one minute
        exit_symbols = set([item['symbol'] for item in self.sequence_data])

        # calculate all values for each symbols
        for symbol in exit_symbols:
            sym_data = [item for item in self.sequence_data if item['symbol'] == symbol]

            temp_dict = {}
            temp_dict['symbol'] = symbol
            temp_dict['date_time'] = ''
            temp_dict['open'] = sym_data[0]['price']
            temp_dict['high'] = max([data['price'] for data in sym_data])
            temp_dict['low'] = min([data['price'] for data in sym_data])
            temp_dict['close'] = sym_data[-1]['price']
            temp_dict['volume'] = sum([data['volume'] for data in sym_data])

            result.append(temp_dict)
        
        return result

