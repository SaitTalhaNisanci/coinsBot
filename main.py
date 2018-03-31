from binance.client import Client, BinanceRequestException, BinanceAPIException
import time
import math
import operator
import os
from multiprocessing import Pool
import twitter
from threading import Thread
from twitter_api_constants import *

BTC_TO_SATOSHI = 1e8


class binance(object):
    # Number of symbols to be displayed in statistics
    display_number_amount = 3
    # this is binance client not that we dont have to give actual api key and secret for now since we are not doing
    # anything with accounts. This should be changed in the future to do anything with accounts
    client = Client('api_key', 'api_secret')
    # This will give an error if you do not create a 'twitter_api_constants.py' in the current folder
    # and put your secret keys there. This file is added to .gitignore so that they wont be visible
    # on github.
    twitter_api = twitter.Api(consumer_key=CONSUMER_KEY,
                              consumer_secret=CONSUMER_SECRET,
                              access_token_key=ACCESS_TOKEN_KEY,
                              access_token_secret=ACCESS_TOKEN_SECRET)
    # last prices of symbols, there are 'array_size' values stored for each symbol
    lastPrices = {}
    # last volumes of symbols, there are 'array_size' values stored for each symbol
    lastVolumes = {}
    # current active alarms, this is stored to get information of current alarms, this does not have
    # any other functionality in the code.
    cur_alarms = {}
    # current index of lastPrices and lastVolumes, this is in the range of [0,array_size)
    current_index = 0
    # when volume change percentage of a coin is greater than this value it will give a notification
    # Note that a notification will also be given when the change is less than -1*vol_change_perc_threshold
    vol_change_perc_threshold = 2
    # when proce change percentage of a coin is greater than this value it will give a notification
    # Note that a notification will also be given when the change is less than -1*price_change_perc_threshold
    price_change_perc_threshold = 2
    # Symbols that do have less than this value will be filtered out, the reason for this is coins with little volume
    # are very volatile and they send a lot of notifications, the value is based on the coin, for example if the symbol
    # is TRXBTC then if the volume is less than volume_threshold BTC it will be filtered out, or if it is TRXETH if it is less
    # than volume_threshold ETH it will be filtered out
    volume_threshold = 500
    # if this value is True coins will be checked periodically to send a notification
    give_stats = False
    # these values are for bases of change calculations, for example if x,y,z are written then coins will be compared
    # to their x y z seconds ago prices
    periods_in_seconds = [30, 60, 90]
    # this is to filter out the same notifications, this map stores pairs of (symbol,changePercentage)
    # TODO:: we should make this with an id, currently if there is the same percentage change, which is very unlikely,
    # TODO:: it will not be shown
    # TODO:: scalability issue, we should clear this periodically, currently it wont have a lot of notifications but for future it can have a lot.
    sent_notifications = {}
    # this stores the indexes to look at for previous values, this is an helper for periods_in_seconds
    previous_value_index = []
    # this is how often we send a request to binance server. Note that the smaller this is the faster we get information
    # but the more CPU power as well
    wait_interval_in_seconds = min(periods_in_seconds) / 3
    # this is how often we send a request to binance server to see if a target price is reached for a coin
    alarm_check_period_secs = 3
    # calculate the previous indexes as a helper
    for value in periods_in_seconds:
        previous_value_index.append(value / wait_interval_in_seconds)
    # this is how many values we store for every coin
    array_size = math.ceil(max(periods_in_seconds) / wait_interval_in_seconds)

    def set_notification_for_twitter_account(self, account):
        """
        This method opens a twitter stream to listen tweets of the given accounts.
        Sends a notification when there is a tweet from the given accounts.
        :param account: A list of user ids to listen to
        :return:
        """
        tweet_stream = (self.twitter_api.GetStreamFilter(follow=account))
        for tweet in tweet_stream:
            # Skip replies
            if tweet['in_reply_to_status_id'] is not None:
                continue
            # Skip retweets
            if str(tweet['user']['id']) not in ACCOUNT_IDS:
                continue
            created_at = tweet['created_at']
            text = tweet['text']
            user = tweet['user']['name']
            title = user + ' ' + created_at
            self.notify(title, text)

    def give_statistics(self, *args):
        """
        Starts sending a periodic requests to binance server to see if there is any volume or price changes that are greater
        than the thresholds. Low volumes are filtered out.
        :param args: ETH BTC BNB OR USDT or any combination of them. If no parameters is given then BTC is chosen.
        :return:
        """
        filters = []
        if len(args) != 0:
            for arg in args:
                if len(arg) > 0:
                    filters.append(arg)
        else:
            filters.append('BTC')
        while binance.give_stats:
            binance_client.update_products(filters)
            time.sleep(self.wait_interval_in_seconds)

    def get_volume(self, symbol, lower, upper):
        """
        Get volume for the given symbol in the range of [lower,upper]
        :param symbol: TRXBTC XVGBTC etc
        :param lower: 520 in satoshi if not USDT
        :param upper: 560 in satoshi if not USDT
        :return:
        """
        lower = float(lower)
        upper = float(upper)
        # Convert to BTC if not USDT
        if 'USDT' not in symbol:
            lower = lower / BTC_TO_SATOSHI
            upper = upper / BTC_TO_SATOSHI

        base_coin = 'BTC'
        if symbol[-3:] == 'ETH':
            base_coin = 'ETH'
        elif symbol[-3:] == 'BNB':
            base_coin = 'BNB'
        try:
            bids = self.client.get_order_book(symbol=symbol, limit=1000)['bids']
            asks = self.client.get_order_book(symbol=symbol, limit=1000)['asks']
            bids = bids + asks
        except BinanceAPIException as err:
            print(err.message)
            print("symbol is not found, ", symbol)
            return
        total_amount = 0
        for price, amount, _ in bids:
            price = float(price)
            amount = float(amount)
            if price > lower and price < upper:
                total_amount += price * amount
        print('total amount of ', symbol[:-3], ' ,' in ',', base_coin, 'between', lower, '-', upper, ' : ',
              total_amount)

    def get_trade_history(self, symbol):
        closed_prices = {}
        trades = self.client.get_recent_trades(symbol=symbol)
        for trade in trades:
            price = float(trade['price'])
            qty = float(trade['qty'])
            if price not in closed_prices:
                closed_prices[price] = 0
            closed_prices[price] += qty
        price, qty = (max(closed_prices.items(), key=operator.itemgetter(1)))
        print('price: ', price, 'quantity ', qty * price)

    def set_notification_for_symbol(self, symbol, target_price):
        """
        Set notification for a symbol with a target price
        When the given symbol reaches to the target price a notification will be sent with the current price.
        :param symbol: TRXBTC etc
        :param target_price: target price in satoshi or in USDT
        :return:
        """
        target_price = float(target_price)
        # if there is already the same alarm then dont do anything
        if (symbol, target_price) in self.cur_alarms:
            return
        # add to current alarms for information purposes
        self.cur_alarms[(symbol, target_price)] = True
        if 'USDT' not in symbol:
            target_price = target_price / BTC_TO_SATOSHI
        is_reached = False
        # In the first request look at the current price and the target price and decide if this is a sell or buy alarm
        is_sell_target = None
        price = 0
        while not is_reached:
            try:
                data = binance_client.client.get_symbol_ticker(symbol=symbol)
            except BinanceAPIException:
                print("symbol: ", symbol, " could not be found")
                break
            except BinanceRequestException:
                print('got a binance request exception')
                break
            price = float(data['price'])
            if is_sell_target is None:
                print("succesfully set notification for ", symbol, ' ', str(target_price))
                if target_price > price:
                    is_sell_target = True
                else:
                    is_sell_target = False
            if is_sell_target:
                if price >= target_price:
                    is_reached = True
            else:
                if price <= target_price:
                    is_reached = True
            if not is_reached:
                time.sleep(self.alarm_check_period_secs)
        if is_reached:
            del self.cur_alarms[(symbol, target_price)]
            text = symbol + " price reached to target : " + str(price)
            self.notify(symbol, text)

    def does_exist_otherwise_add(self, cur):
        if (cur[0], cur[1]) in self.sent_notifications:
            return True
        else:
            self.sent_notifications[(cur[0], cur[1])] = True
            return False

    def display_current_alarms(self):
        """
        display current alarms for information purposes
        :return:
        """
        for symbol, target_price in self.cur_alarms.keys():
            print(symbol, target_price)

    def update_products(self, filters):
        """
        this is the internal method for getting statistics, this sends a request to binance server and updates the
        last prices and volumes, and checks if there is any notification case with the set thresholds, if so sends a notification
        :param filters:
        :return:
        """

        def get_history_indexes():
            """
            With the periods find which indexes to look in the last prices and volumes to get the correct change.
            :return: a list of indexes
            """
            history_indexes = []
            for index in self.previous_value_index:
                index = self.current_index - index
                if index < 0:
                    index += self.array_size
                history_indexes.append(int(index))
            return history_indexes

        # get products information from the server
        products = self.client.get_products()['data']
        # since there are 3 periods for now make a list of 3 arrays and store the changes
        # TODO:: make these generic
        cur_volume_percentage_changes = [[], [], []]
        cur_price_percentage_changes = [[], [], []]

        history_indexes = get_history_indexes()
        for product in products:
            symbol = product['symbol']
            cur_price = float(product['close'])
            cur_volume = float(product['tradedMoney'])
            # if the current symbol doesnt belongs to any filters then continue
            if not any(filter in symbol for filter in filters):
                continue

            if cur_volume < self.volume_threshold:
                continue
            if symbol != "":
                if symbol not in self.lastPrices:
                    self.lastPrices[symbol] = [0] * self.array_size
                if symbol not in self.lastVolumes:
                    self.lastVolumes[symbol] = [0] * self.array_size

                for index, history_index in enumerate(history_indexes):
                    perc_change = 0
                    # if this value is zero then we dont have enough data yet so continue otherwise the change will be infinity.
                    if self.lastPrices[symbol][history_index] != 0:
                        perc_change = (cur_price - self.lastPrices[symbol][history_index]) / self.lastPrices[symbol][
                            history_index] * 100
                    cur_price_percentage_changes[index].append([symbol, perc_change])

                    volume_change_perc = 0
                    # if this value is zero then we dont have enough data yet so continue otherwise the change will be infinity.
                    if self.lastVolumes[symbol][history_index] != 0:
                        volume_change_perc = (cur_volume - self.lastVolumes[symbol][history_index]) / \
                                             self.lastVolumes[symbol][
                                                 history_index] * 100
                    cur_volume_percentage_changes[index].append([symbol, volume_change_perc])
                # update the volumes and prices
                self.lastVolumes[symbol][self.current_index] = cur_volume
                self.lastPrices[symbol][self.current_index] = cur_price
        # sort the changes to get the maximum and minimum ones
        for i, _ in enumerate(self.periods_in_seconds):
            cur_price_percentage_changes[i].sort(key=operator.itemgetter(1), reverse=True)
            cur_volume_percentage_changes[i].sort(key=operator.itemgetter(1), reverse=True)

        # update the index, not that if we reach to the end we go to the beginning.
        self.current_index += 1
        self.current_index %= self.array_size

        for i in range(0, len(self.periods_in_seconds)):
            print(self.periods_in_seconds[i], " seconds change")
            for k in range(0, self.display_number_amount):
                from_beg = cur_price_percentage_changes[i][k]
                from_end = cur_price_percentage_changes[i][len(cur_price_percentage_changes[i]) - 1 - k]
                if from_beg[1] >= self.price_change_perc_threshold:
                    if not self.does_exist_otherwise_add(from_beg):
                        text = "price percentage change: " + str(from_beg[0]) + " : " + str(from_beg[1])
                        self.notify(from_beg[0], text)
                if from_end[1] <= -1 * self.price_change_perc_threshold:
                    if not self.does_exist_otherwise_add(from_end):
                        text = "price percentage change: " + str(from_end[0]) + " : " + str(from_end[1])
                        self.notify(from_end[0], text)
                print("symbol: ", from_beg[0], " max positive price change ", from_beg[1], "% --- ", "symbol: ",
                      from_end[0], " max negative price change ", from_end[1])
            print()
        for i in range(0, len(self.periods_in_seconds)):
            print(self.periods_in_seconds[i], " seconds change")
            for k in range(0, self.display_number_amount):
                from_beg = cur_volume_percentage_changes[i][k]
                from_end = cur_volume_percentage_changes[i][len(cur_volume_percentage_changes[i]) - 1 - k]
                if from_beg[1] >= self.vol_change_perc_threshold:
                    if not self.does_exist_otherwise_add(from_beg):
                        text = "volume percentage change: " + str(from_beg[0]) + " : " + str(from_beg[1])
                        self.notify(from_beg[0], text)
                if from_end[1] <= -1 * self.vol_change_perc_threshold:
                    if not self.does_exist_otherwise_add(from_end):
                        text = "volume percentage change: " + str(from_end[0]) + " : " + str(from_end[1])
                        self.notify(from_end[0], text)

                print("symbol: ", from_beg[0], " maximum positive volume change ", from_beg[1], "% --- ", "symbol: ",
                      from_end[0],
                      " max negative volume change ", from_end[1])
            print()

    def notify(self, title, text):
        os.system("""
                      osascript -e 'display notification "{}" with title "{}"'
                      """.format(text, title))


def get_user_input():
    print("options:")
    print('1 to get top 3 volume and price changes of coins; 1 BTC')
    print('2 to get volume in a range of a symbol, 2 TRXBTC 0.00000520 0.00000560')
    print('3 to get trade history of a symbol, 3 TRXBTC')
    print('4 to stop getting statistics 4')
    print('5 to set an alarm for a symbol, 5 TRXBTC 0.00000520')
    print('6 to get alarms, 6')
    user_input = input()
    return user_input.split(' ')


def update_products(*args):
    binance_client.give_statistics(*args)


def set_notification_for_symbol(symbol, target_price):
    binance_client.set_notification_for_symbol(symbol, target_price)


USERS = ['@twitter',
         '@twitterapi',
         '@support']

ACCOUNT_IDS = [
    '2170600542',  # me
    '902926941413453824',  # cz_binance
    '4826209539',  # vergecurrency
    '886832413',  # bitfinex
    '295218901',  # vitalikbuterin
]

if __name__ == '__main__':

    binance_client = binance()
    pool = Pool(processes=10)
    pool.apply_async(binance_client.set_notification_for_twitter_account, [ACCOUNT_IDS])
    while True:
        try:
            char = get_user_input()
            while len(char) == 0:
                char = get_user_input()
            if char[0] == '1':
                if not binance.give_stats:
                    binance.give_stats = True
                    thread = Thread(target=update_products, args=(char[1:]))
                    thread.start()
            elif char[0] == '2':
                if len(char) == 4:
                    binance_client.get_volume(char[1], char[2], char[3])
            elif char[0] == '3':
                if len(char) == 2:
                    binance_client.get_trade_history(char[1])
            elif char[0] == '4':
                binance.give_stats = False
            elif char[0] == '5':
                if len(char) == 3:
                    thread = Thread(target=set_notification_for_symbol, args=(char[1], char[2]))
                    thread.start()
            elif char[0] == '6':
                binance_client.display_current_alarms()
        except BinanceAPIException:
            print("got binance API exception")
        except BinanceRequestException:
            print("got binance request exception")
        except Exception as message:
            print("got an exception")
            print(message)
