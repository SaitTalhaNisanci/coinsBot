from binance.client import Client, BinanceRequestException, BinanceAPIException
import time
import math
import operator
import os
from multiprocessing import Pool
import twitter
from threading import Thread
from twitter_api_constants import *


class binance(object):
    display_number_amount = 3
    client = Client('api_key', 'api_secret')
    twitter_api = twitter.Api(consumer_key=CONSUMER_KEY,
                              consumer_secret=CONSUMER_SECRET,
                              access_token_key=ACCESS_TOKEN_KEY,
                              access_token_secret=ACCESS_TOKEN_SECRET)
    lastPrices = {}
    lastVolumes = {}
    current_index = 0
    vol_change_perc_threshold = 3
    price_change_perc_threshold = 4
    volume_threshold = 500
    give_stats = False
    periods_in_seconds = [30, 60, 90]
    sent_notifications = {}
    previous_value_index = []
    wait_interval_in_seconds = min(periods_in_seconds) / 3
    alarm_check_period_secs = 10
    for value in periods_in_seconds:
        previous_value_index.append(value / wait_interval_in_seconds)
    array_size = math.ceil(max(periods_in_seconds) / wait_interval_in_seconds)

    def set_notification_for_twitter_account(self, account):
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

    def give_statistics(self,*args):
        filters = []
        if len(args) != 0:
            for arg in args:
                if len(arg) > 0:
                    filters.append(arg)
        else:
            filters.append('BTC')
        print(filters)
        while binance.give_stats:
            binance_client.update_products(filters)
            time.sleep(self.wait_interval_in_seconds)

    def get_volume(self, symbol, lower, upper):
        lower = float(lower)
        upper = float(upper)
        base_coin = 'BTC'
        if symbol[-3:] == 'ETH':
            base_coin = 'ETH'
        elif symbol[-3:] == 'BNB':
            base_coin = 'BNB'
        try:
            bids = self.client.get_order_book(symbol=symbol, limit=1000)['bids']
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
        target_price = float(target_price)
        is_reached = False
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
            text = symbol + " price reached to target : " + str(price)
            self.notify(symbol, text)
    def does_exist_otherwise_add(self,cur):
        if (cur[0],cur[1]) in self.sent_notifications:
            return True
        else:
            self.sent_notifications[(cur[0],cur[1])] = True
            return False

    def update_products(self,filters):
        def get_history_indexes():
            history_indexes = []
            for index in self.previous_value_index:
                index = self.current_index - index
                if index < 0:
                    index += self.array_size
                history_indexes.append(int(index))
            return history_indexes

        products = self.client.get_products()['data']
        cur_volume_percentage_changes = [[], [], []]
        cur_price_percentage_changes = [[], [], []]

        history_indexes = get_history_indexes()
        for product in products:
            symbol = product['symbol']
            cur_price = float(product['close'])
            cur_volume = float(product['tradedMoney'])

            if not any(filter in symbol for filter in filters):
                continue

            if cur_volume < self.volume_threshold:
                continue
            if symbol != "":
                if symbol not in self.lastPrices:
                    self.lastPrices[symbol] = [0] * self.array_size
                if symbol not in self.lastVolumes:
                    self.lastVolumes[symbol] = [0] * self.array_size

                for i, index in enumerate(history_indexes):
                    perc_change = 0
                    print(self.lastPrices[symbol][index],self.lastVolumes[symbol][index],symbol,cur_price,cur_volume)
                    if self.lastPrices[symbol][index] != 0:
                        perc_change = (cur_price - self.lastPrices[symbol][index]) / self.lastPrices[symbol][
                            index] * 100
                    cur_price_percentage_changes[i].append([symbol, perc_change])

                    volume_change_perc = 0
                    if self.lastVolumes[symbol][index] != 0:
                        volume_change_perc = (cur_volume - self.lastVolumes[symbol][index]) / self.lastVolumes[symbol][
                            index] * 100
                    cur_volume_percentage_changes[i].append([symbol, volume_change_perc])

                self.lastVolumes[symbol][self.current_index] = cur_volume
                self.lastPrices[symbol][self.current_index] = cur_price
        for i, _ in enumerate(self.periods_in_seconds):
            cur_price_percentage_changes[i].sort(key=operator.itemgetter(1), reverse=True)
            cur_volume_percentage_changes[i].sort(key=operator.itemgetter(1), reverse=True)
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
                cur = cur_volume_percentage_changes[i][k]
                from_end = cur_volume_percentage_changes[i][len(cur_price_percentage_changes[i]) - 1 - k]
                if cur[1] >= self.vol_change_perc_threshold:
                    if not self.does_exist_otherwise_add(cur):
                        text = "volume percentage change: " + str(cur[0]) + " : " + str(cur[1])
                        self.notify(cur[0], text)
                if from_end[1] <= -1 * self.vol_change_perc_threshold:
                    if not self.does_exist_otherwise_add(from_end):
                        text = "volume percentage change: " + str(from_end[0]) + " : " + str(from_end[1])
                        self.notify(from_end[0], text)

                print("symbol: ", cur[0], " maximum positive volume change ", cur[1], "% --- ", "symbol: ", from_end[0],
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
                    thread = Thread(target=update_products,args=(char[1:]))
                    thread.start()
                    # pool.apply_async(binance_client.give_statistics, [])
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
                    # pool.apply_async(set_notification_for_symbol, (char[1],char[2]))
        except BinanceAPIException:
            print("got binance API exception")
        except BinanceRequestException:
            print("got binance request exception")
        except Exception as message:
            print("got an exception")
            print(message)

            # notify("heisenberg","this is notifying you")
