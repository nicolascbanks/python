import requests
import configparser
import json
import re
import csv
import datetime
import pandas as pd
from telethon import TelegramClient, events
from pairs import get_all_pairs
from methods import get_methods
from jpy_pairs import get_jpy
from non_usd_pairs import get_non_usd_pairs, get_before_usd_pairs


# Reading Configs
config = configparser.ConfigParser()
config.read('oanda_config.ini')

# Telegram config values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
api_hash = str(api_hash)
phone = config['Telegram']['phone']
username = config['Telegram']['username']

# Oanda config values
oanda_api = config['Oanda']['practice_api']
oanda_token = config['Oanda']['token']
oanda_account_id = config['Oanda']['account_id']
oanda_orders_path = config['Oanda']['orders_path']
oanda_trades_path = config['Oanda']['trades_path']
oanda_pending_path = config['Oanda']['pending_orders_path']
oanda_open_trades_path = config['Oanda']['open_trades_path']
oanda_positions_path = config['Oanda']['positions_path']
oanda_acc_summary_path = config['Oanda']['account_summary_path']
oanda_pricing_path = config['Oanda']['pricing_path']

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)

# async def main():
#     # Getting information about yourself
#     me = await client.get_me()
#     username = me.username
#     phone = me.phone
#
#     # "me" is a user object. You can pretty-print
#     # any Telegram object with the "stringify" method:
#     print(username)
#     print(phone)
#
# with client:
#     client.loop.run_until_complete(main())

@client.on(events.NewMessage)
# async def my_event_handler(event): -> Async version
def my_event_handler(event):

    ### Variables ###

    signal_channel = 1302702985
    # Test bot: 1161888901
    # Century Capital Signals: 1176009023
    # New CCG Signals Channel: 1302702985
    channel_id = event.input_chat.channel_id
    is_reply = event.is_reply
    message_id = event.id
    message_text = event.raw_text
    reply_message = event.reply_to_msg_id
    split_text = re.split('\s', message_text)
    all_pairs = get_all_pairs()
    all_methods = get_methods()
    jpy_pairs = get_jpy()
    non_usd_pairs = get_non_usd_pairs()
    before_usd_pairs = get_before_usd_pairs()
    now = datetime.datetime.now()
    csv_file = 'trade_db.csv'

    ### Functions ###

    # Get the position of the signal (Buy or Sell)
    def get_buy_position(signal):
        position = []
        for i in signal:
            if i.lower() == 'buy':
                position.append(i)
                return position[0]

    def get_sell_position(signal):
        position = []
        for i in signal:
            if i.lower() == 'sell':
                position.append(i)
                return position[0]

    def get_limit_order(signal):
        limit = []
        for i in signal:
            if i.lower() == 'limit':
                limit.append(i)
                return limit[0]

    # Get the currency pair to trade
    def get_pair(signal):
        pair = []
        for i in signal:
            if i in all_pairs:
                pair.append(all_pairs[i])
                return pair[0]
                # Need to figure out if signal comes with pair delimited by a space (GBP USD)

    # Get the stop loss price of the signal
    def get_stop_loss(signal):
        for i in signal:
            if (i == 'SL:' or i == 'SL' or i == 'SL.'):
                sl_index = signal.index(i)
                return signal[sl_index + 1]

    # Get the take profit price of the signal
    def get_take_profit(signal):
        for i in signal:
            if (i == 'TP:' or i == 'TP' or i == 'TP.'):
                tp_index = signal.index(i)
                return signal[tp_index + 1]

    def get_acc_value():
    	headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token}
    	response = requests.get(oanda_api+oanda_account_id+oanda_acc_summary_path, headers=headers) # Execute order
    	response_data = response.json()
    	return float(response_data['account']['balance'])

    def get_pair_price(pair):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
            }
        response = requests.get(oanda_api+oanda_account_id+oanda_pricing_path+"?instruments="+pair, headers=headers)
        response_data = response.json()
        return float(response_data['prices'][0]['asks'][0]['price'])

    def get_position_method(pair):
        method = []
        for i in pair:
            if i in all_methods:
                method.append(all_methods[i])
                return method[0]

    def get_position_jpy(pair):
        jpy = []
        for i in pair:
            if i in jpy_pairs:
                jpy.append(jpy_pairs[i])
                return jpy[0]


    def get_position_fx(pair):
        split_pair = re.split('_', pair)
        counter_curr = split_pair[1]

        if counter_curr in before_usd_pairs:
            ex_pair = counter_curr + '_USD'
        else:
            ex_pair = 'USD_' + counter_curr

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
            }
        response = requests.get(oanda_api+oanda_account_id+oanda_pricing_path+"?instruments="+ex_pair, headers=headers)
        response_data = response.json()
        fx_price = float(response_data['prices'][0]['asks'][0]['price'])

        return fx_price



    def size_position(price, stop, risk, method=0, exchange_rate=None, JPY_pair=False):
        '''
        Helper function to calcuate the position size given a known amount of risk.

        *Args*
        - price: Float, the current price of the instrument
        - stop: Float, price level of the stop loss
        - risk: Float, the amount of the account equity to risk

        *Kwargs*
        - JPY_pair: Bool, whether the instrument being traded is part of a JPY
        pair. The muliplier used for calculations will be changed as a result.
        - Method: Int,
            - 0: Acc currency and counter currency are the same
            - 1: Acc currency is same as base currency
            - 2: Acc currency is neither same as base or counter currency
        - exchange_rate: Float, is the exchange rate between the account currency
        and the counter currency. Required for method 2.
        '''

        if JPY_pair == True: #check if a YEN cross and change the multiplier
            multiplier = 0.01
        else:
            multiplier = 0.0001

        #Calc how much to risk
        acc_value = get_acc_value()
        cash_risk = acc_value * risk
        stop_pips_int = abs((price - stop) / multiplier)
        pip_value = cash_risk / stop_pips_int

        if method == 1:
            #pip_value = pip_value * price
            units = pip_value / multiplier
            return int(units)

        elif method == 2:
            pip_value = pip_value * exchange_rate
            units = pip_value / multiplier
            return int(units)

        else: # is method 0
            units = pip_value / multiplier
            return int(units)


    def extract_element_from_json(obj, path):

        def extract(obj, path, ind, arr):

            key = path[ind]
            if ind + 1 < len(path):
                if isinstance(obj, dict):
                    if key in obj.keys():
                        extract(obj.get(key), path, ind + 1, arr)
                    else:
                        arr.append(None)
                elif isinstance(obj, list):
                    if not obj:
                        arr.append(None)
                    else:
                        for item in obj:
                            extract(item, path, ind, arr)
                else:
                    arr.append(None)
            if ind + 1 == len(path):
                if isinstance(obj, list):
                    if not obj:
                        arr.append(None)
                    else:
                        for item in obj:
                            arr.append(item.get(key, None))
                elif isinstance(obj, dict):
                    arr.append(obj.get(key, None))
                else:
                    arr.append(None)
            return arr
        if isinstance(obj, dict):
            return extract(obj, path, 0, [])
        elif isinstance(obj, list):
            outer_arr = []
            for item in obj:
                outer_arr.append(extract(item, path, 0, []))
            return outer_arr



    def submit_market_order(pair, stoploss, takeprofit, units):
        order = {
          "order": {
            "stopLossOnFill": {
              "price": stoploss
            },
            "takeProfitOnFill": {
              "price": takeprofit
            },
            "instrument": pair,
            "units": units,
            "type": "MARKET",
            "positionFill": "DEFAULT"
          }
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
        }

        data = json.dumps(order)

        response = requests.post(oanda_api+oanda_account_id+oanda_orders_path, headers=headers, data=data) # Execute order

        response_data = response.json()
        submit_market_order.response_status_code = response.status_code
        submit_market_order.trade_id = response_data['orderFillTransaction']['tradeOpened']['tradeID']
        submit_market_order.instrument = response_data['orderFillTransaction']['instrument']
        submit_market_order.take_profit = response_data['orderCreateTransaction']['takeProfitOnFill']['price']
        submit_market_order.stop_loss = response_data['orderCreateTransaction']['stopLossOnFill']['price']
        submit_market_order.units = response_data['orderFillTransaction']['units']


    def set_sl_to_be(orderid, tradeid, price):
        order = {
            "order": {
                "timeInForce": "GTC",
                "price": str(float(price)),
                "type": "STOP_LOSS",
                "tradeID": str(tradeid)
            }
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
            }


        data = json.dumps(order)
        response = requests.put(oanda_api+oanda_account_id+oanda_orders_path+"/"+str(orderid), headers=headers, data=data) # Execute order
        response_data = response.json()
        print(response_data)


    def take_partials(trade_id, units):
        order = {
          "units": str(units)
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
        }

        data = json.dumps(order)

        response = requests.put(oanda_api+oanda_account_id+oanda_trades_path+"/"+str(trade_id)+"/close", headers=headers, data=data)
        response_data = response.json()
        print(response_data)

    def close_trade(trade_id):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
        }

        response = requests.put(oanda_api+oanda_account_id+oanda_trades_path+"/"+str(trade_id)+"/close", headers=headers)
        response_data = response.json()
        print(response_data)

    def log_trade(csvfile, signal, trade, pair, position, tp, sl):
        with open(csvfile, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([signal, trade, pair, position, tp, sl])

    def get_signal_list(csvfile):
        data = pd.read_csv(csvfile)
        signal_list = list(data['signal_id'])
        return signal_list

    def get_trade_assoc_with_reply(csvfile, reply):
        data = pd.read_csv(csvfile)
        data_i = data.set_index('signal_id') # Set the signal_id column as DataFrame index
        trade = data_i.loc[reply, 'trade_id'] # Assign value of trade_id column that corresponds to xxx index
        return trade

    def get_specific_trade(trade_id):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+ oanda_token
            }

        response = requests.get(oanda_api+oanda_account_id+oanda_trades_path+"/"+str(trade_id), headers=headers)
        response_data = response.json()
        return response_data

    buy_position = get_buy_position(split_text)
    sell_position = get_sell_position(split_text)
    limit_order = get_limit_order(split_text)
    trade_pair = get_pair(split_text)
    trade_sl = get_stop_loss(split_text)
    trade_tp = get_take_profit(split_text)
    is_buy_signal = buy_position and trade_pair and trade_sl and trade_tp
    is_sell_signal = sell_position and trade_pair and trade_sl and trade_tp
    is_buy_limit_signal = buy_position and trade_pair and trade_sl and trade_tp and limit_order
    is_sell_limit_signal = sell_position and trade_pair and trade_sl and trade_tp and limit_order

    signal_list = get_signal_list(csv_file)
    risk = 0.01

    # print(type(trade_position))
    # print(type(trade_pair))
    # print(type(trade_sl))
    # print(type(trade_tp))

    if channel_id == signal_channel:
        print(now)
        print(message_text)

        if is_reply: # Check if message is a reply

            # Check if message being replied to is an open trade
            if reply_message in signal_list:
                assoc_trade = get_trade_assoc_with_reply(csv_file, reply_message)
                sl_of_specific_trade = extract_element_from_json(get_specific_trade(assoc_trade), ["trade", "stopLossOrder", "id"])
                price_of_specific_trade = extract_element_from_json(get_specific_trade(assoc_trade), ["trade", "price"])
                current_units = extract_element_from_json(get_specific_trade(assoc_trade), ["trade", "currentUnits"])
                units_to_close = abs(round(float(current_units[0]) / 2))

                if 'sl to be' in message_text.lower():
                    # Set Stop Loss to break even and take partials
                    set_sl_to_be(sl_of_specific_trade[0], assoc_trade, price_of_specific_trade[0])
                    take_partials(assoc_trade, units_to_close)

                elif ('partials' in message_text.lower()) or ('take partials' in message_text.lower()) or ('secure partials' in message_text.lower()):
                    # Take Partials
                    take_partials(assoc_trade, units_to_close)

                elif ('close' in message_text.lower()) or ('close fully' in message_text.lower()) or ('fully close' in message_text.lower()):
                    # Close trade
                    close_trade(assoc_trade)
            else:
                print ('Reply is not from a signal')
        else:
            if is_buy_signal:
                price = get_pair_price(trade_pair)
                position_method = get_position_method([trade_pair])
                jpy_pair = get_position_jpy([trade_pair])

                if trade_pair in non_usd_pairs:
                    ex_rate = get_position_fx(trade_pair)
                else:
                    ex_rate = None

                units = size_position(price, float(trade_sl), risk, position_method, ex_rate, jpy_pair)

                print(price)
                print(units)
                print(position_method)
                print(jpy_pair)
                print(ex_rate)
                submit_market_order(trade_pair, trade_sl, trade_tp, units)
                print('Order submitted: ' + submit_market_order.trade_id)
                if submit_market_order.response_status_code == 201:
                    log_trade(csv_file, message_id, submit_market_order.trade_id, submit_market_order.instrument, buy_position, submit_market_order.take_profit, submit_market_order.stop_loss)
                    print('Order logged')

            elif is_sell_signal:
                price = get_pair_price(trade_pair)
                position_method = get_position_method([trade_pair])
                jpy_pair = get_position_jpy([trade_pair])

                if trade_pair in non_usd_pairs:
                    ex_rate = get_position_fx(trade_pair)
                else:
                    ex_rate = None

                units = float(size_position(price, float(trade_sl), risk, position_method, ex_rate, jpy_pair) * -1)
                print(price)
                print(units)
                print(position_method)
                print(jpy_pair)
                print(ex_rate)
                submit_market_order(trade_pair, trade_sl, trade_tp, str(units))
                print('Order submitted: ' + submit_market_order.trade_id)
                if submit_market_order.response_status_code == 201:
                    log_trade(csv_file, message_id, submit_market_order.trade_id, submit_market_order.instrument, sell_position, submit_market_order.take_profit, submit_market_order.stop_loss)
                    print('Order logged')

            else:
                print('Not a signal')

    else:
        print('Not signals channel')



    # is_reply = event.is_reply
    # signal = event.raw_text
    # event_id = event.id
    # reply_message = event.reply_to_msg_id
    # chat = event.input_chat
    #
    # print(signal)
    # print(event_id)
    # print(reply_message)
    # print(chat.channel_id)
    # print(event)

client.start()
client.run_until_disconnected()
