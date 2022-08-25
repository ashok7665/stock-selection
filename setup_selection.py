
from smartapi import SmartConnect  # or from
import pandas as pd
import pymongo
from datetime import date as dateObj
import datetime
import time

# CONFIGRAION
CANDLE_PERCENT = 0.05

# INIT
myclient = pymongo.MongoClient(
    "mongodb+srv://admin:admin@cluster0.6ur6q.mongodb.net/msquare?retryWrites=true&w=majority", connect=False)
mydb = myclient["msquare"]
trades = mydb["trades"]
today = dateObj.today()

obj = SmartConnect(api_key="ilgUlOtU")
data = obj.generateSession("P246447", "Jangir76@")
print(data)
refreshToken = data['data']['refreshToken']
print(refreshToken)
trade_list = trades.find({"date": "{}".format(today)})


def fetchQuantity(price):
    if price > 10000:
        return 10
    elif price > 5000:
        return 20
    elif price > 1000:
        return 60
    elif price > 500:
        return 200
    else:
        return 300


def cleanData(_df):
    _df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Di']
    _df.index = _df['Time']
    _df.drop('Time', axis='columns', inplace=True)
    _df.drop('Di', axis='columns', inplace=True)
    return _df


today_date = dateObj.today()
today_date = "{}".format(today_date)

now_datetime = datetime.datetime.now()

print('\n\n-----------------------------------')
print("datetime: {}".format(today))


try:
    print('okey fucc')
    for row in trade_list:
        print('data ', row)
        time.sleep(0.5)
        historicParam = {
            "exchange": "NSE",
            "symboltoken": row["symbol_token"],
            "interval": "FIFTEEN_MINUTE",
            "fromdate": today_date+" 09:15",
            "todate": today_date+" 09:30",
        }
        historicData = obj.getCandleData(historicParam)
        if historicData['data'] is None:
            print('data is none')
            continue
        print("processing-->", trade)
        intra_df = pd.DataFrame(historicData['data'])
        intra_df = cleanData(intra_df)
        mother_candle = intra_df.iloc[0]
        child_candle = intra_df.iloc[1]

        #print( mother_candle["Low"],child_candle['Low'],  abs(mother_candle['High'] - mother_candle['Low']),CANDLE_PERCENT * mother_candle['Low'],row['trading_symbol'])
        if mother_candle["Low"] < child_candle['Low'] and mother_candle['High'] > child_candle['High'] and (
                abs(mother_candle['High'] - mother_candle['Low']) <= CANDLE_PERCENT * mother_candle['Low']):
            target_ratio = 1.5
            if mother_candle['High'] > 2000:
                target_ratio = 2.2
            buy_price = child_candle['High'] + 1
            sl = child_candle['Low']
            quantity = fetchQuantity(buy_price)
            target = buy_price + (buy_price - sl) * target_ratio

            buy_order = {
                'buy_price': buy_price,
                'target': target,
                "sl": sl,
                'quantity': quantity,
                'status': 'pending'
            }

            sell_price = child_candle['Low'] - 1
            sell_sl = child_candle['High']
            sell_target = child_candle['Low'] - \
                (sell_sl - child_candle['Low']) * target_ratio
            sell_order = {
                'sell_price': sell_price,
                'sl': sell_sl,
                'target': sell_target,
                'quantity': quantity,
                'status': 'pending'
            }

            print(row['trading_symbol'])
            cursor = trades.update_one({"symbol_token": str(row['symbol_token']), "date": today_date}, {
                "$set": {
                    "status": "order_selected",
                    "buy_order": buy_order,
                    "sell_order": sell_order
                }}, upsert=True)
except Exception as e:
    print("ERROR ->", e)
