from pymongo import MongoClient
import matplotlib.pyplot as plt
from datetime import timedelta

def get_trades(db):
    collection = db["trades"]

    buyers = []
    sellers = []

    for obj in collection.find():
        if obj["symbol"] != "btcusd":
            continue

        if obj["amount"] > 0:
            buyers.append([obj["tr_ts"], obj["price"]])
        else:
            sellers.append([obj["tr_ts"], obj["price"]])

    ts_buy = [x[0] for x in buyers]
    pr_buy = [x[1] for x in buyers]
    ts_sell = [x[0] for x in sellers]
    pr_sell = [x[1] for x in sellers]
    return ts_buy, pr_buy, ts_sell, pr_sell


def get_orders(db):
    collection = db["orders"]

    buyers = []
    sellers = []

    buy_count = []
    sell_count = []
    bids = 0
    asks = 0

    for obj in collection.find():
        if obj["symbol"] != "btcusd":
            continue
        if obj["count"] == 0:
            continue
        if obj["amount"] > 0:
            buyers.append([obj["timestamp"], obj["price"]])
            buy_count.append([obj["timestamp"], bids])
        else:
            sellers.append([obj["timestamp"] , obj["price"]])
            sell_count.append([obj["timestamp"], asks])
        bids = 0
        asks = 0
    ts_buy = [x[0] for x in buyers]
    pr_buy = [x[1] for x in buyers]
    ts_sell = [x[0] for x in sellers]
    pr_sell = [x[1] for x in sellers]

    ts_buy_count = [x[0] for x in buy_count]
    pr_buy_count = [x[1] for x in buy_count]
    ts_sell_count = [x[0] for x in sell_count]
    pr_sell_count = [x[1] for x in sell_count]

    return ts_buy, pr_buy, ts_sell, pr_sell, ts_buy_count, pr_buy_count, ts_sell_count, pr_sell_count

if __name__ == "__main__":
    client = MongoClient("localhost", 27017, maxPoolSize=50)
    db = client.crypto

    ts_buy, pr_buy, ts_sell, pr_sell, ts_buy_count, pr_buy_count, ts_sell_count, pr_sell_count = get_orders(db)
    ts_buy_trade, pr_buy_trade, ts_sell_trade, pr_sell_trade = get_trades(db)

    fig, ax = plt.subplots()
    buy = ax.plot(ts_buy, pr_buy, color='green', markersize=8, label='Bid')
    sell = ax.plot(ts_sell, pr_sell, color='red', markersize=8, label='Ask')

    trade_buy = ax.plot(ts_buy_trade, pr_buy_trade, color='blue', markersize=8, label='Trade Buy')
    trade_sell = ax.plot(ts_sell_trade, pr_sell_trade, color='purple', markersize=8, label='Trade Sell')

    ax2 = ax.twinx()
    ax2.plot(ts_buy_count, pr_buy_count, color='pink', markersize=8, label='# Bids')
    ax2.plot(ts_sell_count, pr_sell_count, color='orange', markersize=8, label='# Asks')


    legend = ax.legend(loc='upper center', shadow=True)

    plt.show()
