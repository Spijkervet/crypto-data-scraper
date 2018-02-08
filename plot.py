from pymongo import MongoClient
import matplotlib.pyplot as plt

from datetime import timedelta, datetime

def moving_average(db):
    collection = db["trades"]
    buyers = []
    start_idx = 0
    frame = 50
    avg_list = []
    length = collection.find({"symbol": "btcusd"}).count()
    for i in range(length):
        new_frame = frame+start_idx
        coll = collection.find()[start_idx:new_frame]
        average = 0
        idx = 1
        if coll.count() <= frame:
            break
        for obj in coll:
            if obj["price"] > 5000 and obj["amount"] > 0:
                idx += 1
                average += obj["price"]
                ts = obj["timestamp"]
                # print("ID: {}, COLL: {}".format(i, obj["price"]))
        # print(average, average / (idx - 1))
        # print()
        if(average > 7500):
            print(average, idx)
            avg_list.append([average / (idx), datetime.fromtimestamp(ts)])
            start_idx += 1
    print(avg_list)
    return avg_list

def get_cumulative_orders(db):
    collection = db["orders"]
    buyers = []
    sellers = []
    for obj in collection.find():
        # print(obj["price"], obj["amount"])
        if(obj["price"] > 5000 and obj["amount"] > 0):
            buyers.append([round(obj["price"], 1), obj["amount"]])
            # print(collection.find({"price": obj["price"]}).count())

        if obj["price"] > 5000 and obj["amount"] < 0:
            sellers.append([round(obj["price"], 1), obj["amount"]])
    price_buy = [x[0] for x in buyers]
    amount_buy = [x[1] for x in buyers]
    price_sell = [x[0] for x in sellers]
    amount_sell = [x[1] for x in sellers]
    return price_buy, amount_buy, price_sell, amount_sell

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


    # price_buy, amount_buy, price_sell, amount_sell = get_cumulative_orders(db)
    # ts_buy, pr_buy, ts_sell, pr_sell, ts_buy_count, pr_buy_count, ts_sell_count, pr_sell_count = get_orders(db)
    # ts_buy_trade, pr_buy_trade, ts_sell_trade, pr_sell_trade = get_trades(db)

    fig, ax = plt.subplots()
    # buy = ax.bar(price_buy, amount_buy, color='green', label='Bid')
    # sell = ax.bar(price_sell, amount_sell, color='red', label='Ask')

    # ax2 = ax.twinx()
    # trade_buy = ax.plot(ts_buy_trade, pr_buy_trade, color='blue', markersize=8, label='Trade Buy')
    # trade_sell = ax.plot(ts_sell_trade, pr_sell_trade, color='purple', markersize=8, label='Trade Sell')


    # ax2.plot(ts_buy_count, pr_buy_count, color='pink', markersize=8, label='# Bids')
    # ax2.plot(ts_sell_count, pr_sell_count, color='orange', markersize=8, label='# Asks')
    moving_avg = moving_average(db)
    x = [x[1] for x in moving_avg]
    y = [x[0] for x in moving_avg]
    ax.plot(x, y, color='purple', markersize=8, label='Trade Sell')

    legend = ax.legend(loc='upper center', shadow=True)

    plt.show()
