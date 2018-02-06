import sys
import json
import requests
import time
import threading
import datetime
import traceback
import logging


from pymongo import MongoClient
from websocket import create_connection
from random import randint

THREADS = []
BULK_SIZE_LIMIT = 100

class MongoDB:
    def __init__(self):
        self.bulk_data = dict()
        self.client = MongoClient()
        self.crypto_db = self.client.crypto

    def add_data(self, conn_info, data):
        name = conn_info["name"]
        if(name not in self.bulk_data):
            self.bulk_data[name] = []

        data["timestamp"] = datetime.datetime.utcnow()
        self.bulk_data[name].append(data)
        if(len(self.bulk_data[name]) >= BULK_SIZE_LIMIT):
            mongo.bulk_insert(conn_info, self.bulk_data[name])

    def bulk_insert(self, conn_info, bulk_data):
        if(conn_info["channel"] == "book"):
            self.crypto_db.orders.insert_many(bulk_data, ordered=False)
        elif(conn_info["channel"] == "trades"):
            self.crypto_db.trades.insert_many(bulk_data, ordered=False)

        logging.debug("{} *** E: {} || C: {} || Added {} rows || {}".format(conn_info["channel"], conn_info["exchange"],
            conn_info["symbol"], len(bulk_data), datetime.datetime.utcnow()))

        del bulk_data[:]

class Websockets:

    def __init__(self):
        self.connections = dict()

    def add_connection(self, name, url):
        self.connections[name] = create_connection(url)
        return name

    def receive(self, wss_conn):
        return json.loads(wss_conn.recv())

    def subscribe(self, ws_conn, request):
        ws_conn.send(request)

class Bitfinex:
    """
    From API doc:
    We are splitting the public trade messages into two: a “te” message which mimics the current behavior,
    and a “tu” message which will be delayed by 1-2 seconds and include the tradeId.
    If the tradeId is important to you, use the “tu” message.
    If speed is important to you, listen to the “te” message. Or of course use both if you’d like.
    """

    api_v1 = "https://api.bitfinex.com/v1/"
    websockets_url = "wss://api.bitfinex.com/ws/2"

    def __init__(self):
        self.symbols = []

    def get_symbols(self):
        response = requests.request("GET", self.api_v1 + "symbols")
        self.symbols = json.loads(response.text)
        return self.symbols

    def create_connection(self, symbol, channel):
        conn_info = dict()
        conn_info["exchange"] = "Bitfinex"
        conn_info["symbol"] = symbol
        conn_info["channel"] = channel
        conn_info["name"] = "bfx_" + channel + "_" + symbol
        wss.add_connection(conn_info["name"], bfx.websockets_url)
        wss.subscribe(wss.connections[conn_info["name"]], bfx.subscribe(conn_info["channel"], conn_info["symbol"]))
        new_thread = Worker(conn_info)
        THREADS.append(new_thread)
        new_thread.start()
        return new_thread

    def subscribe(self, channel, symbol):
        return json.dumps({
            "event": "subscribe",
            "channel": channel,
            "symbol": symbol
        })

    def receive(self, conn_info, result):
        if(len(result) < 2): return
        if(conn_info["channel"] == "trades"):
            if(result[1] == "te"):
                mongo.add_data(conn_info, {"exchange": conn_info["exchange"], "symbol": conn_info["symbol"],"order_id": result[2][0], "tr_ts": result[2][1], "amount": result[2][2], "price": result[2][3]})
        elif(conn_info["channel"] == "book"):
            if(result[1] and "hb" not in result[1]):
                mongo.add_data(conn_info, {"exchange": conn_info["exchange"], "symbol": conn_info["symbol"], "price": result[1][0], "count": result[1][1], "amount": result[1][2]})

class Worker(threading.Thread):

    def __init__(self, conn_info):
        threading.Thread.__init__(self)
        self.kill_received = False
        self.conn_info = conn_info

    def run(self):
        while not self.kill_received:
            self.receive(self.conn_info)

    def receive(self, conn_info):
        result = wss.receive(wss.connections[conn_info["name"]])
        try:
            if(conn_info["exchange"] == "Bitfinex"):
                bfx.receive(conn_info, result)

        except Exception as e:
            logging.debug(traceback.format_exc())
            pass


mongo = MongoDB()
wss = Websockets()
bfx = Bitfinex()

def create_wss_connections(symbols=None):
    limit = 0
    bfx_symbols = bfx.get_symbols()
    if(not symbols):
        limit = 10
        symbols = bfx_symbols

    for idx, symbol in enumerate(symbols):
        if(limit):
            if(idx > limit):
                break
            time.sleep(5)
        else:
            time.sleep(10)
        try:
            if(symbol in bfx_symbols):
                thread = bfx.create_connection(symbol, "trades")
                thread = bfx.create_connection(symbol, "book")
                print("STARTED LISTENING {}".format(symbol))

            else:
                logging.debug("*** {} not in symbol list".format(symbol))
        except Exception as e:
            logging.debug("*** Broke connection, but continuing. {}".format(e))
            break
            pass


def main(args):
    global THREADS
    logging.basicConfig(filename="crypto.log", level=logging.DEBUG)
    create_wss_connections(args)

    while len(THREADS) > 0:
        try:
            THREADS = [t.join(1) for t in THREADS if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            print("Terminate received...")
            for t in THREADS:
                print("Killing thread {}".format(t))
                t.kill_received = True


if __name__ == "__main__":
    symbols = sys.argv[1:]
    main(symbols)
