import threading
import logging
import traceback

from .Websockets import Websockets
from .MongoDB import MongoDB

mongo = MongoDB()
wss = Websockets()


class Worker(threading.Thread):

    def __init__(self, conn_info):
        threading.Thread.__init__(self)
        self.kill_received = False
        self.conn_info = conn_info
        wss.add_connection(conn_info["name"], conn_info["url"])
        wss.subscribe(wss.connections[conn_info["name"]], conn_info["subscription"])

    def run(self):
        while not self.kill_received:
            self.receive(self.conn_info)
        wss.close(wss.connections[self.conn_info["name"]])

    def receive(self, conn_info):
        result = wss.receive(wss.connections[conn_info["name"]])
        try:
            if(conn_info["exchange"] == "Bitfinex"):
                self.process(conn_info, result)

        except Exception as e:
            logging.debug(traceback.format_exc())
            pass

    def process(self, conn_info, result):
        if(type(result) is dict): return
        if(conn_info["channel"] == "trades"):
            if(result[1] == "te"):
                mongo.add_data(conn_info, {"exchange": conn_info["exchange"], "symbol": conn_info["symbol"],"order_id": result[2][0], "tr_ts": result[2][1], "amount": result[2][2], "price": result[2][3]})
        elif(conn_info["channel"] == "book"):
            if(result[1] and "hb" not in result[1] and len(result) < 3):
                mongo.add_data(conn_info, {"exchange": conn_info["exchange"], "symbol": conn_info["symbol"], "price": result[1][0], "count": result[1][1], "amount": result[1][2]})
