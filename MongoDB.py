import logging
import datetime
from pymongo import MongoClient
from settings import BULK_SIZE_LIMIT

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
            self.bulk_insert(conn_info, self.bulk_data[name])

    def bulk_insert(self, conn_info, bulk_data):
        if(conn_info["channel"] == "book"):
            self.crypto_db.orders.insert_many(bulk_data, ordered=False)
        elif(conn_info["channel"] == "trades"):
            self.crypto_db.trades.insert_many(bulk_data, ordered=False)

        logging.debug("{} *** E: {} || C: {} || Added {} rows || {}".format(conn_info["channel"], conn_info["exchange"],
            conn_info["symbol"], len(bulk_data), datetime.datetime.utcnow()))
        del bulk_data[:]
