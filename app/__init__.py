import sys
import json

import time

import datetime
import traceback
import logging

from .Bitfinex import Bitfinex
from .settings import THREADS

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
                logging.debug("STARTED LISTENING {}".format(symbol))

            else:
                logging.debug("*** {} not in symbol list".format(symbol))
        except Exception as e:
            logging.debug("*** Broke connection, but continuing. {}".format(traceback.format_exc()))
            break
            pass


def main(args):
    global THREADS
    logging.basicConfig(filename="crypto.log", level=logging.DEBUG)
    std_err = logging.StreamHandler()
    std_err.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    logging.getLogger().addHandler(std_err)

    create_wss_connections(args)

    while len(THREADS) > 0:
        try:
            THREADS = [t.join(1) for t in THREADS if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            print("Terminate received...")
            for t in THREADS:
                print("Killing thread {}".format(t))
                t.kill_received = True
