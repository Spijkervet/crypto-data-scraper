import requests
import json

from ..Worker import Worker
from ..settings import THREADS


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
        conn_info["url"] = self.websockets_url
        conn_info["subscription"] = self.subscribe(conn_info["channel"], conn_info["symbol"])
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
