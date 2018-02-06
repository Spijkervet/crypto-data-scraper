import json

from websocket import create_connection

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

    def close(self, wss_conn):
        wss_conn.close()
