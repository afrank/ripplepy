__author__ = 'mtravis'

import urllib.parse
import urllib.request
import websocket
import enum
import time
import socket

class Ripple:
    class ConnectionType(enum.Enum):
        none = 0
        rpc = 1
        websocket = 2

    _connectionType = ConnectionType.none
    _parsedUrl = None
    _ws = websocket.WebSocket
#    _rpc = urllib.request.urlopen
    _remoteIp = None
    _timeout = None

    def __init__(self, connectionString, timeout=None):
        self._timeout = timeout
        self._parsedUrl = urllib.parse.urlparse(connectionString)
        r = urllib.parse.urlparse(connectionString)
        p = list(r)
        self._remoteIp = socket.gethostbyname(r.hostname)
        p[1] = self._remoteIp
        if r.port is not None:
            p[1] += ':' + str(r.port)

        if self._parsedUrl.scheme == 'http' or self._parsedUrl.scheme == 'https':
            self._connectionType = Ripple.ConnectionType.rpc
        elif self._parsedUrl.scheme == 'ws' or self._parsedUrl.scheme == 'wss':
            self._connectionType = Ripple.ConnectionType.websocket

        self.connectRippled()

    def connectRippled(self):
        self._remoteIp = socket.gethostbyname(self._parsedUrl.hostname)
        r = list(self._parsedUrl)
        r[1] = self._remoteIp
        if self._parsedUrl.port is not None:
            r[1] += ':' + str(self._parsedUrl.port)

        if self._connectionType == Ripple.ConnectionType.websocket:
#            self._ws = websocket.create_connection(urllib.parse.urlunparse(r), self._timeout)
            self._ws = websocket.create_connection(urllib.parse.urlunparse(self._parsedUrl), self._timeout)


    def getRemoteIp(self):
        return self._remoteIp