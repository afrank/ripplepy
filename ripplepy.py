__author__ = 'mtravis'

import urllib.parse
import urllib.request
import websocket
import enum
import sys
import time

class RippleLog:
    def __init__(self, connectionString=str(), activity=str(), remoteIP=str(), connectTime=0.0, disconnectTime=0.0,
             activityTime=0.0, exception=list()):
        self._connectionString = connectionString
        self._activity = activity
        self._remoteIP = remoteIP
        self._connectTime = connectTime
        self._disconnectTime = disconnectTime
        self._activityTime = activityTime
        self._exception = list(exception)

class Ripple:
    class ConnectionType(enum.Enum):
        none = 0
        rpc = 1
        websocket = 2

    _connectionType = ConnectionType.none
    _connectionString = str()
    _timeout = 0
    _no_ssl_verify = False
    _parsedUrl = None
    _ws = websocket.WebSocket
#    _rpc = urllib.request.urlopen
    _timeout = None
    _isConnected = False
    _log = None

    def __init__(self, connectionString, timeout=None, no_ssl_verify=False):
        self._connectionString = connectionString
        self._timeout = timeout
        self._no_ssl_verify = no_ssl_verify

        parsedUrl = urllib.parse.urlparse(self._connectionString)

        if parsedUrl.scheme == 'http' or parsedUrl.scheme == 'https':
            self._connectionType = Ripple.ConnectionType.rpc
        elif parsedUrl.scheme == 'ws' or parsedUrl.scheme == 'wss':
            self._connectionType = Ripple.ConnectionType.websocket

        self._log = RippleLog(self._connectionString)

    def connectRippled(self):
        if self._connectionType == Ripple.ConnectionType.websocket:
            if self._isConnected:
                self.disconnectRippled()

            remote_ip = dict()

            try:
                self._ws = websocket.create_connection(self._connectionString, self._timeout, remote_ip=remote_ip,
                                                       no_ssl_verify=self._no_ssl_verify)
                self._isConnected = True
            except:
                self._log._exception = list(sys.exc_info())
            finally:
                self._log._activity = "connect"
                self._log._remoteIP = remote_ip["remote_ip"]
                self._log._connectTime = time.time()

        return self._isConnected

    def getRemoteIP(self):
        return self._log._remoteIP

    def getIsConnected(self):
        return self._isConnected

    def disconnectRippled(self):
        if self._connectionType == Ripple.ConnectionType.websocket:
            try:
                self._ws.close()
            except:
                self._log._exception = list(sys.exc_info())

            self._isConnected = False
            self._log._activity = "disconnect"
            self._log._disconnectTime = time.time()
