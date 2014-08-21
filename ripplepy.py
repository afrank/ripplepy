__author__ = 'mtravis'

import urllib.parse
import urllib.request
import websocket
import enum
import sys
import time
import json
import ssl

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
    # log keys: connectionString, activity, remoteIP, connectTime, disconnectTime
    # activityStartTime, activityFinishTime, exception
    _log = dict()

    def __init__(self, connectionString, timeout=None, no_ssl_verify=False):
        self._connectionString = connectionString
        self._timeout = timeout
        self._no_ssl_verify = no_ssl_verify
        self._log = dict()
        self._log["connectionString"] = connectionString

        parsedUrl = urllib.parse.urlparse(self._connectionString)

        if parsedUrl.scheme == 'http' or parsedUrl.scheme == 'https':
            self._connectionType = Ripple.ConnectionType.rpc
        elif parsedUrl.scheme == 'ws' or parsedUrl.scheme == 'wss':
            self._connectionType = Ripple.ConnectionType.websocket

    def connectRippled(self):
        if self._connectionType == Ripple.ConnectionType.websocket:
            if self._isConnected:
                self.disconnectRippled()

            try:
                if self._no_ssl_verify:
                    self._ws = websocket.create_connection(self._connectionString, self._timeout,
                                                           sslopt={"cert_reqs" : ssl.CERT_NONE})
                else:
                    self._ws = websocket.create_connection(self._connectionString, self._timeout)

                self._isConnected = True
                self._log["remoteIP"] = self._ws.sock.getpeername()[0]
                self._log["connectTime"] = time.time()
                if "exception" in self._log:
                    del self._log["exception"]
            except Exception as e:
                if hasattr(e, 'remote_ip'):
                    self._log["remoteIP"] = e.remote_ip
                self._log["exception"] = sys.exc_info()
                self._log["disconnectTime"] = time.time()

            self._log["activity"] = "connect"

        return self._isConnected

    def getRemoteIP(self):
        return self._log["remoteIP"]

    def getIsConnected(self):
        return self._isConnected

    def disconnectRippled(self):
        if self._connectionType == Ripple.ConnectionType.websocket:
            try:
                self._ws.close()
                if "exception" in self._log:
                    del self._log["exception"]
            except:
                self._log["exception"] = sys.exc_info()

            self._isConnected = False
            self._log["activity"] = "disconnect"
            self._log["disconnectTime"] = time.time()

    def command(self, command, activity=None, params=None, id=None):
        input = { "method" : str(command) }
        if id is not None:
            input["id"] = id
        if params is not None:
            input["params"] = params

        if self._connectionType == Ripple.ConnectionType.rpc:
            try:
                self._log["connectTime"] = time.time()
                reply = urllib.request.urlopen(self._connectionString, json.dumps(input).encode(),timeout=self._timeout)
                output = json.loads(reply.read().decode())
                if "exception" in self._log:
                    del self._log["exception"]
            except:
                self._log["exception"] = sys.exc_info()
                output = None

            self._log["disconnectTime"] = time.time()
            self._log["activity"] = activity

        return output

    def cmd_server_info(self):
        return self.command("server_info", activity="server_info")
