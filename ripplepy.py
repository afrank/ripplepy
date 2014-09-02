__author__ = 'mtravis'

import urllib.parse
import urllib.request
import websocket
import enum
import sys
import time
import json
import ssl
import http.client
import socket
import string
import binascii


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
    _rpc = http.client
    _isConnected = False
    # log keys: connectionString, activity, remoteIP, connectTime,
    # disconnectTime
    # activityStartTime, activityFinishTime, exception
    _log = dict()

    def __init__(self, connection_string, timeout=None, no_ssl_verify=False):
        self._connectionString = connection_string
        self._timeout = timeout
        self._no_ssl_verify = no_ssl_verify
        self._log = dict()
        self._log["connectionString"] = connection_string

        self._parsedUrl = urllib.parse.urlparse(self._connectionString)

        if self._parsedUrl.scheme == 'http'\
                or self._parsedUrl.scheme == 'https':
            self._connectionType = Ripple.ConnectionType.rpc
            socket.setdefaulttimeout(self._timeout)
        elif self._parsedUrl.scheme == 'ws' or self._parsedUrl.scheme == 'wss':
            self._connectionType = Ripple.ConnectionType.websocket
        else:
            raise Exception("Bad URL Scheme", self._parsedUrl.scheme)

    def connect(self):
        if self._isConnected:
            self.disconnect()

        # http.client does not actually connect the socket until a request is
        # sent
        if self._connectionType == Ripple.ConnectionType.rpc:
            if self._parsedUrl.scheme == "https":
                context = ssl.create_default_context()
                self._rpc = http.client.HTTPSConnection(self._parsedUrl.netloc,
                                                        timeout=self._timeout,
                                                        context=context)
            elif self._parsedUrl.scheme == "http":
                self._rpc = http.client.HTTPConnection(self._parsedUrl.netloc,
                                                       timeout=self._timeout)

            self._isConnected = True

        elif self._connectionType == Ripple.ConnectionType.websocket:
            try:
                if self._no_ssl_verify:
                    self._ws = websocket.create_connection(
                        self._connectionString, self._timeout,
                        sslopt={"cert_reqs": ssl.CERT_NONE})
                else:
                    self._ws = websocket.create_connection(
                        self._connectionString, self._timeout)

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

    def get_remote_ip(self):
        return self._log["remoteIP"]

    def get_is_connected(self):
        return self._isConnected

    def disconnect(self):
        if self._connectionType == Ripple.ConnectionType.rpc:
            try:
                self._rpc.close()
            except:
                pass
        elif self._connectionType == Ripple.ConnectionType.websocket:
            try:
                self._ws.close()
            except:
                pass

        self._isConnected = False
        self._log["activity"] = "disconnect"
        self._log["disconnectTime"] = time.time()

    def command(self, command, activity=None, params=None, id_arg=None):
        indata = {"method": str(command)}
        if id_arg is not None:
            indata["id"] = id_arg
        if params is not None:
            indata["params"] = params

        output = None
        if self._connectionType == Ripple.ConnectionType.rpc:
            try:
                self._log["connectTime"] = time.time()
                self._rpc.request("GET", "/", json.dumps(input).encode())
                reply = self._rpc.getresponse()
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

    def cmd_ledger(self, ledger, full=False, accounts=False, transactions=False,
                   expand=False):
        params = list()
        params.append({"full": full, "accounts": accounts,
                       "transactions": transactions, "expand": expand})
        params[0]["ledger"] = ledger

        return self.command("ledger", activity="ledger," + str(ledger),
                            params=params)


class Uint256:
    def __init__(self, arg=""):
        if len(arg) == 32:
            self._data = bytes(arg)
        elif len(arg) == 64 and all(c in string.hexdigits for c in arg):
            self._data = bytes(binascii.unhexlify(arg))
        elif len(arg) == 0:
            data = bytearray()
            for i in range(0, 32):
                data += b"\x00"
            self._data = bytes(data)
        else:
            raise ValueError('not valid uint256 input')

    def data(self):
        return self._data

    def hexstr(self):
        hstr = str()
        for i in self._data:
            hstr += ("%x" % i).upper()

        return hstr

    def is_zero(self):
        return all(b in b'\x00' for b in self._data)
