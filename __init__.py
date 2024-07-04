import socket
import json
import _thread
import time

BUFFER_SIZE = 4096

_LOG_PREFIX = '[JHTP]'
_DEBUG_LEVEL = 0


def set_debug_level(level):
    global _DEBUG_LEVEL
    _DEBUG_LEVEL = level


def _log(text, level=1):
    if level <= _DEBUG_LEVEL:
        print(_LOG_PREFIX, end=' ')
        print(text)


def _receive_fixed_length(sok, bs):
    total_data = b''
    while bs > 0:
        data = sok.recv(min(bs, BUFFER_SIZE))
        total_data += data
        bs -= len(data)
        if data == b'':
            break
    return total_data


class TlaException(Exception):

    def __init__(self):
        pass


class TlaConnectionClose(TlaException):

    def __init__(self, addr=None):
        TlaException.__init__(self)
        _log('TLA connection closed by peer {}.'.format(addr))


class TlaConnectionRefuse(TlaException):

    def __init__(self):
        TlaException.__init__(self)
        _log('TLA connection refused by remote.')


class TransportLayerAdapter:

    def __init__(self, sok, addr=None):
        self.sok = sok
        self.addr = addr  # remote addr

    def recv_bs(self, bs):
        """ Receive fixed bytes(bs) of data. """
        pass

    def send(self, data):
        pass

    def recv(self, bs):
        pass

    def fileno(self):
        return self.sok.fileno()

    def close(self):
        return self.sok.close()


class TCPAdapter(TransportLayerAdapter):

    def __init__(self, sok=None, addr=None):
        _sok = socket.socket(socket.AF_INET, socket.SOCK_STREAM) if sok is None else sok
        TransportLayerAdapter.__init__(self, _sok, addr)

    def recv_bs(self, bs):
        try:
            data = _receive_fixed_length(self.sok, bs)
            if data == b'':
                raise TlaConnectionClose(self.addr)
            return data
        except ConnectionResetError:
            raise TlaConnectionClose(self.addr)

    def send(self, data):
        try:
            rtn = self.sok.send(data)
            return rtn
        except BrokenPipeError:
            raise TlaConnectionClose(self.addr)

    recv = recv_bs

    def listen(self):
        _log('TLA listening incoming connection...')
        self.sok.listen()

    def accept(self):
        conn, addr = self.sok.accept()
        _log('TLA accept connection {}'.format(addr))
        return TCPAdapter(conn, addr), addr

    def connect(self, addr):
        self.addr = addr
        try:
            _log('TLA connection establishing...')
            self.sok.connect(addr)
            _log('TLA connection established.')
        except ConnectionRefusedError:
            raise TlaConnectionRefuse

    def bind(self, addr):
        return self.sok.bind(addr)

    def reconnect(self, addr=None, timeout=3):
        _log('TLA try to reconnect...')
        self.sok = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addr = self.addr if addr is None else addr
        while True:
            try:
                self.connect(self.addr)
                break
            except TlaConnectionRefuse:
                time.sleep(timeout)
                continue


class JHTPBase:

    def __init__(self, tla=None):
        self.tla: TCPAdapter = tla
        self.local_addr = None
        self.remote_addr = None

    def bind(self, host, port):
        self.local_addr = (host, port)
        self.tla.bind(self.local_addr)

    def fileno(self):
        return self.tla.fileno()

    def close(self):
        return self.tla.close()


class JHTPServer(JHTPBase):

    def __init__(self, tla=None):
        JHTPBase.__init__(self, TCPAdapter() if tla is None else tla)
        self._mainloop_continue = True

    def on_accept(self, jhtp_peer):
        pass

    def mainloop(self):
        self.tla.listen()
        while self._mainloop_continue:
            peer_tla, addr = self.tla.accept()
            jhtp_peer = JHTPPeer(peer_tla)
            jhtp_peer.local_addr = self.local_addr
            jhtp_peer.remote_addr = addr
            self.on_accept(jhtp_peer)


class JHTPPeer(JHTPBase):

    def __init__(self, tla=None):
        JHTPBase.__init__(self, tla)
        self._send_lock = _thread.allocate_lock()

    def send(self, head=None, body=b''):
        _log('Send start.')
        payload_head = b''
        payload_body = body
        if type(head) is dict:
            payload_head = json.dumps(head).encode('utf-8')
        # _log('Payload head is {}'.format(head))
        protocol_head_dict = {
            'version': '0.1',
            'head_length': len(payload_head),
            'body_length': len(payload_body)
        }
        protocol_head = json.dumps(protocol_head_dict).encode('utf-8')
        _log('Send head {}'.format(protocol_head_dict))
        head_length = len(protocol_head)
        _log('Head length is {}'.format(head_length), 2)
        self._send_lock.acquire()
        _log('Sending head length...', 2)
        self.tla.send(head_length.to_bytes(length=2, signed=False, byteorder='little'))
        _log('Sending protocol head...', 2)
        self.tla.send(protocol_head)
        _log('Sending payload head...', 2)
        self.tla.send(payload_head)
        _log('Sending payload body...', 2)
        self.tla.send(payload_body)
        self._send_lock.release()
        _log('Send complete.')

    def recv(self):
        _log('Receive start.')
        _log('Receiving head length...', 2)
        head_length = int.from_bytes(self.tla.recv(2), signed=False, byteorder='little')
        _log('Head length is {}'.format(head_length), 2)
        _log('Receiving protocol head...', 2)
        protocol_head = self.tla.recv_bs(head_length)
        protocol_head_dict = json.loads(protocol_head.decode('utf-8'))
        _log('Receive head {}'.format(protocol_head_dict))
        payload_head_length = protocol_head_dict['head_length']
        payload_body_length = protocol_head_dict['body_length']
        _log('Receiving payload head...', 2)
        if payload_head_length == 0:
            payload_head_dict = None
        else:
            payload_head = self.tla.recv_bs(payload_head_length)
            payload_head_dict = json.loads(payload_head.decode('utf-8'))
        # _log('Payload head is {}'.format(payload_head_dict))
        _log('Receiving payload body...', 2)
        payload_body = self.tla.recv_bs(payload_body_length)
        _log('Receive complete.')
        return payload_head_dict, payload_body


class JHTPClient(JHTPPeer):

    def __init__(self, tla=None):
        JHTPPeer.__init__(self, TCPAdapter() if tla is None else tla)

    def connect(self, host, port):
        self.remote_addr = (host, port)
        self.tla.connect(self.remote_addr)

    def reconnect(self, **kv):
        self.tla.reconnect(**kv)
