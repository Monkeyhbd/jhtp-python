import socket
import json
import threading


BUFFER_SIZE = 4096


def _receive_fixed_length(sok, bs):
    total_data = b''
    while bs > 0:
        data = sok.recv(min(bs, BUFFER_SIZE))
        if data == b'':
            raise RuntimeError
        total_data += data
        bs -= len(data)
    return total_data


class TransportLayerAdapter:

    def __init__(self, sok):
        self.sok = sok

    def recv_bs(self, bs):
        """ Receive fixed bytes(bs) of data. """
        pass

    def send(self, data):
        pass

    def recv(self, bs):
        pass

    def fileno(self):
        return self.sok.fileno()


class TCPAdapter(TransportLayerAdapter):

    def __init__(self, sok=None):
        _sok = socket.socket(socket.AF_INET, socket.SOCK_STREAM) if sok is None else sok
        TransportLayerAdapter.__init__(self, _sok)

    def recv_bs(self, bs):
        return _receive_fixed_length(self.sok, bs)

    def send(self, data):
        return self.sok.send(data)

    def recv(self, bs):
        return self.sok.recv(bs)

    def listen(self):
        return self.sok.listen()

    def accept(self):
        conn, addr = self.sok.accept()
        return TCPAdapter(conn), addr

    def connect(self, addr):
        self.sok.connect(addr)

    def bind(self, addr):
        self.sok.bind(addr)


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
        self._send_lock = threading.Lock()

    def send(self, head=None, body=b''):
        payload_head = b''
        payload_body = body
        if type(head) is dict:
            payload_head = json.dumps(head).encode('utf-8')
        protocol_head_dict = {
            'version': '0.1',
            'head_length': len(payload_head),
            'body_length': len(payload_body)
        }
        protocol_head = json.dumps(protocol_head_dict).encode('utf-8')
        head_length = len(protocol_head)
        self._send_lock.acquire()
        self.tla.send(head_length.to_bytes(length=2, signed=False))
        self.tla.send(protocol_head)
        self.tla.send(payload_head)
        self.tla.send(payload_body)
        self._send_lock.release()

    def recv(self):
        head_length = int.from_bytes(self.tla.recv(2), signed=False)
        protocol_head = self.tla.recv_bs(head_length)
        protocol_head_dict = json.loads(protocol_head.decode('utf-8'))
        payload_head_length = protocol_head_dict['head_length']
        payload_body_length = protocol_head_dict['body_length']
        if payload_head_length == 0:
            payload_head_dict = None
        else:
            payload_head = self.tla.recv_bs(payload_head_length)
            payload_head_dict = json.loads(payload_head.decode('utf-8'))
        payload_body = self.tla.recv_bs(payload_body_length)
        return payload_head_dict, payload_body


class JHTPClient(JHTPPeer):

    def __init__(self, tla=None):
        JHTPPeer.__init__(self, TCPAdapter() if tla is None else tla)

    def connect(self, host, port):
        self.remote_addr = (host, port)
        self.tla.connect(self.remote_addr)
