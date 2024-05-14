import select
import queue
import threading


def _default_handler(_jhtp_peer, _head, _body):
    pass


def _nonblock_handler_factory(_block_handler):
    def nonblock_handler(jhtp, head, body):
        threading.Thread(target=_block_handler, args=(jhtp, head, body), daemon=True).start()
    return nonblock_handler


class JHTPPeerMultiplexer:

    def __init__(self):
        self._mainloop_continue = True
        self.poll = select.poll()
        self.fno2peer = {}
        self.fno2messages = {}
        self.handler = _default_handler

    def add(self, jhtp_peer):
        fno = jhtp_peer.fileno()
        self.fno2peer[fno] = jhtp_peer
        self.fno2messages[fno] = queue.Queue()
        self.poll.register(fno, select.POLLIN)

    def remove(self, jhtp_peer):
        fno = jhtp_peer.fileno()
        self.poll.unregister(fno)
        self.fno2peer.pop(fno)
        self.fno2messages.pop(fno)

    def set_on_recv(self, handler, block=False):
        if block:
            self.handler = handler
        else:
            self.handler = _nonblock_handler_factory(handler)

    def set_next_send(self, jhtp_peer, head, body, block=False):
        pass

    def mainloop(self):
        while self._mainloop_continue:
            for fno, event in self.poll.poll(50):
                jhtp_peer = self.fno2peer[fno]
                if event & select.POLLIN:
                    head, body = jhtp_peer.recv()
                    self.handler(jhtp_peer, head, body)
                if (not self.fno2messages[fno].empty()) and (event & select.POLLOUT):
                    pass
