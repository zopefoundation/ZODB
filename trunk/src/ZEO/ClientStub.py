"""Stub for interface exported by ClientStorage"""

class ClientStorage:
    def __init__(self, rpc):
        self.rpc = rpc
        
    def beginVerify(self):
        self.rpc.callAsync('begin')

    # XXX what's the difference between these two?

    def invalidate(self, args):
        self.rpc.callAsync('invalidate', args)

    def Invalidate(self, args):
        self.rpc.callAsync('Invalidate', args)

    def endVerify(self):
        self.rpc.callAsync('end')

    def serialno(self, arg):
        self.rpc.callAsync('serialno', arg)

    def info(self, arg):
        self.rpc.callAsync('info', arg)
