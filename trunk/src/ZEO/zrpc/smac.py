"""Sized message async connections
"""

import asyncore, string, struct

class smac(asyncore.dispatcher):

    def __init__(self, sock, addr):
        asyncore.dispatcher.__init__(self, sock)
        self.addr=addr
        self.__state=None
        self.__inp=None
        self.__l=4
        self.__output=output=[]
        self.__append=output.append
        self.__pop=output.pop

    def handle_read(self,
                    join=string.join, StringType=type('')):
        l=self.__l
        d=self.recv(l)
        inp=self.__inp
        if inp is None:
            inp=d
        elif type(inp) is StringType:
            inp=[inp,d]
        else:
            inp.append(d)

        l=l-len(d)
        if l <= 0:
            if type(inp) is not StringType: inp=join(inp,'')
            if self.__state is None:
                # waiting for message
                self.__l=struct.unpack(">i",inp)[0]
                self.__state=1
                self.__inp=None
            else:
                self.__inp=None
                self.__l=4
                self.__state=None
                self.message_input(inp)
        else:
            self.__l=l
            self.__inp=inp

    def readable(self): return 1
    def writable(self): return not not self.__output

    def handle_write(self):
        output=self.__output
        if output:
            v=output[0]
            n=self.send(v)
            if n < len(v):
                output[0]=v[n:]
            else:
                del output[0]

    def handle_close(self): self.close()

    def message_output(self, message,
                       pack=struct.pack, len=len):
        self.__append(pack(">i",len(message))+message)
