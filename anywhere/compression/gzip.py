import zlib
from cStringIO import StringIO
from anywhere.utils.io import OHelper

READ_BUFFER_SIZE = 65536


class ReadStream(OHelper):
    def __init__(self, stream):
        self.stream = stream
        self.buf = StringIO()
        self.decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
       # self.decompressor = zlib.decompressobj()

    def read(self, size=-1):
        if size >= 0:
            # compression ratio is expected between 2 and 5
            chunk = max(size * 3, READ_BUFFER_SIZE)
        else:
            return self.decompressor.decompress(self.stream.read())
        while 1:
            if self.buf.tell() >= size:
                return self.flush_stream(size)
            data = self.stream.read(chunk)
            if data == '':
                return self.flush_stream(size)
            data = self.decompressor.decompress(data)
            self.buf.write(data)

    def flush_stream(self, pos):
        data = self.buf.getvalue()
        pos = pos < len(data) and pos or len(data)
        self.buf.seek(0)
        self.buf.truncate()
        self.buf.write(data[pos:])
        return data[:pos]


def open(stream, method='r'):
    if method == 'r':
        return ReadStream(stream)

