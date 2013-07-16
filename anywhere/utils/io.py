from cStringIO import StringIO

class OHelper(object):
    """provide some useful methods for objects that expose read()"""
    def __iter__(self):
        buf = StringIO()
        next_char = self.read(1)
        while next_char:
            buf.write(next_char)
            if next_char == '\n':
                line = buf.getvalue()
                buf.seek(0)
                buf.truncate()
                yield line
            next_char = self.read(1)
        last_line = buf.getvalue()
        if last_line:
            yield last_line

    def readlines(self):
       return list(self)
