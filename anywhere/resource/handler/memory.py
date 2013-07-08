import sys
from types import ModuleType
from datetime import datetime
import os
from urlparse import urlparse
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


from .base import AbstractFileResource, AbstractDirectoryResource
from .base import scheme_to_resource

from anywhere.resource.types import RegisterDict


# set up the database as a global module
DBNAME = 'anywhere.handlers.memory.db'
if not DBNAME in sys.modules:
    mod = ModuleType(DBNAME)
    mod.schemes = {}
    sys.modules[DBNAME] = mod

__db__ = sys.modules[DBNAME]

def set_path(scheme, location, path, obj, create=False):
    return _process_path(scheme, location, path, set_=obj, create=create)


def delete_path(scheme, location, path):
    return _process_path(scheme, location, path, delete=True)


def find_by_path(scheme, location, path):
    return _process_path(scheme, location, path)


def _process_path(scheme, location, path, set_=None, delete=False, create=False):
    if scheme not in __db__.schemes:
        return None
    if location not in __db__.schemes[scheme]:
        return None
    if create and set_ is None:
        # create None makes an empty file
        set_ = ''
    # split the path and remove empty chunks
    chunks = path.split('/')
    chunks = [item for item in chunks if item]
    basechunks = []
    root = __db__.schemes[scheme][location]
    obj = root
    while chunks:
        chunk = chunks.pop(0)
        basechunks.append(chunk)
        if chunk not in root:
            if create and not chunks:
                continue
            elif create and chunks:
                root[chunk] = {}
            else:
                return None
        obj = root[chunk]
        if isinstance(obj, dict):
            root = obj
            continue
        if chunks and isinstance(obj, basestring):
            # trying to reference a file as a directory
            raise ValueError('on `{}` : `{}` does not exist : `/{}` is a file.'.format(
                location, path, '/'.join(basechunks)))
    if set_ is not None:
        root[chunk] = set_
        return set_
    elif delete:
        del root[chunk]
        return True
    return obj


def Resource(path, location='', scheme='mem'):
    # create the scheme and the location
    sch = __db__.schemes.setdefault(scheme, dict())
    sch.setdefault(location, dict())
    # get the actual object or create it.
    obj = find_by_path(scheme, location, path)
    if isinstance(obj, dict) or obj is None and path.endswith('/'):
        return MemDirectoryResource(path, location, scheme=scheme, db_object=obj)
    if obj is not None and path.endswith(os.path.sep):
            raise ValueError('on `{}` : `{}` does not exist : `{}` is a file.'.format(
                location, path, path.rstrip('/')))
    return MemFileResource(path, location, scheme=scheme, db_object=obj)


class MemResource(object):
    type = 'mem'

    def __init__(self, path, location='', scheme='mem', db_object=None):
        self._path = path
        self._location = location
        self.scheme = scheme
        self.db_object = db_object

    @property
    def path(self):
       return self._path

    @property
    def location(self):
       return self._location

    @property
    def url(self):
        return "{}://{}/{}".format(self.scheme,
                                  self._location and '{}/'.format(
                                      self._location) or '',
                                  self._path)

    @property
    def exists(self):
        return self.db_object is not None

    @property
    def size(self):
        if not self.exists:
            return 0
        else:
            return len(bytes(self.db_object))

    @property
    def ctime(self):
        return datetime.now()

    @property
    def mtime(self):
        return datetime.now()

    @property
    def atime(self):
        return datetime.now()

    def delete(self):
        'remove the resource from its location'
        if delete_path(self.scheme, self.location, self.path) is not None:
            self.db_object = None
            return True
        else:
            return False

    def copy(self, dest, overwrite=False):
        'copy the resource to another resource'
        raise NotImplementedError()

    def move(self, dest, overwrite=False):
        'equivalent to self.copy(dest) then self.delete()'
        raise NotImplementedError()

    def __str__(self):
        return self.url

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.url)


class MemFileResource(MemResource, AbstractFileResource):

    @property
    def _write_stream(self):
        if hasattr(self, "_append_stream_"):
            raise IOError("can't write until append operation is flush")
        if not hasattr(self, "_write_stream_"):
            self._write_stream_ = StringIO()
        return self._write_stream_

    @property
    def _append_stream(self):
        if hasattr(self, "_write_stream_"):
            raise IOError("can't append until write operation is flush")
        if not hasattr(self, "_append_stream_"):
            self._append_stream_ = StringIO()
            self._append_stream_.write(self.db_object)
        return self._append_stream_

    @property
    def _read_stream(self):
        if hasattr(self, "_write_stream_"):
            raise IOError("can't read until write operation is flush")
        if hasattr(self, "_append_stream_"):
            raise IOError("can't read until append operation is flush")
        if not hasattr(self, "_read_stream_"):
            self._read_stream_ = StringIO(self.db_object)
        return self._read_stream_

    def flush(self):
        for stream_name in ['_write_stream_', '_append_stream_']:
            if hasattr(self, stream_name):
                self.db_object = getattr(self, stream_name).getvalue()
                set_path(self.scheme, self.location, self.path,
                         self.db_object)
                delattr(self, stream_name)

    def reset(self):
        "abort not flushed writes"
        for stream_name in ['_write_stream_', '_append_stream_']:
            try:
                delattr(self, stream_name)
            except AttributeError:
                pass

    def empty(self, flush=True):
        'empty the file'
        self.reset()
        self.write('')
        if flush:
            self.flush()

    def create(self):
        if self.exists:
            return False
        set_path(self.scheme, self.location, self.path, '', create=True)
        self.db_object = ''
        return True

    def read(self, size=-1):
        'read at most `size` bytes, returned as a string'
        stream = self._read_stream
        return stream.read(size)

    def write(self, string):
        'write `string` to the resource'
        stream = self._write_stream
        stream.write(string)

    def append(self, string):
        'append `string` to the resource'
        stream = self._append_stream
        stream.write(string)


    def __iter__(self):
        return iter(self._read_stream)


class MemDirectoryResource(MemResource, AbstractDirectoryResource):

    def flush():
        'does nothing'

    def list(self):
        if not self.exists:
            return []
        else:
            # XXX: py3k
            return self.db_object.keys()

    def create(self):
        if self.exists:
            return False
        self.db_object = {}
        set_path(self.scheme, self.location, self.path, self.db_object, create=True)
        return True

scheme_to_resource.register('mem', Resource)
