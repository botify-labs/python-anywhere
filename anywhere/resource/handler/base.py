import os
from urlparse import urlparse
from functools import wraps

from anywhere.resource.types import RegisterDict


all = ['Resource']


scheme_to_resource = RegisterDict()


def must_exist(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self.exists:
            raise IOError("`{}` Resource is not bound to an existing file".format(self))
        return fn(self, *args, **kwargs)
    return wrapper


def ensure_exists(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self.exists:
            self.create()
        return fn(self, *args, **kwargs)
    return wrapper


class UnknownScheme(Exception):
    """Anywhere cannot handle the given scheme"""
    def __init__(self, msg='', url=None):
        if not msg and url:
            msg = '`{}`: unknown url scheme'.format(url)
        super(UnknownScheme, self).__init__(msg)


def Resource(url):
    if not "://" in url:
        # must be a local file
        url = 'file://{}'.format(os.path.abspath(url))
    result = urlparse(url)
    scheme = result.scheme
    if not scheme in scheme_to_resource:
        raise UnknownScheme(url=url)
    return scheme_to_resource[scheme](path=result.path,
                                      location=result.netloc)


class AbstractResource(object):
    type = 'abstract'

    def __init__(self, url):
        self.url = url

    @property
    def path(self):
        'Path to the resource in its location'
        return self.url.split('://', 1)[1:]

    @property
    def location(self):
        """Network location of the resource"""
        raise NotImplementedError()

    @property
    def name(self):
        'basename of the resource'
        return os.path.basename(self.path)

    @property
    def exists(self):
        """True if bound to an existing resource"""
        raise NotImplementedError()

    @property
    def size(self):
        raise NotImplementedError()

    @property
    def ctime(self):
        raise NotImplementedError()

    @property
    def mtime(self):
        raise NotImplementedError()

    @property
    def atime(self):
        raise NotImplemented

    def get(self, path=None, overwrite=False):
        'copy the resource to local `path`'
        raise NotImplementedError()

    @ensure_exists
    def put(self, path, overwrite=False):
        'put local `path` file to the resource'
        raise NotImplementedError()

    def create(self):
        'create the resource on its location if it doesn\'t exist'
        raise NotImplementedError()

    def delete(self):
        'remove the resource from its location'
        raise NotImplementedError()

    def copy(self, dest, overwrite=False):
        'copy the resource to another resource'
        raise NotImplementedError()

    def move(self, dest, overwrite=False):
        'equivalent to self.copy(dest) then self.delete()'
        raise NotImplementedError()

    def flush(self, dest):
        'sync the changes on the physical device, if any'
        raise NotImplementedError()

    def __str__(self):
        return self.url

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.url)

    def __equals__(self, other):
        return (self.__class__ is other.__class__ and
                self.url == other.url)


class AbstractFileResource(AbstractResource):
    def empty(self):
        'empty the file'
        raise NotImplementedError()

    def read(self, size=None):
        'read at most `size` bytes, returned as a string'
        raise NotImplementedError()

    def write(self, string):
        'write `string` to the resource'
        raise NotImplementedError()

    @must_exist
    def get(self, path=None, overwrite=False):
        'copy the resource to local `path`'
        if not overwrite and os.path.exists(path):
            raise IOError('`{}` exists. Aborting'.format(path))
        with open(path, 'w') as f:
            while True:
                chunk = self.read(65535)
                if not chunk:
                    break
                f.write(chunk)

    def put(self, path, overwrite=False):
        if self.exists and not overwrite:
            raise IOError('{} exists. Aborting'.format(self))
        if not self.exists:
            self.create()
        else:
            self.empty()
        with open(path) as f:
            for line in f:
                self.write(line)
        self.flush()

    @must_exist
    def copy(self, dest):
        if not isinstance(dest, AbstractFileResource):
            raise ValueError('Destination must be a file resource')
        dest.reset()
        while True:
            chunk = self.read(65535)
            if not chunk:
                break
            dest.write(chunk)
        dest.flush()


class AbstractDirectoryResource(AbstractResource):

    def join(self, name):
        return os.path.join(self.url, name)

    __div__ = join

    @must_exist
    def __getitem__(self, name):
        return Resource(self.join(name))

    @must_exist
    def list(self):
        'ls the directory as a list'
        raise NotImplementedError()

    @must_exist
    def __iter__(self):
        for name in self.list():
            yield self[name]

    @must_exist
    def get(self, path, overwrite=False):
        'copy the resource to local `path`'
        if not overwrite and os.path.exists(path):
            raise IOError('`{}` exists. Aborting'.format(path))
        if not os.path.exists(path):
            os.makedirs(path)
        for name in self.list:
            newpath = os.path.join(path, name)
            self[name].get(newpath, overwrite=False)

    @ensure_exists
    def put(self, path, overwrite=False):
        'copy local `path` into the resource'
        if not os.path.is_dir(path):
            raise ValueError('`{}` is not a directory. Aborting'.format(path))
        if not self.exist:
            self.create
        for name in os.listdir(path):
            child_path = os.path.join(path, name)
            if os.path.isdir(child_path):
                name += '/'
            child = self[name]
            if child.exists and not overwrite:
                raise IOError('`{}` exists. Aborting'.format(path))
            child.put(os.path.join(path, name))
