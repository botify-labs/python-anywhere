"""
Handles swift objects.
"""
from __future__ import absolute_import

import sys
import os
from os import environ
import subprocess
import tempfile
import datetime
from cStringIO import StringIO
import shutil

try:
    from swiftclient import client as swift
    from swiftclient import Connection, ClientException, HTTPException, utils
except:
    # let the module import run. It will raise only if the
    # user tries to use 'swift://' urls
    pass

from .base import AbstractFileResource, AbstractDirectoryResource
from .base import scheme_to_resource
from .base import UnknownScheme
from .exceptions import URLError
from anywhere.compression.utils import guess_compression_module
from anywhere.utils.io import OHelper


SWIFT_CMD = 'swift'


class SwiftError(Exception):
    "an error in swift CLI call"
    def __init__(self, message, errno):
        super(SwiftError, self).__init__(message)
        self.errno = errno


### swift lib helpers
def get_conn_params(auth_url=environ.get('OS_AUTH_URL'),
                    user_name=environ.get('OS_USER_NAME'),
                    password=environ.get('OS_PASSWORD'),
                    tenant_id=environ.get('OS_TENANT_ID'),
                    tenant_name=environ.get('OS_TENANT_NAME'),
                    service_type=environ.get('OS_SERVICE_TYPE'),
                    endpoint_type=environ.get('OS_ENDPOINT_TYPE'),
                    auth_token=environ.get('OS_AUTH_TOKEN'),
                    object_storage_url=environ.get('OS_STORAGE_URL'),
                    region_name=environ.get('OS_REGION_NAME'),
                    snet=False,
                    os_cacert=environ.get('OS_CACERT'),
                    insecure=utils.config_true_value(
                        environ.get('SWIFTCLIENT_INSECURE')),
                    ssl_compression=True
                   ):
    "create a ConnectionParam given new style params"

    os_options = {
        'tenant_id': tenant_id,
        'tenant_name': tenant_name,
        'service_type': service_type,
        'endpoint_type': endpoint_type,
        'auth_token': auth_token,
        'object_storage_url': object_storage_url,
        'region_name': region_name}

    return ConnectionParams(auth_url, user_name, password, '2.0', os_options,
                            snet, os_cacert, insecure, ssl_compression)


class ConnectionParams(object):
    """
    Connection parameters including old-style params.
    """
    def __init__(self, auth, user, key, auth_version, os_options,
                 snet=False,
                 os_cacert=environ.get('OS_CACERT'),
                 insecure=utils.config_true_value(
                     environ.get('SWIFTCLIENT_INSECURE')),
                 ssl_compression=True
                ):
        self.auth = auth
        self.user = user
        self.key = key
        self.auth_version = auth_version
        self.os_options = os_options
        self.snet = snet
        self.os_cacert = os_cacert
        self.insecure = insecure
        self.ssl_compression = ssl_compression


def get_conn(options):
    """
    Return a connection building it from the options.
    """
    return Connection(options.auth,
                      options.user,
                      options.key,
                      auth_version=options.auth_version,
                      os_options=options.os_options,
                      snet=options.snet,
                      cacert=options.os_cacert,
                      insecure=options.insecure)
                      #ssl_compression=options.ssl_compression)


### register location stuff
class LocationRegistry(dict):
    def register(self, key, value):
        self[key] = value


location_registry = LocationRegistry()


def register_location(name, user_name, tenant_name, auth_url, password, temp_dir=None):
    location_registry[name] = SwiftLocation(name, user_name, tenant_name, auth_url,
                                            password, temp_dir)


def unregister_location(name):
    loc = location_registry.get(name, None)
    if loc is None:
        return
    loc.close()
    del location_registry[name]

class SwiftFileBody(OHelper):
    def __init__(self, body):
        self.body = body
        self.buf = StringIO()

    def read(self, size=-1):
        if size == -1:
            size=sys.maxint
        while self.buf.tell() < size:
            try:
                next_chunk = self.body.next()
            except StopIteration:
                break
            self.buf.write(next_chunk)
        return self.flush_stream(pos=size)

    def flush_stream(self, pos):
        data = self.buf.getvalue()
        pos = pos < len(data) and pos or len(data)
        self.buf.seek(0)
        self.buf.truncate()
        self.buf.write(data[pos:])
        return data[:pos]


class SwiftLocation(object):
    """
    a swift location abstraction. Holds the configuration for the
    location and is Responsible for all swift command invocation.
    """
    def __init__(self, name, user_name, tenant_name, auth_url,
                 password, temp_dir):
        self.name = name
        self.base_temp_dir = temp_dir or tempfile.gettempdir()
        self.tmpdirs = set()
        self.swift_env = {
            'OS_USERNAME': user_name,
            'OS_TENANT_NAME': tenant_name,
            'OS_AUTH_URL': auth_url,
            'OS_PASSWORD': password
        }
        self.conn_params = get_conn_params(auth_url=auth_url, user_name=user_name,
                                           tenant_name=tenant_name, password=password)
        self._dirs = {}

    @property
    def conn(self):
        return get_conn(self.conn_params)

    def close(self):
        for dir_ in self.tmpdirs:
            shutil.rmtree(dir_)
        del self.base_temp_dir
        del self.tmpdirs
        del self.swift_env
        del self.conn_params
        del self._dirs
        self.__class__ = ClosedSwiftLocation

    def iter_container(self, container=''):
        ''' iterate the names of objects in a container, or all containers
        names if no container is provided'''
        cmd = [SWIFT_CMD, 'list', container]
        list_ = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=self.env)
        while True:
            out = list_.stdout.readline()
            if out == '':
                return_code = list_.poll()
                if return_code:
                    raise SwiftError(list_.stderr.read(), return_code)
                if return_code == 0:
                    break
            yield out.strip()

    def open_file(self, container, path):
        headers, body = \
            self.conn.get_object(container, path, resp_chunk_size=65536)
        ## XXX: check headers for errors
        return SwiftFileBody(body)

    @property
    def env(self):
        if not hasattr(self, '_env'):
            self._env = os.environ.copy()
            self._env.update(self.swift_env)
        return self._env

    def call_cmd(self, cmd, cwd=None):
        if isinstance(cmd, basestring):
            cmd = cmd.split()
        return subprocess.Popen(cmd, stderr=subprocess.PIPE, cwd=cwd,
                                stdout=subprocess.PIPE, env=self.env)

    def get_temp_file(self, container, path, tmpdir=None, mode='r'):
        '''
        get_temp_file(str, str, str, str) -> (file, str)

        return an open file (according to `mode`). It's created at
        the given `tmpdir`/`path`. If tmpdir is not provided, a new one
        is created. tmpdir path is returned along with the open file.'''
        tmpdir = tmpdir or tempfile.mkdtemp(dir=self.base_temp_dir)
        self.tmpdirs.add(tmpdir)
        path = os.path.join(tmpdir, container, path)
        dirname, basename = os.path.split(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return (open(path, mode), tmpdir)

    def simple_cmd(self, cmd, cwd=None):
        'helper to execute a swift command in given cwd'
        if isinstance(cmd, basestring):
            cmd = cmd.split(' ')
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, cwd=cwd,
                                stdout=subprocess.PIPE, env=self.env)
        retcode = proc.wait()
        if retcode != 0:
            raise SwiftError(proc.stderr.read(), retcode)
        return proc.communicate()[0]

    def get_stat(self, container='', path=''):
        """get object's stats"""
        cmd = [SWIFT_CMD, 'stat', container, path]
        return self.simple_cmd(cmd)

    def push_temp(self, container, path, tmpdir):
        self.push_object(container, path,
                        cwd=os.path.join(tmpdir, container), changes=False)

    def push_object(self, container, path, cwd=None, changes=True):
        '''send an object to swift backend'''
        cmd = [SWIFT_CMD, 'upload', changes and '-c' or '', container, path]
        self.simple_cmd(filter(None, cmd), cwd)
        return True

    def exists(self, container, path):
        if path is not None:
            path = path.lstrip('/')
        try:
            result = path in self.iter_container(container)
        except SwiftError as e:
            if e.errno == 1:
                # container not found
                return False
            else:
                raise
        return result or path is None

    def delete_object(self, container, path):
        'delete an object. if path is None, delete the container'
        if path == '':
            raise ValueError('empty path. If you want to remove a container, use path=None')
        if path is None:
            path = ''
        cmd = [SWIFT_CMD, 'delete', container, path]
        try:
            self.simple_cmd(cmd)
        except SwiftError as e:
            if e.errno == 1:
                # path not found
                return False
            else:
                raise
        return True

    def create_container(self, container_name):
        'create a container'
        # right now, using swift cli the only way to create a container
        # is to push and remove a file
        container_name = str(container_name)
        if container_name == '':
            raise ValueError('Empty container_name')
        if not container_name in self.iter_container():
            tmp = tempfile.mktemp()
            with open(tmp, 'w'):
                pass
            self.push_object(container_name, tmp)
            self.delete_object(container_name, tmp.lstrip('/'))

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)


class NotActive(Exception):
    '''a Swift Location is no more active'''

class ClosedSwiftLocation(SwiftLocation):
    """
    """
    def __getattr__(self, name):
        if name in ('name', '__class__'):
            return super(ClosedSwiftLocation, self).__getattr__(name)
        raise NotActive('SwiftLocation `{}` is no more active'.format(self.name))

    def __setattr__(self, name, value):
        raise NotActive('SwiftLocation `{}` is no more active'.format(self.name))


def Resource(path, location='', scheme='swift'):
    if location not in location_registry:
        raise ValueError('swift location `{}` is not registered yet')
    location = location_registry[location]
    if path.endswith('/') or not '/' in path.strip('/'):
        return SwiftDirectoryResource(path, location)
    return SwiftFileResource(path, location)


class SwiftResource(object):
    type = 'swift'

    def __init__(self, path, location, _tmpdir=None):
        self._path = path
        self._location = location
        chunks = path.strip('/').split('/', 1)
        try:
            self._container = chunks[0]
        except IndexError:
            self._container = ''
        if len(chunks) == 1:
            self._local_path = None
        else:
            self._local_path = chunks[1]
        self._stat_ = None
        self._tmpdir = _tmpdir

    @property
    def _stat(self):
        if not self._stat_:
            stat = self._location.get_stat(self._container, self._local_path)
            self._stat_ = {}
            for line in stat.splitlines():
                key, value = line.split(':',1)
                self._stat_[key.strip()] = value.strip()
        return self._stat_

    @property
    def path(self):
       return self._path

    @property
    def location(self):
       return str(self._location)

    @property
    def url(self):
        return "{}://{}{}".format(self.type,
                                   self._location,
                                   self._path)

    @property
    def exists(self):
        return self._location.exists(self._container, self._local_path)

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
        return self._location.delete_object(self._container, self._local_path)

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


class SwiftFileResource(SwiftResource, AbstractFileResource):

    @property
    def _read_stream(self):
        if hasattr(self, "_write_stream_"):
            raise IOError("can't read until write operation is flush")
        if hasattr(self, "_append_stream_"):
            raise IOError("can't read until append operation is flush")
        if not hasattr(self, "_read_stream_"):
            self._read_stream_ = self._location.open_file(self._container, self._local_path)
        return self._read_stream_

    @property
    def _write_stream(self):
        if hasattr(self, "_append_stream_"):
            raise IOError("can't write until append operation is flush")
        if not hasattr(self, "_write_stream_"):
            self._write_stream_, self._tmpdir = self._location.get_temp_file(
                self._container, self._local_path, self._tmpdir, 'w')
        return self._write_stream_

    @property
    def _append_stream(self):
        if hasattr(self, "_write_stream_"):
            raise IOError("can't append until write operation is flush")
        if not hasattr(self, "_append_stream_"):
            self._append_stream_ = StringIO()
            self._append_stream_.write(self.db_object)
        return self._append_stream_

    def uncompress(self):
        mod = guess_compression_module(self._local_path)
        return mod.open(self._read_stream)

    @property
    def size(self):
        if not self.exists:
            return 0
        return int(self._stat["Content Length"])

    def flush(self):
        for stream_name in ['_write_stream_', '_append_stream_']:
            if hasattr(self, stream_name):
                stream = getattr(self, stream_name)
                stream.close()
                self._location.push_temp(self._container,
                                         self._local_path, self._tmpdir)
                delattr(self, stream_name)

    def close(self):
        self.flush()
        if self._tmpdir:
            try:
                os.remove(os.path.join(self._tmpdir, self._container,
                                       self._local_path))
            except OSError:
                pass

    def reset(self):
        "abort not flushed writes"
        for stream_name in ['_write_stream_', '_append_stream_']:
            if hasattr(self, stream_name):
                stream = getattr(self, stream_name)
                stream.close()
                delattr(self, stream_name)

    def empty(self, flush=True):
        'empty the file'
        self.reset()
        self.write('')
        if flush:
            self.flush()

    def create(self):
        if self.exists:
            return False
        raise NotImplementedError()
        return True

    def read(self, size=-1):
        'read at most `size` bytes, returned as a string'
        return self._read_stream.read(size)

    def write(self, string):
        return self._write_stream.write(string)

    def append(self, string):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self._read_stream)


class SwiftDirectoryResource(SwiftResource, AbstractDirectoryResource):

    @property
    def size(self):
        if not self.exists:
            return 0
        return int(self._stat["Bytes"])

    def flush(self):
        'does nothing'

    @property
    def exists(self):
        if self._local_path is None:
            # just check that the container exists
            return super(SwiftDirectoryResource, self).exists
        try:
            for entry in self._location.iter_container(self._container):
                if entry.startswith(self._local_path + '/'):
                    return True
        except SwiftError as e:
            if e.errno == 1:
                return False
            else:
                raise
        return False

    def list(self, recursive=False):
        if not self.exists:
            return []
        else:
            result = set()
            path = self._local_path or ''
            for entry in self._location.iter_container(self._container):
                if entry.startswith(path):
                    entry = entry[len(path):].strip('/')
                    if recursive or not '/' in entry:
                        result.add(entry)
                    if '/' in entry:
                        dir_, _ = entry.split('/', 1)
                        result.add(dir_ + '/')
        result = list(result)
        result.sort()
        return result

    def create(self):
        raise NotImplementedError()


try:
    import swiftclient  # noqa
except ImportError:
    def unknown(path, location, scheme=None):
        raise UnknownScheme('`swift://` sheme requires python-swiftclient')
    scheme_to_resource.register('swift', unknown)
else:
    scheme_to_resource.register('swift', Resource)
