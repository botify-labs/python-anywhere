"""
Handles swift objects.
"""
from __future__ import absolute_import

import os
import subprocess
import tempfile
import datetime

try:
    from swiftclient import client as swift
except:
    # let the module import run. It will raise only if the
    # user tries to use 'swift://' urls
    pass

from .base import AbstractFileResource, AbstractDirectoryResource
from .base import scheme_to_resource
from .base import UnknownScheme
from .exceptions import URLError


SWIFT_CMD = 'swift'


class LocationRegistry(dict):
    def register(self, key, value):
        self[key] = value

location_registry = LocationRegistry()

def register_location(name, user_name, tenant_name, auth_url, password):
    location_registry[name] = SwiftLocation(name, user_name, tenant_name, auth_url,
                                            password)


class SwiftError(Exception):
    "an error in swift CLI call"
    def __init__(self, message, errno):
        super(SwiftError, self).__init__(message)
        self.errno = errno


class SwiftLocation(object):
    """
    a swift location abstraction. Holds the configuration for the
    location and is Responsible for all swift command invocation.
    """
    def __init__(self, name, user_name, tenant_name, auth_url, password):
        self.name = name
        self.swift_env = {
            'OS_USERNAME': user_name,
            'OS_TENANT_NAME': tenant_name,
            'OS_AUTH_URL': auth_url,
            'OS_PASSWORD': password
        }
        self._dirs = {}

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

    def iter_file(self, container, path, cwd=None):
        print container, path
        return iter(self.stream_cmd([SWIFT_CMD , 'download', container,
                                     path, '-o', '-']))

    @property
    def env(self):
        if not hasattr(self, '_env'):
            self._env = os.environ.copy()
            self._env.update(self.swift_env)
        return self._env

    def stream_cmd(self, cmd, cwd=None):
        if isinstance(cmd, basestring):
            cmd = cmd.split()
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, cwd=cwd,
                                stdout=subprocess.PIPE, env=self.env)
        while True:
            out = proc.stdout.readline()
            if out == '':
                return_code = proc.poll()
                if return_code:
                    raise SwiftError(proc.stderr.read(), return_code)
                if return_code == 0:
                    break
            yield out


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

    def push_object(self, container, path, changes=True):
        '''send an object to swift backend'''
        cmd = [SWIFT_CMD, 'upload', changes and '-c' or '', container, path]
        self.simple_cmd(cmd)
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


def Resource(path, location='', scheme='swift'):
    if location not in location_registry:
        raise ValueError('swift location `{}` is not registered yet')
    location = location_registry[location]
    if path.endswith('/') or not '/' in path.strip('/'):
        return SwiftDirectoryResource(path, location)
    return SwiftFileResource(path, location)


class SwiftResource(object):
    type = 'swift'

    def __init__(self, path, location):
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
        return self._location.delete_object(self._location, self._local_path)

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
    def size(self):
        if not self.exists:
            return 0
        return int(self._stat["Content Length"])


    def flush(self):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()

    def empty(self, flush=True):
        raise NotImplementedError()

    def create(self):
        if self.exists:
            return False
        raise NotImplementedError()
        return True

    def read(self, size=-1):
        raise NotImplementedError()

    def write(self, string):
        raise NotImplementedError()

    def append(self, string):
        raise NotImplementedError()

    def __iter__(self):
        return self._location.iter_file(self._container, self._local_path)


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
