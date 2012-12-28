"""
Handle files and directories over SSH.

The `scp` command line tool is preferred to paramiko for performance reasons.
Then to avoid mixing command line tools and paramiko, `ssh` is also used.
Please refer to `ssh` and `ssh_config` documentation to configure session
multiplexing.

Examples
========

>>> import os
>>> import tempfile
>>> host = os.getenv('EXAMPLE_HOST', 'localhost')
>>> dir = Resource('/tmp', host)
>>> isinstance(dir, SSHDirectoryResource)
True
>>> tmp = tempfile.NamedTemporaryFile()
>>> with open(tmp.name, 'wb') as fp:
...     fp.write('a\\nb\\nc\\n')
>>> dir.add(tmp.name)
>>> filename = os.path.basename(tmp.name)
>>> filename in dir
True
>>> file = dir[filename]
>>> file.read()
'a\\nb\\nc\\n'
>>> list(file)
['a', 'b', 'c']
>>> file.get(tmp.name) == tmp.name
True
>>> dir.remove(tmp.name)
>>> tmp.name in dir
False

"""
from __future__ import absolute_import

import os
import subprocess
import collections

from .base import AbstractResource, scheme_to_resource


DEFAULT_LOCATION = 'localhost'


StatResult = collections.namedtuple('StatResult',
                                    ['st_atime', 'st_mtime', 'st_ctime'])


def _stat(path, format, location=DEFAULT_LOCATION):
    return ssh(location,
               "stat \"--format='{}'\" {}".format(format, path))[0]


def is_dir(path, location=DEFAULT_LOCATION):
    return (_stat(path, ' %F', location)
                .strip()
                .rsplit(' ', 1)[-1]) == 'directory'


OPTIONS = ' '.join(['-o "PasswordAuthentication no"',  # returns an error
                    # instead of asking the password if public is not accepted
                    '-o "StrictHostKeyChecking no"',  # does not ask to
                    # confirm the host fingerprint
               ])

SCP = 'scp -B {options}'.format(options=OPTIONS)
SSH = 'ssh {options}'.format(options=OPTIONS)


def path(host, path, user=''):
    return '{user}@{host}:{path}'.format(user=user, host=host, path=path)


def scp(src, dst, options=None):
    if options is None:
        options = []

    command = '{scp} {options} {src} {dst}'.format(
               scp=SCP, src=src, dst=dst,
               options=' '.join(options))
    return subprocess.call(command, shell=True)


def ssh(host, command):
    command = '{ssh} {host} {cmd}'.format(ssh=SSH,
                                          host=host,
                                          cmd=command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    return process.communicate()


def Resource(path, location=DEFAULT_LOCATION):
    if is_dir(path, location):
        return SSHDirectoryResource(path, location)
    return SSHFileResource(path, location)


class SSHResource(AbstractResource):
    type = 'ssh'

    def __init__(self, path, location):
        """
        Parameters
        ---------
        path: str
            path to a file on the remote host
        location: str
            location of the remote host:
                - host
                - user@host

        """
        AbstractResource.__init__(self, 'ssh://' + location + path)
        self._path = path
        self._location = location
        location_ = location.split('@', 1)
        if len(location_) > 1:
            self._user = location_[0]
            self._host = location_[1]
        else:
            self._host = location
        self._remote_path = '{host}:{path}'.format(host=location, path=path)

    @property
    def path(self):
        return self._path

    @property
    def location(self):
        return self._location

    @property
    def name(self):
        return os.path.basename(self._path)

    @property
    def user(self):
        return self._user

    @property
    def host(self):
        return self._host

    @property
    def size(self):
        return int(ssh(self._location,
                       'du -bs {}'.format(self.path))[0].split()[0])

    @property
    def atime(self):
        return self.stat().st_atime

    @property
    def ctime(self):
        return self.stat().st_ctime

    @property
    def mtime(self):
        return self.stat().st_mtime

    def stat(self):
        stdout = ssh(self._location,
                     "stat \"--format='%X %Y %Z'\" {}".format(self._path))[0]
        return StatResult(*map(int, stdout.split()))

    def get(self, path):
        scp(self._remote_path, path)
        return path

    def put(self, path):
        scp(path, self._remote_path)


class SSHFileResource(SSHResource):
    def read(self):
        return ssh(self._location, 'cat {}'.format(self._path))[0]

    def __iter__(self):
        start = 0
        end = 0
        content = self.read()
        for c in content:
            end += 1
            if c == '\n':
                yield content[start:end - 1]
                start = end


class SSHDirectoryResource(SSHResource):
    def join(self, name):
        return os.path.join(self.path, name)

    __div__ = join

    def __getitem__(self, name):
        return Resource(self.join(name), self._location)

    def add(self, path, overwrite=True):
        filename = os.path.basename(path)
        if not overwrite and filename in self:
            raise IOError("'{}' already exists in '{}' on {}".format(
                          filename, self.path, self.location))
        SSHFileResource(self / filename, self._location).put(path)

    def update(self, files):
        for file in files:
            file.put(self.join(file.name))

    def remove(self, filename):
        ssh(self._location, 'rm {}'.format(self.join(filename)))

    def __iter__(self):
        stdout = ssh(self._location, 'ls -1 {}'.format(self._path))[0]
        return iter(stdout.strip().split())

    def __contains__(self, name):
        return name in list(self)

    def get(self, path):
        scp(self._remote_path, path, options=['-r'])
        return path

    def put(self, path):
        scp(path, self._remote_path, options=['-r'])


scheme_to_resource.register('ssh', Resource)
