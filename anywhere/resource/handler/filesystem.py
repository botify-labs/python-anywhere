"""
Handles local files and directories.

Examples
========

Handling a file
---------------

Let's start by importing some modules to compare the resource interface with
the Python standard library: ::

    >>> import tempfile
    >>> import os.path
    >>> import shutil

Now we create a temporary file and wrap it in a FileResource: ::

    >>> tmp = tempfile.NamedTemporaryFile()
    >>> file = Resource(tmp.name)
    >>> file # doctest: +ELLIPSIS
    <FileResource file:///tmp/tmp...>
    >>> str(file) # doctest: +ELLIPSIS
    'file:///tmp/tmp...'
    >>> file.path == tmp.name
    True
    >>> file.name == os.path.basename(tmp.name)
    True
    >>> file.size
    0

The file is currently empty. We can append a line to it to add some content: ::

    >>> file.append('First line.')
    >>> file.read()
    'First line.'

We can also add several lines at the same time: ::

    >>> file.extend(['Second line.', 'Third line.'])
    >>> file.read()
    'Second line.\\nThird line.'

Handling a directory
--------------------

We continue by creating a temporary directory: ::

    >>> tmpdir = tempfile.mkdtemp()
    >>> dir = Resource(tmpdir)

The file is not inside:

    >>> file in dir
    False

Let's copy it into the directory: ::

    >>> dir.add(file)
    >>> file in dir
    True
    >>> list(dir) == [file.name]
    True
    >>> file_alias = dir[file.name]
    >>> file_alias.read() == file.read()
    True

By default `meth:FileResource.add` overwrites the file if it already exists in
the directory: ::

    >>> dir.add(file)

Setting the parameter *overwrite* to `False` allows to prevent from overwriting
the file:

    >>> dir.add(file, overwrite=False) #doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    IOError: '...' already exists in '/tmp/...'

    >>> dir.remove(file)
    >>> list(dir)
    []

Beware the a file alias may reference a file that was removed:

    >>> file_alias.read() #doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    IOError: [Errno 2] No such file or directory: '/tmp/...'

Let's clean: ::

    >>> shutil.rmtree(dir.path)

"""
from __future__ import absolute_import

import os
import shutil
from .base import AbstractResource, scheme_to_resource


def Resource(path, location=''):
    if os.path.isdir(path):
        return DirectoryResource(path)
    return FileResource(path)


class FilesystemResource(AbstractResource):
    type = 'filesystem'

    def __init__(self, path, location=''):
        AbstractResource.__init__(self, 'file://' + path)
        self._path = path

    @property
    def path(self):
        return self._path

    @property
    def location(self):
        return ''

    @property
    def name(self):
        return os.path.basename(self._path)

    @property
    def size(self):
        return os.stat(self.path).st_size

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
        return os.stat(self.path)

    def __iter__(self):
        raise NotImplementedError()


class FileResource(FilesystemResource):
    """"
    A file is represented like a sequence of lines.

    """
    def read(self):
        return open(self._path).read()

    def __iter__(self):
        return open(self._path)

    def append(self, line):
        with open(self._path, 'wb') as fp:
            fp.write(line)

    def extend(self, lines):
        with open(self._path, 'wb') as fp:
            fp.writelines('\n'.join(lines))


class DirectoryResource(FilesystemResource):
    """
    A directory is represented like a set of files.

    """
    def join(self, name):
        return os.path.join(self.path, name)

    __div__ = join

    def __getitem__(self, name):
        return Resource(self.join(name))

    def add(self, file, overwrite=True):
        if not overwrite and file in self:
            raise IOError("'{}' already exists in '{}'".format(
                          file.name, self.path))
        shutil.copy(file.path, self.path)

    def remove(self, file):
        os.unlink(self.join(file.name))

    def __contains__(self, file):
        return os.path.exists(self.join(file.name))

    def __iter__(self):
        return iter(os.listdir(self._path))


scheme_to_resource.register('file', Resource)
