## Overview

Anywhere provides a set of modules to work with resources that are
identified by a URL. It comes with a set of handlers that can be extended.

It is licensed under a BSD 2-Clause license (see LICENSE.txt).

## The Resource type

The Resource type abstracts access to a resource identified by a URL that contains:

- the protocol
- the path

```python
>>> remote = Resource('ssh://storage1/path/to/file.json')
```

Every Resource implements the interface:

- path: full path without the protocol
- str returns the URL
- name
- size
- atime
- mtime
- ctime
- get(path)
- put(path)
- create()
- delete()
- read(size)
- flush()
- copy(Resource)
- empty()

FileResources also implements

- write()
- __iter__() – iterate over the resources lines
- reset() – abort not flushed writes

DirectoryResources implement

- list() – list childs names
- __iter__() – iterate over child resources
- add(Resource) – add an new child


## Examples

These examples come from the filesystem interface in `anywhere.resource.handler.filesystem`.

### Handling a file

Let's start by importing some modules to compare the resource interface with
the Python standard library:

```python
>>> import tempfile
>>> import os.path
>>> import shutil
```

Now we create a temporary file and wrap it in a `FileResource`:

```python
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
```

The file is currently empty. We can append a line to it to add some content:

```python
>>> file.append('First line.')
>>> file.read()
'First line.'
```

We can also add several lines at the same time:

```python
>>> file.extend(['Second line.', 'Third line.'])
>>> file.read()
'Second line.\\nThird line.'
```

### Handling a directory

We continue by creating a temporary directory:

```python
>>> tmpdir = tempfile.mkdtemp()
>>> dir = Resource(tmpdir)
```

The file is not inside:

```python
>>> file in dir
False
```

Let's copy it into the directory:

```
>>> dir.add(file)
>>> file in dir
True
>>> list(dir) == [file.name]
True
>>> file_alias = dir[file.name]
>>> file_alias.read() == file.read()
True
```

By default `meth:FileResource.add` overwrites the file if it already exists in
the directory:

```python
    >>> dir.add(file)
```

Setting the parameter *overwrite* to `False` allows to prevent from overwriting
the file:

```python
>>> dir.add(file, overwrite=False) #doctest: +ELLIPSIS
Traceback (most recent call last):
...
IOError: '...' already exists in '/tmp/...'

>>> dir.remove(file)
>>> list(dir)
[]
```

Beware the a file alias may reference a file that was removed:

```python
>>> file_alias.read() #doctest: +ELLIPSIS
Traceback (most recent call last):
...
IOError: [Errno 2] No such file or directory: '/tmp/...'
```
