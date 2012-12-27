## Overview

Anywhere provides a set of modules to work with resources that are
identified by a URL. It comes with a set of handlers that can be extended.

## The Resource type

The Resource type abstracts access to a resource identified by a URL that contains:

- the protocol
- the path

```python
>>> remote = Resource('ssh://storage1/path/to/file.json')
```

Every Resource implements the interface:

- type: file | dir
- path: full path without the protocol
- str returns the URL
- name
- size
- mtime
- get(path=None)
- put(url)
- delete()
- read()


## Example

```python
>>> local = remote.get()
>>> local
<FilesystemResource 'file:///path/to/file.json' type='file'>
>>> local.type
'file'
>>> local.basename
'file.json'
>>> local.path
'/storage1/path/to/file.json'
>>> str(local)
'ssh://storage1/path/to/file.json'
>>> local.codec
<JSONCodec>
>>> local_yaml = local.encode(path='/path/to/file.yaml')
>>> local_yaml
<FilesystemResource 'file:///path/to/file.yaml'>
>>> local_yaml.delete()
>>> url = 's3:///path/to/bucket'
>>> local.put('s3:///path/to/bucket')
>>> remote_s3 = Resource(s3_url)

>>> remote_dir = Resource('ssh://storage1/path/to')
>>> remote_dir
<SSHResource 'ssh://storage1/path/to' type='dir'>
```
