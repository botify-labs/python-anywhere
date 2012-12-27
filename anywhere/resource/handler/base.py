from urlparse import urlparse

from anywhere.resource.types import RegisterDict


all = ['Resource']


scheme_to_resource = RegisterDict()


def Resource(url):
    result = urlparse(url)
    return scheme_to_resource[result.scheme](path=result.path,
                                             location=result.netloc)


class AbstractResource(object):
    type = 'abstract'

    def __init__(self, url):
        self.url = url

    @property
    def path(self):
        return self.url.split('://', 1)[1:]

    @property
    def name(self):
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

    def get(self, path=None):
        raise NotImplementedError()

    def put(self, url):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()

    def __str__(self):
        return self.url

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.url)
