import os

from . import gzip
from . import bzip

# XXX: implement none module
none='nomodule'

COMP_METHODS = {
    '': none,
    '.gz': gzip,
    '.bz2': bzip,
}

def guess_compression_module(path):
    while True:
       path, ext = os.path.splitext(path)
       mod = COMP_METHODS.get(ext, None)
       if mod is not None:
           return mod
