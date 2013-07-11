import os
import tempfile

from unittest import TestCase, skip
from anywhere.resource.handler import Resource

from anywhere.resource.handler.memory import __db__

FILE1_LINE1 = 'file 1 line 1 content\n'
FILE1_LINE2 = 'file 1 line 2 content\n'
FILE1_LINE3 = 'file 1 line 3 content'
FILE1_CONTENT = FILE1_LINE1 + FILE1_LINE2
FILE1_URL = 'mem://testloc/root/file1'
FILE2_URL = 'mem://testloc/root/file2'
PUTFILE_CONTENT = "put file content"

class TestMemoryFile(TestCase):
    def setUp(self):
       __db__.schemes['mem'] = {
           'testloc': {
                'root':{
                    'file1': FILE1_CONTENT
                }
           }
       }
       self.file1 = Resource(FILE1_URL)

    def test_init(self):
        """test Resource init"""
        # existing file
        self.assertTrue(self.file1.exists)
        self.assertEqual(self.file1.read(), FILE1_CONTENT)
        # non existing file
        file2 = Resource(FILE2_URL)
        self.assertFalse(file2.exists)

    def test_create(self):
        ## existing file are not created
        self.assertTrue(self.file1.exists)
        self.assertFalse(self.file1.create())
        ## non existing files are crated
        file2 = Resource(FILE2_URL)
        self.assertFalse(file2.exists)
        self.assertTrue(file2.create())
        self.assertTrue(file2.exists)
        # the file exists and is empty
        self.assertEqual(__db__.schemes['mem']['testloc']['root']['file2'],
                         '')

    def test_delete(self):
        self.assertTrue(self.file1.exists)
        self.assertIn('file1', __db__.schemes['mem']['testloc']['root'])
        self.file1.delete()
        self.assertFalse(self.file1.exists)
        self.assertNotIn('file1', __db__.schemes['mem']['testloc']['root'])

    def test_iter(self):
        file_list = [line for line in self.file1]
        self.assertEqual(file_list, [FILE1_LINE1, FILE1_LINE2])

    def test_write(self):
        NEW_CONTENT = "42"
        self.file1.write(NEW_CONTENT)
        self.file1.flush()
        self.assertEqual(self.file1.read(), NEW_CONTENT)

    def test_append(self):
        self.file1.append(FILE1_LINE3)
        self.file1.flush()
        self.assertEqual(self.file1.read(), FILE1_CONTENT + FILE1_LINE3)

    def test_flush(self):
        NEW_CONTENT = "42"
        self.file1.write(NEW_CONTENT)
        # can't read until flushed
        with self.assertRaises(IOError):
            self.file1.read()
        # file data are not affected by the write
        self.assertEqual(__db__.schemes['mem']['testloc']['root']['file1'],
                         FILE1_CONTENT)
        self.file1.flush()
        self.assertEqual(self.file1.read(), NEW_CONTENT)
        self.assertEqual(__db__.schemes['mem']['testloc']['root']['file1'],
                         NEW_CONTENT)

    def test_get(self):
        path = tempfile.mktemp()
        self.file1.get(path)
        with open(path) as fd:
            self.assertEqual(fd.read(), FILE1_CONTENT)
        os.remove(path)

    @skip
    def test_put(self):
        path = tempfile.mktemp()
        with open(path, 'w') as fd:
            fd.write(PUTFILE_CONTENT)
        self.file1.put(path)
        self.assertEqual(self.file1, PUTFILE_CONTENT)


class TestMemoryDirectory(TestCase):
    def setUp(self):
       __db__.schemes['mem'] = {
           'testloc': {
                'root':{
                    'file1': FILE1_CONTENT
                }
           }
       }

    def test_init(self):
        # existing directory
        dir1 = Resource('mem://testloc/root')
        self.assertTrue(dir1.exists)
        self.assertEqual(dir1.list(), ['file1'])
        # non existing directory
        dir2 = Resource('mem://testloc/plop/')
        self.assertFalse(dir2.exists)
