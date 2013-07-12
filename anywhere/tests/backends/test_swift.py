import os
import tempfile
import subprocess

from unittest import TestCase
from anywhere.resource.handler import Resource
from anywhere.resource.handler.swift import register_location, location_registry
from anywhere.resource.handler.swift import SwiftDirectoryResource, SwiftFileResource


FILE1_LINE1 = 'file 1 line 1 content\n'
FILE1_LINE2 = 'file 1 line 2 content\n'
FILE1_LINE3 = 'file 1 line 3 content'
FILE1_CONTENT = FILE1_LINE1 + FILE1_LINE2
SWIFT_LOCATION = 'testloc'
SWIFT_CONTAINER = 'anywhere_test'
DIR1_NAME = 'dir1'
CONTAINER_URL = 'swift://{}/{}'.format(SWIFT_LOCATION,
                                        SWIFT_CONTAINER)
DIR1_URL = '{}/{}/'.format(CONTAINER_URL, DIR1_NAME)
FILE1_URL = '{}file1'.format(DIR1_URL)
FILE2_URL = '{}file2'.format(DIR1_URL)
PUTFILE_CONTENT = "put file content"
SWIFT_ENV = {
    'OS_USERNAME': os.environ['ANYWHERE_TEST_USERNAME'],
    'OS_TENANT_NAME': os.environ['ANYWHERE_TEST_TENANT_NAME'],
    'OS_AUTH_URL': os.environ['ANYWHERE_TEST_AUTH_URL'],
    'OS_PASSWORD': os.environ['ANYWHERE_TEST_PASSWORD']
}


def init_swift_backend():
    register_location(SWIFT_LOCATION,
                      os.environ['ANYWHERE_TEST_USERNAME'],
                      os.environ['ANYWHERE_TEST_TENANT_NAME'],
                      os.environ['ANYWHERE_TEST_AUTH_URL'],
                      os.environ['ANYWHERE_TEST_PASSWORD']
                      )
    tmpdir = tempfile.mkdtemp()
    testdir = os.path.join(tmpdir, DIR1_NAME)
    os.makedirs(testdir)
    with open(os.path.join(testdir, 'file1'), 'w') as f:
        f.write(FILE1_CONTENT)
    swift_command(['swift', 'delete', SWIFT_CONTAINER])
    swift_command(['swift', 'upload', SWIFT_CONTAINER, DIR1_NAME], cwd=tmpdir)
    return tmpdir


def swift_command(cmd, cwd=None):
    if isinstance(cmd, basestring):
        cmd = cmd.split(' ')
    env = os.environ.copy()
    env.update(SWIFT_ENV)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=cwd, env=env)
    proc.wait()
    return proc.stdout.read()


class TestSwiftLocation(TestCase):
    def setUp(self):
        self.tmpdir = init_swift_backend()

    def test_iter_container(self):
        it = location_registry[SWIFT_LOCATION].iter_container()
        containers = list(it)
        self.assertIn(SWIFT_CONTAINER, containers)


class TestSwiftDirectory(TestCase):
    def setUp(self):
        init_swift_backend()
        self.dir1 = Resource(DIR1_URL)
        self.container = Resource(CONTAINER_URL)

    def test_url(self):
        self.assertEqual(self.dir1.url, DIR1_URL)

    def test_init(self):
        self.assertTrue(self.dir1.exists)
        self.assertEqual(self.dir1.list(), ['file1'])
        self.assertIsInstance(self.container, SwiftDirectoryResource)

    def test_exists(self):
        self.assertTrue(self.dir1.exists)
        self.assertTrue(self.container.exists)
        self.assertFalse(Resource(CONTAINER_URL+'/notadir/').exists)
        self.assertFalse(Resource(DIR1_URL + 'notatfile').exists)
        cont_url = 'swift://{}/{}'.format(SWIFT_LOCATION,'notacontainer')
        self.assertFalse(Resource(cont_url).exists)


class TestSwiftFile(TestCase):
    def setUp(self):
        init_swift_backend()
        self.file1 = Resource(FILE1_URL)

    def test_url(self):
        self.assertEqual(self.file1.url, FILE1_URL)

    def test_iter(self):
        file_list = [line for line in self.file1]
        self.assertEqual(file_list, [FILE1_LINE1, FILE1_LINE2])

    #def test_init(self):
        #"""test Resource init"""
        ## existing file
        #self.assertTrue(self.file1.exists)
        #self.assertEqual(self.file1.read(), FILE1_CONTENT)
        ## non existing file
        #file2 = Resource(FILE2_URL)
        #self.assertFalse(file2.exists)

    #def test_create(self):
        ### existing file are not created
        #self.assertTrue(self.file1.exists)
        #self.assertFalse(self.file1.create())
        ### non existing files are crated
        #file2 = Resource(FILE2_URL)
        #self.assertFalse(file2.exists)
        #self.assertTrue(file2.create())
        #self.assertTrue(file2.exists)
        ## the file exists and is empty
        #self.assertEqual(__db__.schemes['mem']['testloc']['root']['file2'],
                         #'')

    #def test_delete(self):
        #self.assertTrue(self.file1.exists)
        #self.assertIn('file1', __db__.schemes['mem']['testloc']['root'])
        #self.file1.delete()
        #self.assertFalse(self.file1.exists)
        #self.assertNotIn('file1', __db__.schemes['mem']['testloc']['root'])

    #def test_write(self):
        #NEW_CONTENT = "42"
        #self.file1.write(NEW_CONTENT)
        #self.file1.flush()
        #self.assertEqual(self.file1.read(), NEW_CONTENT)

    #def test_append(self):
        #self.file1.append(FILE1_LINE3)
        #self.file1.flush()
        #self.assertEqual(self.file1.read(), FILE1_CONTENT + FILE1_LINE3)

    #def test_flush(self):
        #NEW_CONTENT = "42"
        #self.file1.write(NEW_CONTENT)
        ## can't read until flushed
        #with self.assertRaises(IOError):
            #self.file1.read()
        ## file data are not affected by the write
        #self.assertEqual(__db__.schemes['mem']['testloc']['root']['file1'],
                         #FILE1_CONTENT)
        #self.file1.flush()
        #self.assertEqual(self.file1.read(), NEW_CONTENT)
        #self.assertEqual(__db__.schemes['mem']['testloc']['root']['file1'],
                         #NEW_CONTENT)

    #def test_get(self):
        #path = tempfile.mktemp()
        #self.file1.get(path)
        #with open(path) as fd:
            #self.assertEqual(fd.read(), FILE1_CONTENT)
        #os.remove(path)

    #def test_put(self):
        #path = tempfile.mktemp()
        #with open(path, 'w') as fd:
            #fd.write()
            #self.assertEqual(fd.read(), FILE1_CONTENT)


