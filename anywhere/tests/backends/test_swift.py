import os
import tempfile
import subprocess
import shutil

from unittest import TestCase
from anywhere.resource.handler import Resource
from anywhere.resource.handler.swift import register_location, location_registry
from anywhere.resource.handler.swift import SwiftDirectoryResource, SwiftFileResource
from anywhere.resource.handler.swift import unregister_location, NotActive


FILE1_LINE1 = 'file 1 line 1 content\n'
FILE1_LINE2 = 'file 1 line 2 content\n'
FILE1_LINE3 = 'file 1 line 3 content'
FILE2_LINE1 = 'file 2 line 1 content\n'
FILE2_LINE2 = 'file 2 line 2 content\n'
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
GZIPED_FILE_CONTENT = "gziped content"
GZIPED_FILE_URL = '{}hello.gz'.format(DIR1_URL)
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
                      os.environ['ANYWHERE_TEST_PASSWORD'],
                      os.environ.get('ANYWHERE_TEST_TEMPDIR', None)
                      )
    tmpdir = tempfile.mkdtemp()
    testdir = os.path.join(tmpdir, DIR1_NAME)
    os.makedirs(testdir)
    with open(os.path.join(testdir, 'file1'), 'w') as f:
        f.write(FILE1_CONTENT)
    command = 'echo "{}" | gzip -c > {}'.format(GZIPED_FILE_CONTENT,
                                                os.path.join(testdir, 'hello.gz'))
    subprocess.Popen(command, shell=True)
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
        self.loc = location_registry[SWIFT_LOCATION]

    def tearDown(self):
        unregister_location(SWIFT_LOCATION)
        shutil.rmtree(self.tmpdir)

    def test_iter_container(self):
        it = self.loc.iter_container()
        containers = list(it)
        self.assertIn(SWIFT_CONTAINER, containers)

    def test_close(self):
        # create a tempdir
        f, tmpdir = self.loc.get_temp_file('container', 'path/to/file', None, 'w')
        f.close()
        # the location exists
        self.assertEqual(self.loc.tmpdirs, set([tmpdir]))
        # its local cache is created
        self.assertTrue(os.path.exists(tmpdir))
        self.assertTrue(os.path.isdir(tmpdir))
        # let's close our location
        unregister_location(SWIFT_LOCATION)
        # it's not active any more
        with self.assertRaises(NotActive) as e:
            self.loc.tmpdirs
        # its cache was deleted
        self.assertFalse(os.path.exists(tmpdir))


class TestSwiftDirectory(TestCase):
    def setUp(self):
        self.tmpdir = init_swift_backend()
        self.dir1 = Resource(DIR1_URL)
        self.container = Resource(CONTAINER_URL)

    def tearDown(self):
        unregister_location(SWIFT_LOCATION)
        shutil.rmtree(self.tmpdir)

    def test_url(self):
        self.assertEqual(self.dir1.url, DIR1_URL)

    def test_init(self):
        self.assertTrue(self.dir1.exists)
        self.assertEqual(self.dir1.list(), ['file1', 'hello.gz'])
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
        self.tmpdir = init_swift_backend()
        self.file1 = Resource(FILE1_URL)

    def tearDown(self):
        unregister_location(SWIFT_LOCATION)
        shutil.rmtree(self.tmpdir)

    def test_url(self):
        self.assertEqual(self.file1.url, FILE1_URL)

    def test_iter(self):
        file_list = [line for line in self.file1]
        self.assertEqual(file_list, [FILE1_LINE1, FILE1_LINE2])

    def test_ungzip(self):
        gz_file = Resource(GZIPED_FILE_URL)
        self.assertEqual(gz_file.uncompress().read(), GZIPED_FILE_CONTENT+'\n')
        gz_file = Resource(GZIPED_FILE_URL)
        self.assertEqual(gz_file.uncompress().read(200), GZIPED_FILE_CONTENT+'\n')

    def test_write(self):
        file2 = Resource(FILE2_URL)
        file2.write(FILE2_LINE1)
        file2.write(FILE2_LINE2)
        self.assertFalse(file2.exists)
        file2.flush()
        self.assertTrue(file2.exists)
        cmd = ['swift', 'download', '-o', '-', SWIFT_CONTAINER, 'dir1/file2']
        self.assertEqual(swift_command(cmd), ''.join([FILE2_LINE1, FILE2_LINE2]))
        file2_copy = Resource(FILE2_URL)
        file_list = [line for line in file2_copy]
        self.assertEqual(file_list, [FILE2_LINE1, FILE2_LINE2])

    def test_close(self):
        self.file1.write('test')
        self.file1.flush()
        tmp_file_path = os.path.join(self.file1._tmpdir,
                                     self.file1._container,
                                     self.file1._local_path)
        # the local cache exists
        with open(tmp_file_path) as f:
            self.assertEquals(f.read(), 'test')
        # free all resources
        self.file1.close()
        self.assertFalse(os.path.exists(tmp_file_path))

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


