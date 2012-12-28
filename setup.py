from distutils.core import setup, Command


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def run(self):
        import subprocess
        subprocess.call('nosetests --with-doctest anywhere', shell=True)

    def finalize_options(self):
        pass

setup(
    name='anywhere',
    version='0.1.0',
    author='Greg Leclercq',
    author_email='greg@0x80.net',
    packages=['anywhere',
              'anywhere.resource',
              'anywhere.resource.handler'],
    url='http://pypi.python.org/pypi/anywhere/',
    license='LICENSE.txt',
    description='Handle files located anywhere through multiple protocols '
                '(filesystem, ssh, s3)',
    long_description=open('README.md').read(),
    cmdclass={'test': TestCommand}
)
