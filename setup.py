from distutils.core import setup


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
)
