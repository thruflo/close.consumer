from setuptools import setup, find_packages

setup(
    name = 'close.consumer',
    version = '0.2.1',
    description = 'gevent based Streaming API consumer',
    long_description = open('README.rst').read(),
    author = 'James Arthur',
    author_email = 'thruflo@googlemail.com',
    url = 'http://github.com/thruflo/close.consumer',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: Public Domain',
        'Programming Language :: Python'
    ],
    license = 'Creative Commons CC0 1.0 Universal',
    packages = ['close.consumer'],
    package_dir = {'': 'src'},
    include_package_data = True,
    zip_safe = False,
    namespace_packages = [
        'close'
    ],
    install_requires=[
        'greenlet==0.2',
        'gevent==0.12.2',
        #'redis==1.34.1'
    ],
    entry_points = {
        'console_scripts': [
            'close-consume = close.consumer.consumer:main',
            'close-process = close.consumer.process:main'
        ]
    }
)
