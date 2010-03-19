from setuptools import setup, find_packages

setup(
    name = 'close.consumer',
    version = '0.1',
    description = 'gevent based Streaming API consumer',
    author = 'James Arthur',
    author_email = 'thruflo@googlemail.com',
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
        'redis==1.34.1'
    ],
    entry_points = {
        'console_scripts': [
            'close-consume = close.consumer.consumer:main',
            'close-process = close.consumer.process:main'
        ]
    }
)
