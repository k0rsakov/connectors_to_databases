from io import open
from setuptools import setup

"""
:authors: k0rsakov
:license: Apache License, Version 2.0, see LICENSE file
:copyright: (c) 2022 k0rsakov
"""

version = '0.0.3'

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='connectors_to_databases',
    version=version,

    author='k0rsakov',
    author_email='korsakov.iyu@gmail.com',

    description=(
        u'Python module for connect with PostgreSQL '
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',

    url='https://github.com/k0rsakov/connectors_to_databases',
    download_url='https://github.com/k0rsakov/connectors_to_databases/archive/refs/heads/main.zip',

    license='Apache License, Version 2.0, see LICENSE file',

    packages=['connectors_to_databases'],
    install_requires=['SQLAlchemy', 'pandas', 'psycopg2'],

    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
    ]
)
