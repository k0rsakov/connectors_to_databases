from io import open
from setuptools import setup


version = '1.1.1'

with open('README.md', encoding='utf-8-sig') as f:
    long_description = f.read()

setup(
    name='connectors_to_databases',
    version=version,

    author='k0rsakov',
    author_email='korsakov.iyu@gmail.com',

    description=(
        u'Python module for connect with PostgreSQL and ClickHouse '
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',

    url='https://github.com/k0rsakov/connectors_to_databases',
    download_url='https://github.com/k0rsakov/connectors_to_databases/archive/refs/heads/main.zip',

    license='Apache License, Version 2.0, see LICENSE file',
    
    # TODO: deprecated psycopg2, clickhouse-sqlalchemy
    packages=['connectors_to_databases'],
    install_requires=[
        'SQLAlchemy==1.4.44',
        'pandas==1.5.1',
        'psycopg2-binary==2.9.5',
        # 'psycopg2==2.9.5',
        'clickhouse-sqlalchemy==0.2.3',
        'clickhouse_driver==0.2.5',
        'ruff==0.0.278',
    ],

    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    
    python_requires='>=3.9',
    keywords=['database', 'SQL', 'PostgreSQL', 'ClickHouse', 'dataframe'],
)
