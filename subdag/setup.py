from setuptools import setup

setup(
    name='subdag',
    version='0.2',
    py_modules=['sd'],
    install_requires=[
        'click==7.0',
        'dask[complete]==0.20.0',
        'toolz==0.9.0',
        'mysqlclient==1.3.13',
        'pytest==3.9.3',
    ],
    entry_points='''
        [console_scripts]
        sd=sd:cli
    ''',
)
