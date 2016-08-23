name = 'zodbdocumentationtests'
version = '0'

from setuptools import setup

setup(
    name = name,
    version = version,
    author = "Jim Fulton",
    author_email = "jim@jimfulton.info",
    description = "ZODB documentation tests",
    packages = [name],
    package_dir = {'':'.'},
    install_requires = ['manuel', 'six', 'zope.testing', 'ZODB'],
    )
