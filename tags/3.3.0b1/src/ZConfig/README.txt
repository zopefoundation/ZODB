This is ZConfig.

ZConfig is a configuration library intended for general use.  It
supports a hierarchical schema-driven configuration model that allows
a schema to specify data conversion routines written in Python.
ZConfig's model is very different from the model supported by the
ConfigParser module found in Python's standard library, and is more
suitable to configuration-intensive applications.

ZConfig schema are written in an XML-based language and are able to
"import" schema components provided by Python packages.  Since
components are able to bind to conversion functions provided by Python
code in the package (or elsewhere), configuration objects can be
arbitrarily complex, with values that have been verified against
arbitrary constraints.  This makes it easy for applications to
separate configuration support from configuration loading even with
configuration data being defined and consumed by a wide range of
separate packages.

ZConfig is licensed under the Zope Public License, version 2.0.  See
the file LICENSE.txt in the distribution for the full license text.

Reference documentation is available in the ZConfig/doc/ directory.

Information on the latest released version of the ZConfig package is
available at

  http://www.zope.org/Members/fdrake/zconfig/

You may either create an RPM and install this, or install directly from
the source distribution.


Creating RPMS:

  python setup.py bdist_rpm

  If you need to force the Python interpreter to, for example, python2:

    python2 setup.py bdist_rpm --python=python2


Installation from the source distribution:

    python setup.py install

  To install to a user's home-dir:
    python setup.py install --home=<dir>

  To install to another prefix (eg. /usr/local)
    python setup.py install --prefix=/usr/local

  If you need to force the python interpreter to eg. python2:
    python2 setup.py install

  For more information please refer to
    http://www.python.org/doc/current/inst/inst.html
