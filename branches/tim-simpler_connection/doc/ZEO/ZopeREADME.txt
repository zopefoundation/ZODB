Zope Enterprise Objects (ZEO)

  Installation

    ZEO 2.0 requires Zope 2.4 or higher and Python 2.1 or higher.
    If you use Python 2.1, we recommend the latest minor release
    (2.1.3 as of this writing) because it includes a few bug fixes
    that affect ZEO.

    Put the package (the ZEO directory, without any wrapping directory
    included in a distribution) in your Zope lib/python.

    The setup.py script in the top-level ZEO directory can also be
    used.  Run "python setup.py install --home=ZOPE" where ZOPE is the
    top-level Zope directory.

    You can test ZEO before installing it with the test script::

      python test.py -v

    Run the script with the -h option for a full list of options.  The
    ZEO 2.0b2 release contains 122 unit tests on Unix.

  Starting (and configuring) the ZEO Server

    To start the storage server, go to your Zope install directory and
    run::

      python lib/python/ZEO/start.py -p port_number

    This run the storage sever under zdaemon.  zdaemon automatically
    restarts programs that exit unexpectedly.

    The server and the client don't have to be on the same machine.
    If they are on the same machine, then you can use a Unix domain
    socket::

      python lib/python/ZEO/start.py -U filename

    The start script provides a number of options not documented here.
    See doc/start.txt for more information.
        
  Running Zope as a ZEO client

    To get Zope to use the server, create a custom_zodb module,
    custom_zodb.py, in your Zope install directory, so that Zope uses a
    ClientStorage::

      from ZEO.ClientStorage import ClientStorage
      Storage = ClientStorage(('', port_number))

    You can specify a host name (rather than '') if you want.  The port
    number is, of course, the port number used to start the storage
    server.

    You can also give the name of a Unix domain socket file::

      from ZEO.ClientStorage import ClientStorage
      Storage = ClientStorage(filename)

    There are a number of configuration options available for the
    ClientStorage. See doc/ClientStorage.txt for details.

    If you want a persistent client cache which retains cache contents
    across ClientStorage restarts, you need to define the environment
    variable, ZEO_CLIENT, or set the client keyword argument to the
    constructor to a unique name for the client.  This is needed so
    that unique cache name files can be computed.  Otherwise, the
    client cache is stored in temporary files which are removed when
    the ClientStorage shuts down.  For example, to start two Zope
    processes with unique caches, use something like::

      python z2.py -P8700 ZEO_CLIENT=8700
      python z2.py -P8800 ZEO_CLIENT=8800

  Zope product installation

    Normally, Zope updates the Zope database during startup to reflect
    product changes or new products found. It makes no sense for
    multiple ZEO clients to do the same installation. Further, if
    different clients have different software installed, the correct
    state of the database is ambiguous.

    Zope will not modify the Zope database during product installation
    if the environment variable ZEO_CLIENT is set.

    Normally, Zope ZEO clients should be run with ZEO_CLIENT set so
    that product initialization is not performed.

    If you do install new Zope products, then you need to take a
    special step to cause the new products to be properly registered
    in the database.  The easiest way to do this is to start Zope
    once with the environment variable FORCE_PRODUCT_LOAD set.

    The interaction between ZEO and Zope product installation is
    unfortunate.
