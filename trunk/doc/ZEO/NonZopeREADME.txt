Zope Enterprize Objects

  Installation

    ZEO 2.0 requires Python 2.1 or higher when used without Zope.  If
    you use Python 2.1, we recommend the latest minor release (2.1.3 as
    of this writing) because it includes a few bug fixes that affect
    ZEO.

    ZEO is packaged with distutils.  To install it, run this command
    from the top-level ZEO directory::

      python setup.py install

    The setup script will install the ZEO package in your Python
    site-packages directory.

    You can test ZEO before installing it with the test script::

      python test.py -v

    Run the script with the -h option for a full list of options.  The
    ZEO 2.0a1 release contains 87 unit tests on Unix.

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

  Running a ZEO client

    In your application, create a ClientStorage, rather than, say, a
    FileStorage:

      import ZODB
      from ZEO.ClientStorage import ClientStorage
      Storage = ClientStorage(('', port_number))
      db = ZODB.DB(Storage)

    You can specify a host name (rather than '') if you want.  The port
    number is, of course, the port number used to start the storage
    server.

    You can also give the name of a Unix domain socket file::

      import ZODB
      from ZEO.ClientStorage import ClientStorage
      Storage = ClientStorage(filename)
      db = ZODB.DB(Storage)

    There are a number of configuration options available for the
    ClientStorage. See ClientStorage.txt for details.

    If you want a persistent client cache which retains cache contents
    across ClientStorage restarts, you need to define the environment
    variable, ZEO_CLIENT, or set the client keyword argument to the
    constructor to a unique name for the client.  This is needed so
    that unique cache name files can be computed.  Otherwise, the
    client cache is stored in temporary files which are removed when
    the ClientStorage shuts down.

  Dependencies on other modules

    ZEO depends on other modules that are distributed with
    StandaloneZODB and with Zope.  You can download StandaloneZODB
    from http://www.zope.org/Products/StandaloneZODB.
