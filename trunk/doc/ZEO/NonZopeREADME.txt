Zope Enterprize Objects

  Put this package (the ZEO directory, without any wrapping directory
  included in a distribution) in your python path.

  Starting (and configuring) the ZEO Server

    To start the storage server, go to your Zope install directory and::

      python ZEO/start.py -p port_number

    (Run start without arguments to see options.)

    Of course, the server and the client don't have to be on the same
    machine.

    If the server and client *are* on the same machine, then you can use 
    a Unix domain socket::

      python ZEO/start.py -U filename

    The start script provides a number of options not documented here.
    See doc/start.txt for more information.
        
  Running a ZEO client

    In your application, create a ClientStorage, rather than, say, a
    FileStorage:

      import ZODB, ZEO.ClientStorage
      Storage=ZEO.ClientStorage.ClientStorage(('',port_number))
      db=ZODB.DB(Storage)

    You can specify a host name (rather than '') if you want.  The port
    number is, of course, the port number used to start the storage
    server.

    You can also give the name of a Unix domain socket file::

      import ZODB, ZEO.ClientStorage
      Storage=ZEO.ClientStorage.ClientStorage(filename)
      db=ZODB.DB(Storage)

    There are a number of configuration options available for the
    ClientStorage. See doc/ClientStorage.txt for details.

    If you want a persistent client cache which retains cache contents
    across ClientStorage restarts, you need to define the environment
    variable, ZEO_CLIENT, to a unique name for the client.  This is
    needed so that unique cache name files can be computed.  Otherwise,
    the client cache is stored in temporary files which are removed when
    the ClientStorage shuts down.

  Dependencies on other modules

      - The module, ThreadedAsync must be in the python path.

      - The zdaemon module is necessary if you want to run your
        storage server as a daemon that automatically restarts itself
        if there is a fatal error.

      - The zLOG module provides a handy logging capability.

      If you are using a version of Python before Python 2:

        - ZServer should be in the Python path, or you should copy the
          version of asyncore.py from ZServer (from Zope 2.2 or CVS) to
          your Python path, or you should copy a version of a asyncore
          from the medusa CVS tree to your Python path. A recent change
          in asyncore is required.

        - The version of cPickle from Zope, or from the python.org CVS
          tree must be used. It has a hook to provide control over which
          "global objects" (e.g. classes) may be pickled.
