##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
"""Mounted database support

$Id: Mount.py,v 1.12 2001/12/04 23:04:14 shane Exp $"""
__version__='$Revision: 1.12 $'[11:-2]

import thread, Persistence, Acquisition
import ExtensionClass, string, time, sys
from POSException import MountedStorageError
from zLOG import LOG, ERROR, INFO, WARNING

# dbs is a holder for all DB objects, needed to overcome
# threading issues.  It maps connection params to a DB object
# and a mapping of mount points.
dbs = {}

# dblock is locked every time dbs is accessed.
dblock=thread.allocate_lock()

try:
    # Make special provisions for ZClasses if we're in a Zope
    # installation.
    from Zope.ClassFactory import ClassFactory
    
    def RootDefsClassFactory(jar, module, name):
        # Use the class definitions given at
        # the root of the Zope installation.
        while hasattr(jar, '_mount_parent_jar'):
            jar = jar._mount_parent_jar
        return ClassFactory(jar, module, name)
except:
    ClassFactory = None
    RootDefsClassFactory = None


class MountPoint(Persistence.Persistent, Acquisition.Implicit):
    '''The base class for a Zope object which, when traversed,
    accesses a different database.
    '''

    # Default values for non-persistent variables.
    _v_db = None
    _v_data = None
    _v_connect_error = None

    def __init__(self, path, params=None, classDefsFromRoot=1):
        '''
        @arg path The path within the mounted database from which
        to derive the root.

        @arg params The parameters used to connect to the database.
        No particular format required.
        If there is more than one mount point referring to a
        database, MountPoint will detect the matching params
        and use the existing database.  Include the class name of
        the storage.  For example,
        ZEO params might be "ZODB.ZEOClient localhost 1081".

        @arg classDefsFromRoot If true (the default), MountPoint will
        try to get ZClass definitions from the root database rather
        than the mounted database.
        '''
        # The only reason we need a __mountpoint_id is to
        # be sure we don't close a database prematurely when
        # it is mounted more than once and one of the points
        # is unmounted.
        self.__mountpoint_id = '%s_%f' % (id(self), time.time())
        if params is None:
            # We still need something to use as a hash in
            # the "dbs" dictionary.
            params = self.__mountpoint_id
        self._params = repr(params)
        self._path = path
        self._classDefsFromRoot = classDefsFromRoot

    def _createDB(self):
        '''Gets the database object, usually by creating a Storage object
        and returning ZODB.DB(storage).
        '''
        raise 'NotImplemented'

    def _getDB(self):
        '''Creates or opens a DB object.
        '''
        newMount = 0
        dblock.acquire()
        try:
            params = self._params
            dbInfo = dbs.get(params, None)
            if dbInfo is None:
                LOG('ZODB', INFO, 'Opening database for mounting: %s' % params)
                db = self._createDB()
                newMount = 1
                dbs[params] = (db, {self.__mountpoint_id:1})

                if RootDefsClassFactory is not None and \
                   getattr(self, '_classDefsFromRoot', 1):
                    db.setClassFactory(RootDefsClassFactory)
                elif ClassFactory is not None:
                    db.setClassFactory(ClassFactory)
            else:
                db, mounts = dbInfo
                # Be sure this object is in the list of mount points.
                if not mounts.has_key(self.__mountpoint_id):
                    newMount = 1
                    mounts[self.__mountpoint_id] = 1
            self._v_db = db
        finally:
            dblock.release()
        return db, newMount

    def _getMountpointId(self):
        return self.__mountpoint_id

    def _getMountParams(self):
        return self._params

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, repr(self._path),
                               self._params)

    def _openMountableConnection(self, parent):
        # Opens a new connection to the database.
        db = self._v_db
        if db is None:
            self._v_close_db = 0
            db, newMount = self._getDB()
        else:
            newMount = 0
        jar = getattr(self, '_p_jar', None)
        if jar is None:
            # Get _p_jar from parent.
            self._p_jar = jar = parent._p_jar
        conn = db.open(version=jar.getVersion())

        # Add an attribute to the connection which
        # makes it possible for us to find the primary
        # database connection.  See ClassFactoryForMount().
        conn._mount_parent_jar = jar

        mcc = MountedConnectionCloser(self, conn)
        jar.onCloseCallback(mcc)
        return conn, newMount, mcc

    def _getObjectFromConnection(self, conn):
        obj = self._getMountRoot(conn.root())
        data = getattr(obj, 'aq_base', obj)
        # Store the data object in a tuple to hide from acquisition.
        self._v_data = (data,)
        return data

    def _getOrOpenObject(self, parent):
        t = self._v_data
        if t is None:
            self._v_connect_error = None
            conn = None
            newMount = 0
            mcc = None
            try:
                conn, newMount, mcc = self._openMountableConnection(parent)
                data = self._getObjectFromConnection(conn)
            except:
                # Possibly broken database.
                if mcc is not None:
                    # Note that the next line may be a little rash--
                    # if, for example, a working database throws an
                    # exception rather than wait for a new connection,
                    # this will likely cause the database to be closed
                    # prematurely.  Perhaps DB.py needs a
                    # countActiveConnections() method.
                    mcc.setCloseDb()
                self._logConnectException()
                raise
            if newMount:
                try: id = data.getId()
                except: id = '???'  # data has no getId() method.  Bad.
                p = string.join(parent.getPhysicalPath() + (id,), '/')
                LOG('ZODB', INFO, 'Mounted database %s at %s' %
                    (self._getMountParams(), p))
        else:
            data = t[0]

        return data.__of__(parent)
        
    def __of__(self, parent):
        # Accesses the database, returning an acquisition
        # wrapper around the connected object rather than around self.
        try:
            return self._getOrOpenObject(parent)
        except:
            return Acquisition.ImplicitAcquisitionWrapper(
                self, parent)

    def _test(self, parent):
        '''Tests the database connection.
        '''
        self._getOrOpenObject(parent)
        return 1

    def _getMountRoot(self, root):
        '''Gets the object to be mounted.
        Can be overridden to provide different behavior.
        '''
        try:
            app = root['Application']
        except:
            raise MountedStorageError, (
                "No 'Application' object exists in the mountable database.")
        try:
            return app.unrestrictedTraverse(self._path)
        except:
            raise MountedStorageError, (
                "The path '%s' was not found in the mountable database."
                % self._path)

    def _logConnectException(self):
        '''Records info about the exception that just occurred.
        '''
        try:
            from cStringIO import StringIO
        except:
            from StringIO import StringIO
        import traceback
        exc = sys.exc_info()
        LOG('ZODB', WARNING, 'Failed to mount database. %s (%s)' % exc[:2],
            error=exc)
        f=StringIO()
        traceback.print_tb(exc[2], 100, f)
        self._v_connect_error = (exc[0], exc[1], f.getvalue())
        exc = None


class MountedConnectionCloser:
    '''Closes the connection used by the mounted database
    while performing other cleanup.
    '''
    close_db = 0

    def __init__(self, mountpoint, conn):
        # conn is the child connection.
        self.mp = mountpoint
        self.conn = conn

    def setCloseDb(self):
        self.close_db = 1

    def __call__(self):
        # The onCloseCallback handler.
        # Closes a single connection to the database
        # and possibly the database itself.
        conn = self.conn
        close_db = 0
        if conn is not None:
            mp = self.mp
            # Remove potential circular references.
            self.conn = None
            self.mp = None
            # Detect whether we should close the database.
            close_db = self.close_db
            t = mp.__dict__.get('_v_data', None)
            if t is not None:
                del mp.__dict__['_v_data']
                data = t[0]
                if not close_db and data.__dict__.get(
                    '_v__object_deleted__', 0):
                    # This mount point has been deleted.
                    del data.__dict__['_v__object_deleted__']
                    close_db = 1
            # Close the child connection.
            try: del conn._mount_parent_jar
            except: pass
            conn.close()
        
        if close_db:
            # Stop using this database. Close it if no other
            # MountPoint is using it.
            dblock.acquire()
            try:
                params = mp._getMountParams()
                mp._v_db = None
                if dbs.has_key(params):
                    dbInfo = dbs[params]
                    db, mounts = dbInfo
                    try: del mounts[mp._getMountpointId()]
                    except: pass
                    if len(mounts) < 1:
                        # No more mount points are using this database.
                        del dbs[params]
                        db.close()
                        LOG('ZODB', INFO, 'Closed database: %s' % params)
            finally:
                dblock.release()
