##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
"""Mounted database support

$Id: Mount.py,v 1.5 2000/08/03 19:56:35 shane Exp $"""
__version__='$Revision: 1.5 $'[11:-2]

import Globals, thread, Persistence, Acquisition
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
    _v_close_db = 0
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

    def __repr__(self):
        return "%s %s" % (self.__class__.__name__, self._path)

    def _close(self):
        # The onCloseCallback handler.
        # Closes a single connection to the database
        # and possibly the database itself.
        t = self._v_data
        if t is not None:
            data = t[0]
            if getattr(data, '_v__object_deleted__', 0):
                # This mount point has been deleted.
                del data._v__object_deleted__
                self._v_close_db = 1
            if data is not None:
                conn = data._p_jar
                if conn is not None:
                    try: del conn._mount_parent_jar
                    except: pass
                    conn.close()
            self._v_data = None
        if self._v_close_db:
            # Stop using this database. Close it if no other
            # MountPoint is using it.
            dblock.acquire()
            try:
                self._v_close_db = 0
                self._v_db = None
                params = self._params
                if dbs.has_key(params):
                    dbInfo = dbs[params]
                    db, mounts = dbInfo
                    try: del mounts[self.__mountpoint_id]
                    except: pass
                    if len(mounts) < 1:
                        # No more mount points are using this database.
                        del dbs[params]
                        db.close()
                        LOG('ZODB', INFO, 'Closed database: %s' % params)
            finally:
                dblock.release()

    def __openConnection(self, parent):
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

        try:
            jar.onCloseCallback(self._close)
            obj = self._getMountRoot(conn.root())
            data = getattr(obj, 'aq_base', obj)
            # Store the data object in a tuple to hide from acquisition.
            self._v_data = (data,)
        except:
            # Close the connection before processing the exception.
            del conn._mount_parent_jar
            conn.close()
            raise
        if newMount:
            id = data.id
            if callable(id):
                id = id()
            p = string.join(parent.getPhysicalPath() + (id,), '/')
            LOG('ZODB', INFO, 'Mounted database %s at %s' % \
                (self._params, p))
        return data

    def __of__(self, parent):
        # Accesses the database, returning an acquisition
        # wrapper around the connected object rather than around self.
        t = self._v_data
        if t is None:
            self._v_connect_error = None
            try:
                data = self.__openConnection(parent)
            except:
                self._v_close_db = 1
                self._logConnectException()
                # Broken database. Wrap around self.
                return Acquisition.ImplicitAcquisitionWrapper(self, parent)
        else:
            data = t[0]

        return data.__of__(parent)

    def _test(self, parent):
        '''Tests the database connection.
        '''
        if self._v_data is None:
            try:
                data = self.__openConnection(parent)
            except:
                self._v_close_db = 1
                self._logConnectException()
                raise
        return 1

    def _getMountRoot(self, root):
        '''Gets the object to be mounted.
        Can be overridden to provide different behavior.
        '''
        try:
            app = root['Application']
        except:
            raise MountedStorageError, \
                  'No \'Application\' object exists in the mountable database.'
        try:
            return app.unrestrictedTraverse(self._path)
        except:
            raise MountedStorageError, \
                  ('The path \'%s\' was not found in the mountable database.' \
                   % self._path)

    def _logConnectException(self):
        '''Records info about the exception that just occurred.
        '''
        from cStringIO import StringIO
        import traceback
        exc = sys.exc_info()
        LOG('ZODB', WARNING, 'Failed to mount database. %s (%s)' % exc[:2])
        f=StringIO()
        traceback.print_tb(exc[2], 100, f)
        self._v_connect_error = (exc[0], exc[1], f.getvalue())
