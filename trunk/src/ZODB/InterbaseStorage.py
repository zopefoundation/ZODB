"""
Interbase ZODB Storage

$Id: InterbaseStorage.py,v 1.3 2000/06/03 06:00:41 chrism Exp $
"""

__version__='$Revision: 1.3 $'[11:-2]

import POSException
import BaseStorage
import kinterbasdb
import binascii
from TimeStamp import TimeStamp
dbiraw = kinterbasdb.dbi.dbiRaw
import string, os, time
import pdb

class RelationalStorageError(POSException.StorageError): pass

class RelationalStorageInitError(RelationalStorageError): pass

class CorruptedRelationalStorageError(RelationalStorageError,
                                      POSException.StorageSystemError):
    pass

class CorruptedDataError(CorruptedRelationalStorageError):
    pass

class InterbaseStorage(BaseStorage.BaseStorage):
    """ Relational Storage class stores ZODB records and transaction
    in a relational DB -- this is the Interbase variety """
    def __init__(self, conn, name="", zodb_data="zodb_data",
                 zodb_trans="zodb_trans", zodb_version="zodb_version"):
        self._debug = 1
        if self._debug:
            print "__init__"
        self._c = conn
        self._tables = {'data': zodb_data,
                        'trans': zodb_trans,
                        'version': zodb_version}
        self._zd = zodb_data
        self._zt = zodb_trans
        self._zv = zodb_version
        self._index = {}
        self._build_queries()
        
        status = self._testConn()
        if status:
            self._closeConn()
            raise RelationalStorageInitError, status

        BaseStorage.BaseStorage.__init__(self, name)

        self._oid = self._last_oid()
        self._index, self._vindex = self._build_index()
        
    def __len__(self):
        return len(self._index)
    
    def _build_index(self, stop='\377\377\377\377\377\377\377\377'):
        if self._debug:
            print "_build_index"
        index = {}
        vindex = {}
        tables = self._tables
        
        recs = self._doCommittedSQL(cmd = "SELECT z_oid, z_serial"
                                    " FROM %(data)s" % tables,
                                    fetch = "all")
        for rec in recs:
            oid, serial = map(unbase64, rec)
            if serial >= stop:  continue
            index[oid] = serial

        recs = self._doCommittedSQL(cmd = "SELECT z_version, z_oid, z_serial"
                                    " FROM %(version)s" % tables,
                                    fetch = "all")
        for rec in recs:
            version, oid, serial = rec
            oid, serial = map(unbase64, (oid, serial))
            if serial >= stop: continue
            v = vindex.get(version, None)
            if v is None: vindex[version] = {}
            vindex[version].update({oid:serial})

        return index, vindex 

    def _last_oid(self):
        if self._debug:
            print "_last_oid"
        zd = self._zd
        recs = self._doCommittedSQL(cmd = "SELECT z_oid from %s" % zd,
                                    fetch = "all")
        oids = []
        for rec in recs:
            oid = unbase64(rec[0])
            oids.append(oid)
        oids.sort()
        try:
            return oids[-1]
        except:
            return '\0\0\0\0\0\0\0\0'
            
    def close(self):
        if self._debug:
            print "_close"
        self._closeConn()

    def supportsVersions(self):
        if self._debug:
            print "supportsVersions"
        return 0

    def supportsUndo(self):
        if self._debug:
            print "supportsUndo"
        return 0

    def load(self, oid, version):
        if self._debug:
            print "load"
        self._lock_acquire()
        try:
            serial = self._index[oid]
            s_serial, s_oid = map(base64, (serial, oid))            
            cmd = self._q_load_w_vdata
            r = self._doCommittedSQL(cmd = cmd,
                                     params = (s_oid, s_serial),
                                     fetch = "one")
            (data, datalen, prev, dataserial, oversion, nv) = r
            if oversion:
                if oversion != version:
                    if nv:
                        cmd = self._q_load
                        r = self.doCommittedSQL(cmd = cmd,
                                                params = (s_oid, s_serial),
                                                fetch = "one")
                        data = r[0][0]
                        
            datalen = int(datalen) # ib loves py longs
            
            if datalen > 0:
                return data.value, serial # data.value is interbaseism
            else:
                cmd = self._q_load
                data = self._doCommittedSQL(cmd = cmd,
                                            params = (s_oid, dataserial),
                                            fetch = "one")[0]
                return data.value, serial # another interbaseism

        finally:
            self._lock_release()
            
    def store(self, oid, serial, data, version, transaction):
        if self._debug:
            print "store"
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            tables = self._tables
            oserial = self._index.get(oid, None)

            s_nv = ""
            
            if oserial is not None:
                if serial != oserial:
                    raise POSException.ConflictError, (serial, oserial)

                s_oid = base64(oid)
                s_oserial = base64(oserial)

                cmd = ("SELECT z_version, z_nv from %(version)s"
                       " WHERE z_oid = ?"
                       " AND z_serial = ?" % tables)
                r = self._doUncommittedSQL(cmd = cmd,
                                           params = (s_oid, s_oserial),
                                           fetch = "one")
                if r:
                    oversion, s_nv = r
                    if version != oversion:
                        raise POSException.VersionLockError, oid
                    
            s_oid = base64(oid)
            s_serial = base64(self._serial)
            s_prev = oserial or ""
            s_data = dbiraw(data)
            s_status = "c"
            s_datalen = len(data)
            s_dataserial = "" # fix
            
            cmd = ("INSERT INTO %(data)s (z_oid, z_serial, z_pre, z_status,"
                   "                z_data, z_datalen, z_dataserial)"
                   " VALUES (?, ?, ?, ?, ?, ?, ?)" % tables)
            
            self._doUncommittedSQL(cmd = cmd,
                                   params = (s_oid,
                                             s_serial,
                                             s_prev,
                                             s_status,
                                             s_data,
                                             s_datalen,
                                             s_dataserial
                                             ))
            
            if version:
                cmd = ("INSERT INTO %(version)s (z_version, z_oid,"
                       " z_serial, z_status, z_nv)"
                       " VALUES (?, ?, ?, ?, ?)" % tables)

                self._doUncommittedSQL(cmd = cmd,
                                       params = (version,
                                                 s_oid,
                                                 s_serial,
                                                 s_status,
                                                 s_nv
                                                 ))
                
            self._index[oid] = self._serial
            return self._serial

        finally:
            self._lock_release()

    def undo(self, transaction_id):
        self._lock_acquire()
        try:
            tables = self._tables
            s_tid = base64(transaction_id)
            cmd = ("SELECT z_serial, z_status, z_user, z_desc, z_ext"
                   " FROM %(trans)s"
                   " WHERE z_serial = ?" % tables)
            rec = self._doCommittedSQL(cmd = cmd,
                                       params = (s_tid,),
                                       fetch = "one")
            if not rec:
                raise POSException.UndoError, 'Invalid undo transaction id'


        finally:
            self._lock_release()
            # finish me

    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            serial = self._index[oid]
            cmd = self._q_load_w_vdata
            s_oid, s_serial = map(base64, (oid, serial))
            r = self._doCommittedSQL(cmd = cmd,
                                     params = (s_oid, s_serial),
                                     fetch = "one")
            (data, datalen, prev, dataserial, version, nv) = r    
            if version:
                return version
            return ''
        finally:
            self._lock_release()

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            oids = []
            tables = self._tables
            cmd = ("SELECT z_oid, z_serial, z_status, z_nv"
                   " FROM %(version)s"
                   " WHERE z_version = ?" % tables)
            recs = self._doUncommittedSQL(cmd = cmd,
                                          params = (src,),
                                          fetch = "all")
            if recs:
                for s_oid, s_serial, status, s_nv in rec:
                    oids.append(unbase64(s_oid))
                    cmd = ("DELETE FROM %(version)s, %(data)s"
                           " WHERE z_oid = ?"
                           " AND z_serial = ?" % tables)
                    self._doUncommittedSQL(cmd = cmd,
                                           params = (s_oid, s_serial))

            return oids
        
        finally:
            self._lock_release()
            
    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            oids = []
            tables = self._tables
            cmd = ("SELECT z_version, z_oid, z_serial, z_status, z_nv"
                   " FROM %(version)s"
                   " WHERE z_version = ?" % tables)
            recs = self._doUncommittedSQL(cmd = cmd,
                                          params = (src,),
                                          fetch = "all")
            if not recs: return
            for version, s_oid, s_serial, status, s_nv in recs:
                oids.append(unbase64(s_oid))
            #unfinished
            return oids
        
        finally:
            self._lock_release()
                
        
    def _begin(self, tid, u, d, e):
        if self._debug:
            print "_begin"
        self._beginSQL()

    def _finish(self, tid, u, d, e):
        if self._debug:
            print "_finish"
        try:
            tables = self._tables
            serial = base64(self._serial)

            cmd = ("INSERT INTO %(trans)s (z_serial, z_status, z_user,"
                   " z_desc, z_ext)"
                   " VALUES (?, ?, ?, ?, ?)" % tables)

            self._doUncommittedSQL(cmd = cmd,
                                   params = (serial, ' ', ' ', ' ', ' '))
            # fix this, there is really supposed to be a description and user
        finally:
            self._commitSQL()
            
    def pack(self, t, referencesf):
        if self._debug:
            print "pack"
        if self._debug:
            print "beginning packing"
        zd = self._zd
        zt = self._zt
        if self._debug:
            print "acquiring lock"
        self._lock_acquire()
        try:
            if self._debug:
                print "getting stop time"
            stop = `apply(TimeStamp, time.gmtime(t)[:5]+(t%60,))`
            if self._debug:
                print "building index"
            index, vindex = self._build_index(stop)
            rootl = ['\0\0\0\0\0\0\0\0']
            pop = rootl.pop
            pindex = {}
            referenced = pindex.has_key
            print "traversing rootl"
            while rootl != []:
                oid = pop()
                if referenced(oid):
                    continue
                serial = index[oid]
                pindex[oid] = serial
                cmd = ("SELECT z_data"
                       " FROM %s WHERE z_serial = ?"
                       " AND z_oid = ?" % zd)
                s_serial, s_oid = map(base64, (serial, oid))
                data = self._doCommittedSQL(cmd = cmd,
                                            params = (s_serial, s_oid),
                                            fetch="one")[0]
                data = data.value #interbaseism
                referencesf(data, rootl)
            if self._debug:
                print "moving on to gather stuff"
            deleted = []
            oids = index.keys()
            oids.sort()
            i = 0
            while i < len(oids):
                oid = oids[i]
                if not referenced(oid):
                    if self._debug:
                        print "appending %s to delete list" % str(i)
                    deleted.append(oid)
                i = i + 1

            pindex=referenced=None 

            if self._debug:
                print "deleting"
            if not deleted:
                print "nothing to delete"
                return # no sense in going any further 

            s_oids = map(base64, deleted)

            oids_in = ""
            for s_oid in s_oids:
                oids_in = oids_in + '\"%s\",' % s_oid
            oids_in = oids_in[:-1]

            cmd = ("SELECT DISTINCT z_serial FROM %s"
                   " WHERE z_oid IN (%s)" % (zd, oids_in))

            s_serials = self._doCommittedSQL(cmd = cmd, fetch = "all")

            serials_in = ""
            for s_serial in s_serials:
                serials_in = serials_in + '\"%s\",' % s_serial[0]
            serials_in = serials_in[:-1]
            
            cmd = ("DELETE FROM %s"
                   " WHERE z_serial IN (%s)" % (zt, serials_in))
                    
            self._beginSQL()
            self._doUncommittedSQL(cmd = cmd)

            cmd = ("DELETE FROM %s"
                   " WHERE z_oid IN (%s)" % (zd, oids_in))

            self._doUncommittedSQL(cmd = cmd)
            self._commitSQL()
                
        finally:
            if self._debug:
                print "finished"
            self._lock_release()

    def getSize(self):
        if self._debug:
            print "getSize"
        return 0 # fix
    
    def _testConn(self):
        """ Tests the DB connection and makes sure the tables are sane """
        if self._debug:
            print "_testConn"

        zd = self._zd
        zt = self._zt
        cmds = ("SELECT COUNT(*) FROM %s" % zd,
                "SELECT COUNT(*) FROM %s" % zt)

        for cmd in cmds:
            try:
                self._doCommittedSQL(cmd = cmd, fetch = "all")
            except:
                return "Failed on %s" % cmd
        
    def _closeConn(self):
        if self._debug:
            print "_closeConn"
        self._c.close()

    def _doSQL(self, cmd="", params=[], fetch=""):
        if self._debug:
            print "_doSQL"
        if cmd == "": return
        c = self._c.cursor()
        try:
            cmd = string.join(string.split(cmd))
            if params:
                c.execute(cmd, params)
            else:
                c.execute(cmd)
            if fetch == "all":
                return c.fetchall()
            if fetch == "one":
                return c.fetchone()
            
        finally:
            c.close()

    def _doUncommittedSQL(self, *args, **kw):
        if self._debug:
            print "_doUncommittedSQL"
        return apply(self._doSQL, args, kw or {})
            
    def _doCommittedSQL(self, *args, **kw):
        if self._debug:
            print "_doCommittedSQL"
        self._beginSQL()
        try:
            return apply(self._doSQL, args, kw or {})
        finally:
            self._commitSQL()
            
    def _beginSQL(self):
        if self._debug:
            print "_beginSQL"
        self._c.begin()
        
    def _commitSQL(self):
        if self._debug:
            print "_commitSQL"
        self._c.commit()

    def _clear_temp(self):
        if self._debug:
            print "_clear_temp"
        #self._abort()

    def _abort(self):
        if self._debug:
            print "_abort"
        self._c.rollback()

    def _build_queries(self):
        tables = self._tables

        self._q_load_w_vdata ="""
           SELECT %(data)s.z_data,
                  %(data)s.z_datalen,
                  %(data)s.z_pre,
                  %(data)s.z_dataserial,
                  %(version)s.z_version,
                  %(version)s.z_nv
           FROM (%(data)s
                 LEFT OUTER JOIN %(version)s
                 ON (%(data)s.z_oid = %(version)s.z_oid
                     AND %(data)s.z_serial = 
                     %(version)s.z_serial))
           WHERE %(data)s.z_oid = ?
                 AND %(data)s.z_serial = ? """ % tables

        self._q_load = """
           SELECT z_data
           FROM %(data)s
           WHERE z_oid = ?
           AND z_serial = ? """ % tables

def _sql_quote(v):
    return '\"%s\"' % v

def base64(v):
    b2a = binascii.b2a_base64
    return b2a(v)[:12]

def unbase64(v):
    a2b = binascii.a2b_base64
    return a2b(v)



