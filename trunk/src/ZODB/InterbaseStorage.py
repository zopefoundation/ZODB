"""
Interbase ZODB Storage

$Id: InterbaseStorage.py,v 1.1 2000/05/27 08:12:06 chrism Exp $
"""

__version__='$Revision: 1.1 $'[11:-2]

import POSException
import BaseStorage
import kinterbasdb
import binascii
from TimeStamp import TimeStamp
dbiraw = kinterbasdb.dbi.dbiRaw
import string, os, time

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
        self._zd = zodb_data
        self._zt = zodb_trans
        self._zv = zodb_version
        self._index = {}
        status = self._testConn()
        if status:
            self._closeConn()
            raise RelationalStorageInitError, status

        BaseStorage.BaseStorage.__init__(self, name)

        self._oid = self._last_oid()
        self._index = self._build_index()

    def __len__(self):
        return len(self._index)
    
    def _build_index(self, stop='\377\377\377\377\377\377\377\377'):
        if self._debug:
            print "_build_index"
        index = {}
        zd = self._zd
        recs = self._doCommittedSQL("SELECT zoid, zserial"
                                    " FROM %s" % zd)
        for rec in recs:
            oid, serial = map(unbase64, rec)
            if serial >= stop:  continue
            index[oid] = serial
        return index 

    def _last_oid(self):
        if self._debug:
            print "_last_oid"
        zd = self._zd
        recs = self._doCommittedSQL("SELECT zoid from %s" % zd)
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
            zd = self._zd
            s_serial, s_oid = map(base64, (self._index[oid], oid))            
            cmd = ("SELECT zoid, zserial,"
                   "       zdata, zdatalen, zprev,"
                   "       zdataserial"
                   " FROM %s"
                   " WHERE zoid = ?"
                   " AND zserial = ?" % zd)

            (doid, serial, data,
             datalen, prev,
             dataserial) = self._doCommittedSQL(cmd, s_oid, s_serial)[0]
            
            doid, serial, prev, dataserial=map(unbase64, (doid, serial,
                                                          prev, dataserial))
            
            datalen = int(datalen) # ib loves py longs
            
            if doid != oid:
                raise CorruptedDataError, "%s should match %s" % (doid,
                                                                  oid)
            # the previous block is no longer necessary, I'm leaving it
            # in just to see what happens :-)
            
            if datalen > 0:
                return data.value, serial # data.value is interbaseism
            else:
                cmd = "SELECT data FROM %s WHERE serial = ?" % zd
                data_pickle = self._doCommittedSQL(cmd, dataserial)[0][0]
                return data_pickle.value, serial # another interbaseism

        finally:
            self._lock_release()
            
    def store(self, oid, serial, data, version, transaction):
        if self._debug:
            print "store"
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise POSException.Unsupported, "Versions aren't supported"

        self._lock_acquire()
        try:
            zd = self._zd
            oserial = self._index.get(oid, None)
        
            if oserial is not None:
                if serial != oserial:
                    raise POSException.ConflictError, (serial, oserial)

            s_oid = base64(oid)
            s_serial = base64(self._serial)
            s_prev = "" # fix
            s_data = dbiraw(data)
            s_status = "c"
            s_datalen = len(data)
            s_dataserial = "" # fix
            
            cmd = ("INSERT INTO %s (zoid, zserial, zprev, zstatus,"
                   "                zdata, zdatalen, zdataserial)"
                   " VALUES (?, ?, ?, ?, ?, ?, ?)" % zd)
            
            self._doUncommittedSQL(cmd, s_oid, s_serial, s_prev,
                                   s_status, s_data, s_datalen,
                                   s_dataserial)
            self._index[oid] = self._serial
            return self._serial

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
            zt = self._zt
            serial = base64(self._serial)

            cmd = ("INSERT INTO %s (zserial, zstatus, zusername,"
                   " zdescription, zext)"
                   " VALUES (?, ?, ?, ?, ?)" % zt)

            self._doUncommittedSQL(cmd, serial, ' ', ' ', ' ', ' ')
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
            index = self._build_index(stop)
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
                cmd = ("SELECT zdata"
                       " FROM %s WHERE zserial = ?"
                       " AND zoid = ?" % zd)
                s_serial, s_oid = map(base64, (serial, oid))
                data = self._doCommittedSQL(cmd, s_serial, s_oid)[0][0]
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

            cmd = ("SELECT DISTINCT zserial FROM %s"
                   " WHERE zoid IN (%s)" % (zd, oids_in))

            s_serials = self._doCommittedSQL(cmd)

            serials_in = ""
            for s_serial in s_serials:
                serials_in = serials_in + '\"%s\",' % s_serial[0]
            serials_in = serials_in[:-1]
            
            cmd = ("DELETE FROM %s"
                   " WHERE zserial IN (%s)" % (zt, serials_in))
                    
            self._beginSQL()
            self._doUncommittedSQL(cmd)

            cmd = ("DELETE FROM %s"
                   " WHERE zoid IN (%s)" % (zd, oids_in))

            self._doUncommittedSQL(cmd)
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
                self._doCommittedSQL(cmd)
            except:
                return "Failed on %s" % cmd
        
    def _closeConn(self):
        if self._debug:
            print "_closeConn"
        self._c.close()

    def _doUncommittedSQL(self, cmd, *args):
        if self._debug:
            print "_doUncommittedSQL"
        c = self._c.cursor()
        try:
            cmd = string.join(string.split(cmd))
            if self._debug:
                print cmd, `args`
            if args:
                c.execute(cmd, args)
            else:
                c.execute(cmd)
            if string.find(cmd, 'SELECT') == 0: # this sucks, but is necessary
                return c.fetchall() # kinterbasdb chokes on trying to fetch
            # from a non-query call
            
        finally:
            c.close()

    def _doCommittedSQL(self, cmd, *args):
        if self._debug:
            print "_doCommittedSQL"
        self._beginSQL()
        c = self._c.cursor()
        try:
            cmd = string.join(string.split(cmd))
            if self._debug:
                print cmd, `args`
            if args:
                c.execute(cmd, args)
            else:
                c.execute(cmd)
            if string.find(cmd, 'SELECT') == 0:
                return c.fetchall()
            
        finally:
            c.close()
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

def _sql_quote(v):
    return '\"%s\"' % v

def base64(v):
    b2a = binascii.b2a_base64
    return b2a(v)[:12]

def unbase64(v):
    a2b = binascii.a2b_base64
    return a2b(v)




