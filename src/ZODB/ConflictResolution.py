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
from cStringIO import StringIO
from cPickle import Unpickler, Pickler
import sys

#import traceback

bad_classes={}
bad_class=bad_classes.has_key

ResolvedSerial='rs'

def _classFactory(location, name,
                  _silly=('__doc__',), _globals={}):
    return getattr(__import__(location, _globals, _globals, _silly),
                   name)

def state(self, oid, serial, prfactory):
    p=self.loadSerial(oid, serial)
    file=StringIO(p)
    unpickler=Unpickler(file)
    unpickler.persistent_load=prfactory
    class_tuple=unpickler.load()
    state=unpickler.load()
    return state


class PersistentReference:

    def __repr__(self):
        return "PR(%s %s)" % (id(self), self.data)

    def __getstate__(self):
        raise "Can't pickle PersistentReference"

class PersistentReferenceFactory:

    data=None
    
    def __call__(self, oid,
                 getattr=getattr, None=None):

        data=self.data
        if not data: data=self.data={}

        r=data.get(oid, None)
        if r is None:
            r=PersistentReference()
            r.data=oid
            data[oid]=r

        return r

def persistent_id(object,
                  PersistentReference=PersistentReference,
                  getattr=getattr
                  ):
    if getattr(object, '__class__', 0) is not PersistentReference:
        return None
    return object.data

def tryToResolveConflict(self, oid, committedSerial, oldSerial, newpickle):
    #class_tuple, old, committed, newstate = ('',''), 0, 0, 0
    try:
        file=StringIO(newpickle)
        unpickler=Unpickler(file)
        prfactory=PersistentReferenceFactory()
        unpickler.persistent_load=prfactory
        class_tuple=unpickler.load()[0]
        if bad_class(class_tuple):
            #sys.stderr.write(' b%s ' % class_tuple[1]); sys.stderr.flush()
            return 0

        newstate=unpickler.load()
        klass=_classFactory(class_tuple[0], class_tuple[1])
        klass._p_resolveConflict                    
        inst=klass.__basicnew__()

        try:
            resolve=inst._p_resolveConflict
        except AttributeError:
            bad_classes[class_tuple]=1
            #traceback.print_exc()
            #sys.stderr.write(' b%s ' % class_tuple[1]); sys.stderr.flush()
            return 0

        old=state(self, oid, oldSerial, prfactory)
        committed=state(self, oid, committedSerial, prfactory)

        resolved=resolve(old, committed, newstate)

        file=StringIO()
        pickler=Pickler(file,1)
        pickler.persistent_id=persistent_id
        pickler.dump(class_tuple)
        pickler.dump(resolved)
        #sys.stderr.write(' r%s ' % class_tuple[1]); sys.stderr.flush()
        return file.getvalue(1)
    except Exception, v:
        #print '='*70
        #print v, v.args
        #print '='*70
        #print old
        #print '='*70
        #print committed
        #print '='*70
        #print newstate
        #print '='*70

        #traceback.print_exc()

        #sys.stderr.write(' c%s ' % class_tuple[1]); sys.stderr.flush()

        return 0

class ConflictResolvingStorage:
    "Mix-in class that provides conflict resolution handling for storages"

    tryToResolveConflict=tryToResolveConflict
