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
"""Implement an bobo_application object that is BoboPOS3 aware

This module provides a wrapper that causes a database connection to be created
and used when bobo publishes a bobo_application object.
"""
__version__='$Revision: 1.7 $'[11:-2]

StringType=type('')

class ZApplicationWrapper:

    def __init__(self, db, name, klass= None, klass_args= (),
                 version_cookie_name=None):
        self._stuff = db, name, version_cookie_name
        if klass is not None:
            conn=db.open()
            root=conn.root()
            if not root.has_key(name):
                root[name]=klass()
                get_transaction().commit()
            conn.close()
            self._klass=klass
        

    # This hack is to overcome a bug in Bobo!
    def __getattr__(self, name):
        return getattr(self._klass, name)

    def __bobo_traverse__(self, REQUEST=None, name=None):
        db, aname, version_support = self._stuff
        if version_support is not None and REQUEST is not None:
            version=REQUEST.get(version_support,'')
        else: version=''
        conn=db.open(version)

        # arrange for the connection to be closed when the request goes away
        cleanup=Cleanup()
        cleanup.__del__=conn.close
        REQUEST._hold(cleanup)

        conn.setDebugInfo(REQUEST.environ, REQUEST.other)
        
        v=conn.root()[aname]

        if name is not None:
            if hasattr(v, '__bobo_traverse__'):
                return v.__bobo_traverse__(REQUEST, name)
            
            if hasattr(v,name): return getattr(v,name)
            return v[name]
        
        return v


    def __call__(self, connection=None):
        db, aname, version_support = self._stuff

        if connection is None:
            connection=db.open()
        elif type(connection) is StringType:
            connection=db.open(connection)
        
        return connection.root()[aname]
    

class Cleanup: pass

