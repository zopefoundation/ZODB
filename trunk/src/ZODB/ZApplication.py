##############################################################################
#
# Copyright (c) 1996-1998, Digital Creations, Fredericksburg, VA, USA.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   o Redistributions of source code must retain the above copyright
#     notice, this list of conditions, and the disclaimer that follows.
# 
#   o Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions, and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
# 
#   o Neither the name of Digital Creations nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
# 
# 
# THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS IS*
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
# CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# 
# If you have questions regarding this software, contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
##############################################################################
"""Implement an bobo_application object that is BoboPOS3 aware

This module provides a wrapper that causes a database connection to be created
and used when bobo publishes a bobo_application object.
"""
__version__='$Revision: 1.1 $'[11:-2]

class BoboApplication:

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
        REQUEST[Cleanup]=cleanup
        
        v=conn.root()[aname]

        if name is not None:
            if hasattr(v, '__bobo_traverse__'):
                return v.__bobo_traverse__(REQUEST, name)
            
            if hasattr(v,name): return getattr(v,name)
            return v[name]
        
        return v

    __call__=__bobo_traverse__ # A convenience for command-line use

    

class Cleanup: pass

