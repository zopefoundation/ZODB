#!/usr/local/bin/python 
# $What$

__doc__='''A very simple HTTP transaction manager.

This module provides a very simple transaction manager for
HTTP requests in a single-threaded application environment.
(Future versions of this module may support multiple transactions.)

This module treats each HTTP request as a transaction.

To use, import the module and then call the 'install' function.
This will install the function 'get_transaction'.  This function will
be used by transactional objects to get the current transaction.

$Id: Transaction.py,v 1.2 1997/10/30 20:23:26 brian Exp $'''
#     Copyright 
#
#       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
#       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
#       rights reserved.  Copyright in this software is owned by DCLC,
#       unless otherwise indicated. Permission to use, copy and
#       distribute this software is hereby granted, provided that the
#       above copyright notice appear in all copies and that both that
#       copyright notice and this permission notice appear. Note that
#       any product, process or technology described in this software
#       may be the subject of other Intellectual Property rights
#       reserved by Digital Creations, L.C. and are not licensed
#       hereunder.
#
#     Trademarks 
#
#       Digital Creations & DCLC, are trademarks of Digital Creations, L.C..
#       All other trademarks are owned by their respective companies. 
#
#     No Warranty 
#
#       The software is provided "as is" without warranty of any kind,
#       either express or implied, including, but not limited to, the
#       implied warranties of merchantability, fitness for a particular
#       purpose, or non-infringement. This software could include
#       technical inaccuracies or typographical errors. Changes are
#       periodically made to the software; these changes will be
#       incorporated in new editions of the software. DCLC may make
#       improvements and/or changes in this software at any time
#       without notice.
#
#     Limitation Of Liability 
#
#       In no event will DCLC be liable for direct, indirect, special,
#       incidental, economic, cover, or consequential damages arising
#       out of the use of or inability to use this software even if
#       advised of the possibility of such damages. Some states do not
#       allow the exclusion or limitation of implied warranties or
#       limitation of liability for incidental or consequential
#       damages, so the above limitation or exclusion may not apply to
#       you.
#  
#
# If you have questions regarding this software,
# contact:
#
#   Jim Fulton, jim@digicool.com
#
#   (540) 371-6909
#
# $Log: Transaction.py,v $
# Revision 1.2  1997/10/30 20:23:26  brian
# Fixed thread dependency
#
# Revision 1.1  1997/04/11 21:42:52  jim
# *** empty log message ***
#
# Revision 1.5  1997/03/25 20:42:58  jim
# Changed to make all persistent objects transactional.
#
# Revision 1.4  1997/02/11 13:23:14  jim
# Many changes to support both cPickle and LRT.
#
# Revision 1.3  1996/11/14 17:49:38  jim
# Removed c-implementation support.
#
# Revision 1.2  1996/10/15 18:32:25  jim
# Added support for cSingleThreadedTransaction
#
# Revision 1.1  1996/09/06 14:35:54  jfulton
# For use with Chris doing installation
#
#
# 
__version__='$Revision: 1.2 $'[11:-2]

# Install myself before anything else happens:

# This both gets everything STT has *and* arranges for our
# get_transaction to override their's
from SingleThreadedTransaction import *

import SingleThreadedTransaction

try:
    import thread
    get_id=thread.get_ident
except:
    def get_id(): return 0

theTransactions={}

def get_transaction():
    id=get_id()
    try: theTransaction=theTransactions[id]
    except KeyError: theTransactions[id]=theTransaction=Transaction()
    return theTransaction

import __main__ 
__main__.__builtins__.get_transaction=get_transaction

class Transaction(SingleThreadedTransaction.Transaction):
    '''\
    Simple transaction objects.

    '''

    def abort(self):
	'''\
	Abort the transaction.

	All objects participating in the current transaction will be
	informed of the abort so that they can roll back changes or
	forget pending changes.
	'''
	self._abort()
	id=get_id()
	del theTransactions[id]




