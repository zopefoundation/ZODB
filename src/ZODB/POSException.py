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
'''BoboPOS-defined exceptions

$Id: POSException.py,v 1.1 1998/11/11 02:00:55 jim Exp $'''
__version__='$Revision: 1.1 $'[11:-2]


class POSError(Exception):
    """Persistent object system error
    """

class TransactionError(POSError):
    """An error occured due to normal transaction processing
    """

class ConflictError(TransactionError):
    """Two transactions tried to modify the same object at once

    This transaction should be resubmitted.
    """

class VersionError(POSError):
    """An error in handling versions occurred
    """

class VersionCommitError(VersionError):
    """An invalid combination of versions was used in a version commit
    """

class VersionLockError(VersionError, TransactionError):
    """An attempt was made to modify an object that has
    been modified in an unsaved version"""

class UndoError(POSError):
    """An attempt was made to undo an undoable transaction.
    """

class StorageError(POSError):
    pass

class StorageTransactionError(StorageError):
    """An operation was invoked for an invalid transaction or state
    """

class StorageSystemError(StorageError):
    """Panic! Internal storage error!
    """

