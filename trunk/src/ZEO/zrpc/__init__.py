##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
# zrpc is a package with the following modules
# client -- manages connection creation to remote server
# connection -- object dispatcher
# log -- logging helper
# error -- exceptions raised by zrpc
# marshal -- internal, handles basic protocol issues
# server -- manages incoming connections from remote clients
# smac -- sized message async connections
# trigger -- medusa's trigger

# zrpc is not an advertised subpackage of ZEO; its interfaces are internal
