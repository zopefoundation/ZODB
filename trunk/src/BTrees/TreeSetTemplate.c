/*****************************************************************************
  
  Zope Public License (ZPL) Version 1.0
  -------------------------------------
  
  Copyright (c) Digital Creations.  All rights reserved.
  
  This license has been certified as Open Source(tm).
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
  
  1. Redistributions in source code must retain the above copyright
     notice, this list of conditions, and the following disclaimer.
  
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions, and the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.
  
  3. Digital Creations requests that attribution be given to Zope
     in any manner possible. Zope includes a "Powered by Zope"
     button that is installed by default. While it is not a license
     violation to remove this button, it is requested that the
     attribution remain. A significant investment has been put
     into Zope, and this effort will continue if the Zope community
     continues to grow. This is one way to assure that growth.
  
  4. All advertising materials and documentation mentioning
     features derived from or use of this software must display
     the following acknowledgement:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     In the event that the product being advertised includes an
     intact Zope distribution (with copyright and license included)
     then this clause is waived.
  
  5. Names associated with Zope or Digital Creations must not be used to
     endorse or promote products derived from this software without
     prior written permission from Digital Creations.
  
  6. Modified redistributions of any form whatsoever must retain
     the following acknowledgment:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     Intact (re-)distributions of any official Zope release do not
     require an external acknowledgement.
  
  7. Modifications are encouraged but must be packaged separately as
     patches to official Zope releases.  Distributions that do not
     clearly separate the patches from the original work must be clearly
     labeled as unofficial distributions.  Modifications which do not
     carry the name Zope may be packaged in any form, as long as they
     conform to all of the clauses above.
  
  
  Disclaimer
  
    THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
    EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
    USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
    OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
    SUCH DAMAGE.
  
  
  This software consists of contributions made by Digital Creations and
  many individuals on behalf of Digital Creations.  Specific
  attributions are listed in the accompanying credits file.
  
 ****************************************************************************/

static PyObject *
TreeSet_insert(BTree *self, PyObject *args)
{
  PyObject *key;
  int i;

  UNLESS (PyArg_ParseTuple(args, "O", &key)) return NULL;
  if ((i=_BTree_set(self, key, Py_None, 1, 1)) < 0) return NULL;
  return PyInt_FromLong(i);
}

static PyObject *
TreeSet_remove(BTree *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args, "O", &key)) return NULL;
  if (_BTree_set(self, key, NULL, 0, 1) < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef TreeSet_methods[] = {
  {"__getstate__", (PyCFunction) BTree_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) BTree_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"has_key",	(PyCFunction) BTree_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"keys",	(PyCFunction) BTree_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
  {"maxKey", (PyCFunction) BTree_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Fine the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([key]) -- Fine the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
  {"clear",	(PyCFunction) BTree_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the BTree"},  
  {"insert",	(PyCFunction)TreeSet_insert,	METH_VARARGS,
   "insert(id,[ignored]) -- Add an id to the set"},
  {"remove",	(PyCFunction)TreeSet_remove,	METH_VARARGS,
   "remove(id) -- Remove a key from the set"},
  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
  {NULL,		NULL}		/* sentinel */
};

static PyExtensionClass TreeSetType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  PREFIX "TreeSet",		/*tp_name*/
  sizeof(BTree),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /************* methods ********************/
  (destructor) BTree_dealloc,   /*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)0,			/*tp_repr*/
  &BTree_as_number_for_nonzero,	/*tp_as_number*/
  0,				/*tp_as_sequence*/
  0,				/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,
  0,				/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Set implemented as sorted tree of items", 
  METHOD_CHAIN(TreeSet_methods),
  EXTENSIONCLASS_BASICNEW_FLAG | PERSISTENT_TYPE_FLAG 
  | EXTENSIONCLASS_NOINSTDICT_FLAG,
};
