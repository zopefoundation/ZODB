/*****************************************************************************

  Copyright (c) 2001, 2002 Zope Corporation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#define TREESETTEMPLATE_C "$Id: TreeSetTemplate.c,v 1.15 2002/10/05 00:39:57 gvanrossum Exp $\n"

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
TreeSet_update(BTree *self, PyObject *args)
{
  PyObject *seq=0, *o, *t, *v, *tb;
  int i, n=0, ind;

  UNLESS(PyArg_ParseTuple(args, "|O:update", &seq)) return NULL;

  if (seq)
    {
      for (i=0; ; i++)
        {
          UNLESS (o=PySequence_GetItem(seq, i))
            {
              PyErr_Fetch(&t, &v, &tb);
              if (t != PyExc_IndexError)
                {
                  PyErr_Restore(t, v, tb);
                  return NULL;
                }
              Py_XDECREF(t);
              Py_XDECREF(v);
              Py_XDECREF(tb);
              break;
            }
          ind=_BTree_set(self, o, Py_None, 1, 1);
          Py_DECREF(o);
          if (ind < 0) return NULL;
          n += ind;
        }
    }

  return PyInt_FromLong(n);
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

static PyObject *
TreeSet_setstate(BTree *self, PyObject *args)
{
  int r;

  if (!PyArg_ParseTuple(args,"O",&args)) return NULL;

  PER_PREVENT_DEACTIVATION(self);
  r=_BTree_setstate(self, args, 1);
  PER_ALLOW_DEACTIVATION(self);
  PER_ACCESSED(self);

  if (r < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef TreeSet_methods[] = {
  {"__getstate__", (PyCFunction) BTree_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) TreeSet_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"has_key",	(PyCFunction) BTree_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"keys",	(PyCFunction) BTree_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
  {"maxKey", (PyCFunction) BTree_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Find the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([key]) -- Find the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
  {"clear",	(PyCFunction) BTree_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the BTree"},
  {"insert",	(PyCFunction)TreeSet_insert,	METH_VARARGS,
   "insert(id,[ignored]) -- Add an id to the set"},
  {"update",	(PyCFunction)TreeSet_update,	METH_VARARGS,
   "update(seq) -- Add the items from the given sequence to the set"},
  {"__init__",	(PyCFunction)TreeSet_update,	METH_VARARGS,
   "__init__(seq) -- Initialize with  the items from the given sequence"},
  {"remove",	(PyCFunction)TreeSet_remove,	METH_VARARGS,
   "remove(id) -- Remove a key from the set"},
  {"_check", (PyCFunction) BTree_check,         METH_VARARGS,
   "Perform sanity check on TreeSet, and raise exception if flawed."},
#ifdef PERSISTENT
  {"_p_resolveConflict", (PyCFunction) BTree__p_resolveConflict, METH_VARARGS,
   "_p_resolveConflict() -- Reinitialize from a newly created copy"},
  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static PyMappingMethods TreeSet_as_mapping = {
  (inquiry)BTree_length,		/*mp_length*/
};

static PyExtensionClass TreeSetType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  MOD_NAME_PREFIX "TreeSet",		/*tp_name*/
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
  &TreeSet_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,
  0,				/*tp_setattro*/

  /* Space for future expansion */
  0L,0L,
  "Set implemented as sorted tree of items",
  METHOD_CHAIN(TreeSet_methods),
  EXTENSIONCLASS_BASICNEW_FLAG
#ifdef PERSISTENT
  | PERSISTENT_TYPE_FLAG
#endif
  | EXTENSIONCLASS_NOINSTDICT_FLAG,
};
