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

#define TREESETTEMPLATE_C "$Id: TreeSetTemplate.c,v 1.16 2003/11/28 16:44:44 jim Exp $\n"

static PyObject *
TreeSet_insert(BTree *self, PyObject *args)
{
    PyObject *key;
    int i;

    if (!PyArg_ParseTuple(args, "O:insert", &key)) 
	return NULL;
    i = _BTree_set(self, key, Py_None, 1, 1);
    if (i < 0) 
	return NULL;
    return PyInt_FromLong(i);
}

/* _Set_update and _TreeSet_update are identical except for the
   function they call to add the element to the set.
*/

static int
_TreeSet_update(BTree *self, PyObject *seq)
{
    int n = -1;
    PyObject *iter, *v;
    int ind;

    iter = PyObject_GetIter(seq);
    if (iter == NULL)
	return -1;

    while (1) {
	v = PyIter_Next(iter);
	if (v == NULL) {
	    if (PyErr_Occurred())
		goto err;
	    else
		break;
	}
	ind = _BTree_set(self, v, Py_None, 1, 1);
	Py_DECREF(v);
	if (ind < 0)
	    goto err;
	else
	    n += ind;
    }
    /* n starts out at -1, which is the error return value.  If
       this point is reached, then there is no error.  n must be
       incremented to account for the initial value of -1 instead of
       0.
    */
    n++;

 err:
    Py_DECREF(iter);
    return n;
}

static PyObject *
TreeSet_update(BTree *self, PyObject *args)
{
    PyObject *seq = NULL;
    int n = 0;

    if (!PyArg_ParseTuple(args, "|O:update", &seq))
	return NULL;

    if (seq) {
	n = _TreeSet_update(self, seq);
	if (n < 0)
	    return NULL;
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
  PER_UNUSE(self);

  if (r < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef TreeSet_methods[] = {
  {"__getstate__", (PyCFunction) BTree_getstate,	METH_NOARGS,
   "__getstate__() -> state\n\n"
   "Return the picklable state of the TreeSet."},

  {"__setstate__", (PyCFunction) TreeSet_setstate,	METH_VARARGS,
   "__setstate__(state)\n\n"
   "Set the state of the TreeSet."},

  {"has_key",	(PyCFunction) BTree_has_key,	METH_O,
   "has_key(key)\n\n"
   "Return true if the TreeSet contains the given key."},

  {"keys",	(PyCFunction) BTree_keys,	METH_KEYWORDS,
   "keys([min, max]) -> list of keys\n\n"
   "Returns the keys of the TreeSet.  If min and max are supplied, only\n"
   "keys greater than min and less than max are returned."},

  {"maxKey", (PyCFunction) BTree_maxKey,	METH_VARARGS,
   "maxKey([max]) -> key\n\n"
   "Return the largest key in the BTree.  If max is specified, return\n"
   "the largest key <= max."},

  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([mi]) -> key\n\n"
   "Return the smallest key in the BTree.  If min is specified, return\n"
   "the smallest key >= min."},

  {"clear",	(PyCFunction) BTree_clear,	METH_NOARGS,
   "clear()\n\nRemove all of the items from the BTree."},

  {"insert",	(PyCFunction)TreeSet_insert,	METH_VARARGS,
   "insert(id,[ignored]) -- Add an id to the set"},

  {"update",	(PyCFunction)TreeSet_update,	METH_VARARGS,
   "update(collection)\n\n Add the items from the given collection."},

  {"remove",	(PyCFunction)TreeSet_remove,	METH_VARARGS,
   "remove(id) -- Remove a key from the set"},

  {"_check", (PyCFunction) BTree_check,       METH_NOARGS,
   "Perform sanity check on TreeSet, and raise exception if flawed."},

#ifdef PERSISTENT
  {"_p_resolveConflict", (PyCFunction) BTree__p_resolveConflict, METH_VARARGS,
   "_p_resolveConflict() -- Reinitialize from a newly created copy"},

  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_KEYWORDS,
   "_p_deactivate()\n\nReinitialize from a newly created copy."},
#endif
  {NULL,		NULL}		/* sentinel */
};

static PyMappingMethods TreeSet_as_mapping = {
  (inquiry)BTree_length,		/*mp_length*/
};

static PySequenceMethods TreeSet_as_sequence = {
    (inquiry)0,                     /* sq_length */
    (binaryfunc)0,                  /* sq_concat */
    (intargfunc)0,                  /* sq_repeat */
    (intargfunc)0,                  /* sq_item */
    (intintargfunc)0,               /* sq_slice */
    (intobjargproc)0,               /* sq_ass_item */
    (intintobjargproc)0,            /* sq_ass_slice */
    (objobjproc)BTree_contains,     /* sq_contains */
    0,                              /* sq_inplace_concat */
    0,                              /* sq_inplace_repeat */
};

static int
TreeSet_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *v = NULL;

    if (!PyArg_ParseTuple(args, "|O:" MOD_NAME_PREFIX "TreeSet", &v))
	return -1;

    if (v)
	return _TreeSet_update((BTree *)self, v);
    else
	return 0;
}

static PyTypeObject TreeSetType = {
    PyObject_HEAD_INIT(NULL) /* PyPersist_Type */
    0,					/* ob_size */
    MODULE_NAME MOD_NAME_PREFIX "TreeSet",/* tp_name */
    sizeof(BTree),			/* tp_basicsize */
    0,					/* tp_itemsize */
    (destructor)BTree_dealloc,		/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    0,					/* tp_compare */
    0,					/* tp_repr */
    &BTree_as_number_for_nonzero,	/* tp_as_number */
    &TreeSet_as_sequence,		/* tp_as_sequence */
    &TreeSet_as_mapping,		/* tp_as_mapping */
    0,					/* tp_hash */
    0,					/* tp_call */
    0,					/* tp_str */
    0,					/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
	    Py_TPFLAGS_BASETYPE, 	/* tp_flags */
    0,					/* tp_doc */
    (traverseproc)BTree_traverse,	/* tp_traverse */
    (inquiry)BTree_tp_clear,		/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    (getiterfunc)BTree_getiter,		/* tp_iter */
    0,					/* tp_iternext */
    TreeSet_methods,			/* tp_methods */
    BTree_members,			/* tp_members */
    0,					/* tp_getset */
    0,					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0,					/* tp_dictoffset */
    TreeSet_init,			/* tp_init */
    0,					/* tp_alloc */
    0, /*PyType_GenericNew,*/		/* tp_new */
};
