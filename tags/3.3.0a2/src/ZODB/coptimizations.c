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
static char coptimizations_doc_string[] = 
"C optimization for new_persistent_id().\n"
"\n"
"$Id: coptimizations.c,v 1.26 2003/12/11 16:02:56 jeremy Exp $\n";

#include "cPersistence.h"

static PyObject *py__p_oid, *py__p_jar, *py___getinitargs__, *py___module__;
static PyObject *py_new_oid, *py___class__, *py___name__;

static PyObject *InvalidObjectReference;

typedef struct {
    PyObject_HEAD
    PyObject *jar, *stack, *new_oid;
} persistent_id;

static PyTypeObject persistent_idType;

static persistent_id *
newpersistent_id(PyObject *ignored, PyObject *args)
{
    persistent_id *self;
    PyObject *jar, *stack;

    if (!PyArg_ParseTuple(args, "OO!", &jar, &PyList_Type, &stack)) 
	return NULL;
    self = PyObject_NEW(persistent_id, &persistent_idType);
    if (!self)
	return NULL;
    Py_INCREF(jar);
    self->jar = jar;
    Py_INCREF(stack);
    self->stack = stack;
    self->new_oid = NULL;
    return self;
}

static void
persistent_id_dealloc(persistent_id *self)
{
    Py_DECREF(self->jar);
    Py_DECREF(self->stack);
    Py_XDECREF(self->new_oid);
    PyObject_DEL(self);
}

/* Returns the klass of a persistent object.
   Returns NULL for other objects.
*/
int
get_class(PyObject *object, PyObject **out_class)
{
    PyObject *class = NULL;

    if (!PyType_Check(object)) {
	if (!PER_TypeCheck(object)) 
	    return 0;

	class = PyObject_GetAttr(object, py___class__);
	if (!class) {
	    PyErr_Clear();
	    return 0;
	}
    }
    *out_class = class;
    return 1;
}

/* Return a two-tuple of the class's module and name.
 */
static PyObject *
get_class_tuple(PyObject *class, PyObject *oid)
{
    PyObject *module = NULL, *name = NULL, *tuple;

    module = PyObject_GetAttr(class, py___module__);
    if (!module)
	goto err;
    if (!PyObject_IsTrue(module)) {
	Py_DECREF(module);
	/* If the class has no __module__, it must be a degnerate ZClass. */
	return oid;
    }

    name = PyObject_GetAttr(class, py___name__);
    if (!name)
	goto err;

    tuple = PyTuple_New(2);
    if (!tuple)
	goto err;
    PyTuple_SET_ITEM(tuple, 0, module);
    PyTuple_SET_ITEM(tuple, 1, name);

    return tuple;
 err:
    Py_XDECREF(module);
    Py_XDECREF(name);
    return NULL;
}

static PyObject *
set_oid(persistent_id *self, PyObject *object)
{
    PyObject *oid;

    if (!self->new_oid) {
	self->new_oid = PyObject_GetAttr(self->jar, py_new_oid);
	if (!self->new_oid)
	    return NULL;
    }
    oid = PyObject_CallObject(self->new_oid, NULL);
    if (!oid)
	return NULL;
    if (PyObject_SetAttr(object, py__p_oid, oid) < 0) 
	goto err;
    if (PyObject_SetAttr(object, py__p_jar, self->jar) < 0) 
	goto err;
    if (PyList_Append(self->stack, object) < 0)
	goto err;
    return oid;
 err:
    Py_DECREF(oid);
    return NULL;
}

/* persistent_id_call()

   Returns a reference to a persistent object, appending it to the the
   persistent_id's list of objects.  If a non-persistent object is
   found, return None.

   The returned reference can be either class info, oid pair or a
   plain old oid.  If it is a pair, the class info is the module and
   the name of the class.  The class info can be used to create a
   ghost without loading the class.

   For unusual objects, e.g. ZClasses, return just the oid.  An object
   is unusual if it isn't an ExtensionClass, because that means it
   doesn't inherit from Persistence, or if it has __getinitargs__().
*/

static PyObject *
persistent_id_call(persistent_id *self, PyObject *args, PyObject *kwargs)
{
    PyObject *object, *oid=NULL, *klass=NULL;
    PyObject *t1, *t2;
    int setjar = 0;

    if (!PyArg_ParseTuple(args, "O", &object))
	return NULL;

    /* If it is not an extension class, get the object's class. */
    if (!get_class(object, &klass))
	goto return_none;

    oid = PyObject_GetAttr(object, py__p_oid);
    if (!oid) {
	PyErr_Clear();
	goto return_none;
    }
    if (oid != Py_None) {
	PyObject *jar;

	if (!PyString_Check(oid)) {
	    /* If the object is a class, then asking for _p_oid or
	       _p_jar will return a descriptor.  There is no API to
	       ask whether something is a descriptor; the best you
	       can do is call anything with an __get__ a descriptor.

	       The getattr check is potentially expensive so do the
	       cheap PyString_Check() first, assuming that most oids
	       that aren't None are real oids.  ZODB always uses
	       strings, although some other user of Persistent could
	       use something else.
	    */
	    static PyObject *__get__;
	    PyObject *descr;
	    if (!__get__) {
		__get__ = PyString_InternFromString("__get__");
		if (!__get__)
		    goto err;
	    }
	    descr = PyObject_GetAttr(oid, __get__);
	    if (descr) {
		Py_DECREF(descr);
		goto return_none;
	    }
	    /* Otherwise it's not a descriptor and it's just some
	       weird value.  Maybe we'll get an error later.
	    */

	    /* XXX should check that this was an AttributeError */
	    PyErr_Clear();
	}
	jar = PyObject_GetAttr(object, py__p_jar);
	if (!jar)
	    PyErr_Clear();
	else {
	    if (jar != Py_None && jar != self->jar) {
		PyErr_SetString(InvalidObjectReference, 
				"Attempt to store an object from a foreign "
				"database connection");
		goto err;
	    }
	    /* Ignore the oid of the unknown jar and assign a new one. */
	    if (jar == Py_None)
		setjar = 1;
	    Py_DECREF(jar);
	}
    }

    if (oid == Py_None || setjar) {
	Py_DECREF(oid);
	oid = set_oid(self, object);
	if (!oid)
	    goto err;
    }

    if (PyType_Check(object) || PyObject_HasAttr(klass, py___getinitargs__))
	goto return_oid;

    t2 = get_class_tuple(klass, oid);
    if (!t2)
	goto err;
    if (t2 == oid) /* Couldn't find class info, just used oid. */
	goto return_oid;
    t1 = PyTuple_New(2);
    if (!t1) {
	Py_DECREF(t2);
	goto err;
    }
    /* use borrowed references to oid and t2 */
    PyTuple_SET_ITEM(t1, 0, oid);
    PyTuple_SET_ITEM(t1, 1, t2);

    Py_DECREF(klass);

    return t1;

 err:
    Py_XDECREF(oid);
    oid = NULL;

 return_oid:
    Py_XDECREF(klass);
    return oid;

 return_none:
    Py_XDECREF(oid);
    Py_XDECREF(klass);
    Py_INCREF(Py_None);
    return Py_None;
}


static PyTypeObject persistent_idType = {
    PyObject_HEAD_INIT(NULL)
    0,					/*ob_size*/
    "persistent_id",			/*tp_name*/
    sizeof(persistent_id),		/*tp_basicsize*/
    0,					/*tp_itemsize*/
    (destructor)persistent_id_dealloc,	/*tp_dealloc*/
    0,					/*tp_print*/
    0,					/*tp_getattr*/
    0,					/*tp_setattr*/
    0,					/*tp_compare*/
    0,					/*tp_repr*/
    0,					/*tp_as_number*/
    0,					/*tp_as_sequence*/
    0,					/*tp_as_mapping*/
    0,					/*tp_hash*/
    (ternaryfunc)persistent_id_call,	/*tp_call*/
    0,					/*tp_str*/
    0,					/*tp_getattro*/
    0,					/*tp_setattro*/
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,			/* tp_flags */
    "C implementation of the persistent_id function defined in Connection.py"
    					/* tp_doc */
};

/* End of code for persistent_id objects */
/* -------------------------------------------------------- */


/* List of methods defined in the module */

static struct PyMethodDef Module_Level__methods[] = {
    {"new_persistent_id", (PyCFunction)newpersistent_id, METH_VARARGS,
     "new_persistent_id(jar, stack) -- get a new persistent_id function"},
    {NULL, NULL}		/* sentinel */
};

void
initcoptimizations(void)
{
    PyObject *m;

#define make_string(S) if (! (py_ ## S=PyString_FromString(#S))) return
    make_string(_p_oid);
    make_string(_p_jar);
    make_string(__getinitargs__);
    make_string(__module__);
    make_string(__class__);
    make_string(__name__);
    make_string(new_oid);
			
    /* Get InvalidObjectReference error */
    m = PyImport_ImportModule("ZODB.POSException");
    if (!m)
	return;
    InvalidObjectReference = PyObject_GetAttrString(m, 
						    "InvalidObjectReference");
    Py_DECREF(m);
    if (!InvalidObjectReference)
	return;

    cPersistenceCAPI = PyCObject_Import("persistent.cPersistence", "CAPI");
    if (!cPersistenceCAPI)
	return;

    m = Py_InitModule3("coptimizations", Module_Level__methods,
		       coptimizations_doc_string);

    persistent_idType.ob_type = &PyType_Type;
    Py_INCREF((PyObject *)&persistent_idType);
    PyModule_AddObject(m, "persistent_idType", (PyObject *)&persistent_idType);
}
