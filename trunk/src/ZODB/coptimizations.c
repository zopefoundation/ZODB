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
"$Id: coptimizations.c,v 1.21 2002/12/12 18:52:21 jeremy Exp $\n";

#include "Python.h"
#define DONT_USE_CPERSISTENCECAPI
#include "cPersistence.h"

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define OBJECT(O) ((PyObject*)(O))

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

    if (!PyExtensionClass_Check(object)) {
	if (PyExtensionInstance_Check(object)) {
	    class = PyObject_GetAttr(object, py___class__);
	    if (!class) {
		PyErr_Clear();
		return 0;
	    }
	    if (!PyExtensionClass_Check(class) ||
		!(((PyExtensionClass*)class)->class_flags 
		  & PERSISTENT_TYPE_FLAG)) {
		Py_DECREF(class);
		return 0;
	    }
	}
	else
	    return 0;
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
	/* XXX Handle degenerate 1.x ZClass case. */
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

static PyObject *
persistent_id_call(persistent_id *self, PyObject *args, PyObject *kwargs)
{
    PyObject *object, *oid, *klass=NULL;
    PyObject *t1, *t2;
    int setjar = 0;

    if (!PyArg_ParseTuple(args, "O", &object))
	return NULL;

    /* If it is an extension class, get the class. */
    if (!get_class(object, &klass))
	goto return_none;

    oid = PyObject_GetAttr(object, py__p_oid);
    if (!oid) {
	PyErr_Clear();
	Py_XDECREF(klass);
	goto return_none;
    }

    if (oid != Py_None) {
	PyObject *jar = PyObject_GetAttr(object, py__p_jar);
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

    if (PyExtensionClass_Check(object)
	|| PyObject_HasAttr(klass, py___getinitargs__))
	goto return_oid;

    if (!klass)  /* pass through ZClass special case */
	goto return_oid;
    t2 = get_class_tuple(klass, oid);
    if (!t2)
	goto err;
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
    Py_INCREF(Py_None);
    return Py_None;
}


static PyTypeObject persistent_idType = {
    PyObject_HEAD_INIT(NULL)
    0,				/*ob_size*/
    "persistent_id",			/*tp_name*/
    sizeof(persistent_id),		/*tp_basicsize*/
    0,				/*tp_itemsize*/
    /* methods */
    (destructor)persistent_id_dealloc,	/*tp_dealloc*/
    (printfunc)0,	/*tp_print*/
    (getattrfunc)0,		/*obsolete tp_getattr*/
    (setattrfunc)0,		/*obsolete tp_setattr*/
    (cmpfunc)0,	/*tp_compare*/
    (reprfunc)0,		/*tp_repr*/
    0,		/*tp_as_number*/
    0,		/*tp_as_sequence*/
    0,		/*tp_as_mapping*/
    (hashfunc)0,		/*tp_hash*/
    (ternaryfunc)persistent_id_call,	/*tp_call*/
    (reprfunc)0,		/*tp_str*/
    (getattrofunc)0,	/*tp_getattro*/
    (setattrofunc)0,	/*tp_setattro*/
    
    /* Space for future expansion */
    0L,0L,
    "C implementation of the persistent_id function defined in Connection.py"
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
    PyObject *m, *d;

#define make_string(S) if (! (py_ ## S=PyString_FromString(#S))) return
    make_string(_p_oid);
    make_string(_p_jar);
    make_string(__getinitargs__);
    make_string(__module__);
    make_string(__class__);
    make_string(__name__);
    make_string(new_oid);
			
    /* Get InvalidObjectReference error */
    UNLESS (m=PyString_FromString("ZODB.POSException")) return;
    ASSIGN(m, PyImport_Import(m));
    UNLESS (m) return;
    ASSIGN(m, PyObject_GetAttrString(m, "InvalidObjectReference"));
    UNLESS (m) return;
    InvalidObjectReference=m;

    if (!ExtensionClassImported) 
	return;

    m = Py_InitModule3("coptimizations", Module_Level__methods,
		       coptimizations_doc_string);
    d = PyModule_GetDict(m);

    persistent_idType.ob_type = &PyType_Type;
    PyDict_SetItemString(d,"persistent_idType", OBJECT(&persistent_idType));
}
