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
"$Id: coptimizations.c,v 1.16 2002/02/11 23:40:42 gvanrossum Exp $\n";

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
  PyObject *jar, *stackup, *new_oid;
} persistent_id;

staticforward PyTypeObject persistent_idType;

static persistent_id *
newpersistent_id(PyObject *ignored, PyObject *args)
{
  persistent_id *self;
  PyObject *jar, *stackup;

  UNLESS (PyArg_ParseTuple(args, "OO", &jar, &stackup)) return NULL;
  UNLESS(self = PyObject_NEW(persistent_id, &persistent_idType)) return NULL;
  Py_INCREF(jar);
  self->jar=jar;
  Py_INCREF(stackup);
  self->stackup=stackup;
  self->new_oid=NULL;
  return self;
}


static void
persistent_id_dealloc(persistent_id *self)
{
  Py_DECREF(self->jar);
  Py_DECREF(self->stackup);
  Py_XDECREF(self->new_oid);
  PyMem_DEL(self);
}

static PyObject *
persistent_id_call(persistent_id *self, PyObject *args, PyObject *kwargs)
{
  PyObject *object, *oid, *jar=NULL, *r=NULL, *klass=NULL;

  /*
  def persistent_id(object, self=self,stackup=stackup):
  */
  UNLESS (PyArg_ParseTuple(args, "O", &object)) return NULL;

  /*
    if (not hasattr(object, '_p_oid') or
        type(object) is ClassType): return None
   */


  /* Filter out most objects with low-level test. 
     Yee ha! 
     (Also get klass along the way.)
  */
  if (! PyExtensionClass_Check(object)) {
    if (PyExtensionInstance_Check(object))
      {
	UNLESS (klass=PyObject_GetAttr(object, py___class__)) 
	  {
	    PyErr_Clear();
	    goto not_persistent;
	  }
	UNLESS (
		PyExtensionClass_Check(klass) &&
		(((PyExtensionClass*)klass)->class_flags 
		 & PERSISTENT_TYPE_FLAG)
		)
	  goto not_persistent;

      }
    else 
      goto not_persistent;
  }

  UNLESS (oid=PyObject_GetAttr(object, py__p_oid)) 
    {
      PyErr_Clear();
      goto not_persistent;
    }

  /*
      if oid is None or object._p_jar is not self:
   */
  if (oid != Py_None)
    {
      UNLESS (jar=PyObject_GetAttr(object, py__p_jar)) PyErr_Clear();
      if (jar && jar != Py_None && jar != self->jar)
	{
	  PyErr_SetString(InvalidObjectReference, 
			  "Attempt to store an object from a foreign "
			  "database connection");
	  return NULL;
	}
    }

  if (oid == Py_None || jar != self->jar)
    {
      /*
          oid = self.new_oid()
          object._p_jar=self
          object._p_oid=oid
          stackup(object)
      */
      UNLESS (self->new_oid ||
	      (self->new_oid=PyObject_GetAttr(self->jar, py_new_oid)))
	    goto err;
      ASSIGN(oid, PyObject_CallObject(self->new_oid, NULL));
      UNLESS (oid) goto null_oid;
      if (PyObject_SetAttr(object, py__p_jar, self->jar) < 0) goto err;
      if (PyObject_SetAttr(object, py__p_oid, oid) < 0) goto err;
      UNLESS (r=PyTuple_New(1)) goto err;
      PyTuple_SET_ITEM(r, 0, object);
      Py_INCREF(object);
      ASSIGN(r, PyObject_CallObject(self->stackup, r));
      UNLESS (r) goto err;
      Py_DECREF(r);
    }

  /*
      klass=object.__class__

      if klass is ExtensionKlass: return oid
  */
  
  if (PyExtensionClass_Check(object)) goto return_oid;

  /*
      if hasattr(klass, '__getinitargs__'): return oid
  */

  if ((r=PyObject_GetAttr(klass, py___getinitargs__)))
    {
      Py_DECREF(r);
      goto return_oid;
    }
  PyErr_Clear();

  /*
      module=getattr(klass,'__module__','')
      if module: klass=module, klass.__name__
      else: return oid # degenerate 1.x ZClass case
  */
  UNLESS (jar=PyObject_GetAttr(klass, py___module__)) goto err;

  UNLESS (PyObject_IsTrue(jar)) goto return_oid;

  ASSIGN(klass, PyObject_GetAttr(klass, py___name__));
  UNLESS (klass) goto err;

  UNLESS (r=PyTuple_New(2)) goto err;
  PyTuple_SET_ITEM(r, 0, jar);
  PyTuple_SET_ITEM(r, 1, klass);
  klass=r;
  jar=NULL;

  /*      
      return oid, klass
  */
  UNLESS (r=PyTuple_New(2)) goto err;
  PyTuple_SET_ITEM(r, 0, oid);
  PyTuple_SET_ITEM(r, 1, klass);
  return r;

not_persistent:
  Py_INCREF(Py_None);
  return Py_None;

err:
  Py_DECREF(oid);
  oid=NULL;

null_oid:
return_oid:
  Py_XDECREF(jar);
  Py_XDECREF(klass);
  return oid;
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
   "new_persistent_id(jar, stackup, new_oid)"
   " -- get a new persistent_id function"},
  {NULL, (PyCFunction)NULL, 0, NULL}		/* sentinel */
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
  UNLESS (m=PyString_FromString("POSException")) return;
  ASSIGN(m, PyImport_Import(m));
  UNLESS (m) return;
  ASSIGN(m, PyObject_GetAttrString(m, "InvalidObjectReference"));
  UNLESS (m) return;
  InvalidObjectReference=m;

  UNLESS (ExtensionClassImported) return;

  m = Py_InitModule4("coptimizations", Module_Level__methods,
		     coptimizations_doc_string,
		     (PyObject*)NULL,PYTHON_API_VERSION);
  d = PyModule_GetDict(m);

  persistent_idType.ob_type=&PyType_Type;
  PyDict_SetItemString(d,"persistent_idType", OBJECT(&persistent_idType));

  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module coptimizations");
}
