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
#include "Python.h"
#include "cPersistence.h"

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define OBJECT(O) ((PyObject*)(O))

static PyObject *py__p_oid, *py__p_jar, *py___getinitargs__, *py___module__;
static PyObject *py_new_oid;


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
  PyObject *object, *oid, *jar=NULL, *r=NULL, *klass;

  /*
  def persistent_id(object, self=self,stackup=stackup):
  */
  UNLESS (PyArg_ParseTuple(args, "O", &object)) return NULL;

  /*
    if (not hasattr(object, '_p_oid') or
        type(object) is ClassType): return None
   */


  /* Filter out most objects with low-level test. Yee ha! */
  UNLESS (PyExtensionClass_Check(object)
	  ||
	  (PyExtensionInstance_Check(object) &&
	   (((PyExtensionClass*)(object->ob_type))->class_flags 
	    & PERSISTENT_TYPE_FLAG)
	   )
	  )
    goto not_persistent;

  UNLESS (oid=PyObject_GetAttr(object, py__p_oid)) 
    {
      PyErr_Clear();
      goto not_persistent;
    }

  /*
      if oid is None or object._p_jar is not self:
   */
  if (oid != Py_None)
    UNLESS (jar=PyObject_GetAttr(object, py__p_jar)) PyErr_Clear();

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
      UNLESS (oid) goto err;
      if (PyObject_SetAttr(object, py__p_jar, self->jar) < 0) goto err;
      if (PyObject_SetAttr(object, py__p_oid, oid) < 0) goto err;
      UNLESS (r=PyTuple_New(1)) goto err;
      PyTuple_SET_ITEM(r, 0, object);
      Py_INCREF(object);
      ASSIGN(r, PyObject_CallObject(self->stackup, r));
      UNLESS (r) goto err;
      Py_DECREF(r);
    }

  Py_XDECREF(jar);

  /*
      klass=object.__class__

      if klass is ExtensionKlass: return oid
  */
  
  if (PyExtensionClass_Check(object)) return oid;

  /*
      if hasattr(klass, '__getinitargs__'): return oid
  */

  klass=OBJECT(object->ob_type);
  if ((r=PyObject_GetAttr(klass, py___getinitargs__)))
    {
      Py_DECREF(r);
      return oid;
    }
  PyErr_Clear();

  /*
      module=getattr(klass,'__module__','')
      if module: klass=module, klass.__name__
  */
  if ((jar=PyObject_GetAttr(klass, py___module__)))
    {
      UNLESS (r=PyTuple_New(2)) goto err;
      PyTuple_SET_ITEM(r, 0, jar);
      Py_INCREF(klass);
      PyTuple_SET_ITEM(r, 1, klass);
      klass=r;
    }
  else 
    {
      PyErr_Clear();
      Py_INCREF(klass);
    }

  /*      
      return oid, klass
  */
  if ((r=PyTuple_New(2)))
    {
      PyTuple_SET_ITEM(r, 0, oid);
      PyTuple_SET_ITEM(r, 1, klass);
    }
  else
    {
      Py_DECREF(oid);
      Py_DECREF(klass);
    }
  return r;

not_persistent:
  Py_INCREF(Py_None);
  return Py_None;

err:
  Py_DECREF(oid);
  Py_XDECREF(jar);
  return NULL;
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
initcoptimizations()
{
  PyObject *m, *d;
  char *rev="$Revision: 1.1 $";

#define make_string(S) if (! (py_ ## S=PyString_FromString(#S))) return
  make_string(_p_oid);
  make_string(_p_jar);
  make_string(__getinitargs__);
  make_string(__module__);
  make_string(new_oid);

  UNLESS (ExtensionClassImported) return;

  m = Py_InitModule4("coptimizations", Module_Level__methods,
		     "C optimizations",
		     (PyObject*)NULL,PYTHON_API_VERSION);
  d = PyModule_GetDict(m);

  persistent_idType.ob_type=&PyType_Type;
  PyDict_SetItemString(d,"persistent_idType", OBJECT(&persistent_idType));

  PyDict_SetItemString(d, "__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));  
}
