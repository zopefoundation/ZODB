/*

 Copyright (c) 2003 Zope Corporation and Contributors.
 All Rights Reserved.

 This software is subject to the provisions of the Zope Public License,
 Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
 THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
 WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
 FOR A PARTICULAR PURPOSE.

*/
static char _Persistence_module_documentation[] = 
"Persistent ExtensionClass\n"
"\n"
"$Id: _Persistence.c,v 1.2 2003/11/28 16:44:46 jim Exp $\n"
;

#include "ExtensionClass.h"
#include "cPersistence.h"


/* convert_name() returns a new reference to a string name
   or sets an exception and returns NULL.
*/

static PyObject *
convert_name(PyObject *name)
{
#ifdef Py_USING_UNICODE
    /* The Unicode to string conversion is done here because the
       existing tp_setattro slots expect a string object as name
       and we wouldn't want to break those. */
    if (PyUnicode_Check(name)) {
	name = PyUnicode_AsEncodedString(name, NULL, NULL);
    }
    else
#endif
    if (!PyString_Check(name)) {
	PyErr_SetString(PyExc_TypeError, "attribute name must be a string");
	return NULL;
    } else
	Py_INCREF(name);
    return name;
}

/* Returns true if the object requires unghostification.

   There are several special attributes that we allow access to without
   requiring that the object be unghostified:
   __class__
   __del__
   __dict__
   __of__
   __setstate__
*/

static int
unghost_getattr(const char *s)
{
    if (*s++ != '_')
	return 1;
    if (*s == 'p') {
	s++;
	if (*s == '_')
	    return 0; /* _p_ */
	else
	    return 1;
    }
    else if (*s == '_') {
	s++;
	switch (*s) {
	case 'c':
	    return strcmp(s, "class__");
	case 'd':
	    s++;
	    if (!strcmp(s, "el__"))
		return 0; /* __del__ */
	    if (!strcmp(s, "ict__"))
		return 0; /* __dict__ */
	    return 1;
	case 'o':
	    return strcmp(s, "of__");
	case 's':
	    return strcmp(s, "setstate__");
	default:
	    return 1;
	}
    }
    return 1;
}

static PyObject *
P_getattr(cPersistentObject *self, PyObject *name)
{
  PyObject *v=NULL;
  char *s;

  name = convert_name(name);
  if (!name)
    return NULL;

  s = PyString_AS_STRING(name);

  if (*s != '_' || unghost_getattr(s)) 
    {
      if (PER_USE(self))
        {
          v = Py_FindAttr((PyObject*)self, name);
          PER_ALLOW_DEACTIVATION(self);
          PER_ACCESSED(self);
        }
    }
  else
    v = Py_FindAttr((PyObject*)self, name);

  Py_DECREF(name);

  return v;
}


static PyTypeObject Ptype = {
	PyObject_HEAD_INIT(NULL)
	/* ob_size           */ 0,
	/* tp_name           */ "Persistence.Persistent",
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        /* tp_getattro       */ (getattrofunc)P_getattr,
        0, 0,
        /* tp_flags          */ Py_TPFLAGS_DEFAULT
				| Py_TPFLAGS_BASETYPE ,
	/* tp_doc            */ "Persistent ExtensionClass",
};

static struct PyMethodDef _Persistence_methods[] = {
	{NULL,	 (PyCFunction)NULL, 0, NULL}		/* sentinel */
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
init_Persistence(void)
{
  PyObject *m;
        
  if (! ExtensionClassImported)
    return;

  cPersistenceCAPI = PyCObject_Import("persistent.cPersistence", "CAPI");
  if (cPersistenceCAPI == NULL)
    return;

  Ptype.tp_bases = Py_BuildValue("OO", cPersistenceCAPI->pertype, ECBaseType);
  if (Ptype.tp_bases == NULL)
    return;
  Ptype.tp_base = cPersistenceCAPI->pertype;
  
  Ptype.ob_type = ECExtensionClassType;
  if (PyType_Ready(&Ptype) < 0)
    return;
        
  /* Create the module and add the functions */
  m = Py_InitModule3("_Persistence", _Persistence_methods,
                     _Persistence_module_documentation);
  
  if (m == NULL)
    return;
  
  /* Add types: */
  if (PyModule_AddObject(m, "Persistent", (PyObject *)&Ptype) < 0)
    return;
}

