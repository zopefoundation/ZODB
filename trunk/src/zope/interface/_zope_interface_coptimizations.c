/*###########################################################################
 #
 # Copyright (c) 2003 Zope Corporation and Contributors.
 # All Rights Reserved.
 #
 # This software is subject to the provisions of the Zope Public License,
 # Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
 # THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
 # WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 # WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
 # FOR A PARTICULAR PURPOSE.
 #
 ############################################################################*/

#include "Python.h"
#include "structmember.h"

#define TYPE(O) ((PyTypeObject*)(O))
#define OBJECT(O) ((PyObject*)(O))
#define CLASSIC(O) ((PyClassObject*)(O))

static PyObject *str__dict__, *str__implemented__, *strextends;
static PyObject *BuiltinImplementationSpecifications, *str__provides__;
static PyObject *str__class__, *str__providedBy__, *strisOrExtends;
static PyObject *empty, *fallback, *str_implied, *str_cls, *str_implements;
static PyTypeObject *Implements;

static int imported_declarations = 0;

static int 
import_declarations(void)
{
  PyObject *declarations, *i;

  declarations = PyImport_ImportModule("zope.interface.declarations");
  if (declarations == NULL)
    return -1;
  
  BuiltinImplementationSpecifications = PyObject_GetAttrString(
                    declarations, "BuiltinImplementationSpecifications");
  if (BuiltinImplementationSpecifications == NULL)
    return -1;

  empty = PyObject_GetAttrString(declarations, "_empty");
  if (empty == NULL)
    return -1;

  fallback = PyObject_GetAttrString(declarations, "implementedByFallback");
  if (fallback == NULL)
    return -1;



  i = PyObject_GetAttrString(declarations, "Implements");
  if (i == NULL)
    return -1;

  if (! PyType_Check(i))
    {
      PyErr_SetString(PyExc_TypeError, 
                      "zope.declarations.Implements is not a type");
      return -1;
    }

  Implements = (PyTypeObject *)i;

  Py_DECREF(declarations);

  imported_declarations = 1;
  return 0;
}

static PyTypeObject SpecType;   /* Forward */

static PyObject *
implementedByFallback(PyObject *cls)
{
  if (imported_declarations == 0 && import_declarations() < 0)
    return NULL;

  return PyObject_CallFunctionObjArgs(fallback, cls, NULL);
}

static PyObject *
implementedBy(PyObject *ignored, PyObject *cls)
{
  /* Fast retrieval of implements spec, if possible, to optimize
     common case.  Use fallback code if we get stuck.
  */

  PyObject *dict = NULL, *spec;

  if (PyType_Check(cls))
    {
      dict = TYPE(cls)->tp_dict;
      Py_XINCREF(dict);
    }

  if (dict == NULL)
    dict = PyObject_GetAttr(cls, str__dict__);

  if (dict == NULL)
    {
      /* Probably a security proxied class, use more expensive fallback code */
      PyErr_Clear();
      return implementedByFallback(cls);
    }

  spec = PyObject_GetItem(dict, str__implemented__);
  Py_DECREF(dict);
  if (spec)
    {
      if (imported_declarations == 0 && import_declarations() < 0)
        return NULL;

      if (PyObject_TypeCheck(spec, Implements))
        return spec;

      /* Old-style declaration, use more expensive fallback code */
      Py_DECREF(spec);
      return implementedByFallback(cls);
    }

  PyErr_Clear();

  /* Maybe we have a builtin */
  if (imported_declarations == 0 && import_declarations() < 0)
    return NULL;
  
  spec = PyDict_GetItem(BuiltinImplementationSpecifications, cls);
  if (spec != NULL)
    {
      Py_INCREF(spec);
      return spec;
    }

  /* We're stuck, use fallback */
  return implementedByFallback(cls);
}

static PyObject *
getObjectSpecification(PyObject *ignored, PyObject *ob)
{
  PyObject *cls, *result;

  result = PyObject_GetAttr(ob, str__provides__);
  if (result != NULL)
    return result;

  PyErr_Clear();

  /* We do a getattr here so as not to be defeated by proxies */
  cls = PyObject_GetAttr(ob, str__class__);
  if (cls == NULL)
    {
      PyErr_Clear();
      if (imported_declarations == 0 && import_declarations() < 0)
        return NULL;
      Py_INCREF(empty);
      return empty;
    }

  result = implementedBy(NULL, cls);
  Py_DECREF(cls);

  return result;
}

static PyObject *
providedBy(PyObject *ignored, PyObject *ob)
{
  PyObject *result, *cls, *cp;
  
  result = PyObject_GetAttr(ob, str__providedBy__);
  if (result == NULL)
    {
      PyErr_Clear();
      return getObjectSpecification(NULL, ob);
    } 


  /* We want to make sure we have a spec. We can't do a type check
     because we may have a proxy, so we'll just try to get the
     only attribute.
  */
  if (PyObject_HasAttr(result, strextends))
    return result;
    
  /*
    The object's class doesn't understand descriptors.
    Sigh. We need to get an object descriptor, but we have to be
    careful.  We want to use the instance's __provides__,l if
    there is one, but only if it didn't come from the class.
  */
  Py_DECREF(result);

  cls = PyObject_GetAttr(ob, str__class__);
  if (cls == NULL)
    return NULL;

  result = PyObject_GetAttr(ob, str__provides__);
  if (result == NULL)
    {      
      /* No __provides__, so just fall back to implementedBy */
      PyErr_Clear();
      result = implementedBy(NULL, cls);
      Py_DECREF(cls);
      return result;
    } 

  cp = PyObject_GetAttr(cls, str__provides__);
  if (cp == NULL)
    {
      /* The the class has no provides, assume we're done: */
      PyErr_Clear();
      Py_DECREF(cls);
      return result;
    }

  if (cp == result)
    {
      /*
        Oops, we got the provides from the class. This means
        the object doesn't have it's own. We should use implementedBy
      */
      Py_DECREF(result);
      result = implementedBy(NULL, cls);
    }

  Py_DECREF(cls);
  Py_DECREF(cp);

  return result;
}

static PyObject *
inst_attr(PyObject *self, PyObject *name)
{
  /* Get an attribute from an inst dict. Return a borrowed reference.
   */

  PyObject **dictp, *v;

  dictp = _PyObject_GetDictPtr(self);
  if (dictp && *dictp && (v = PyDict_GetItem(*dictp, name)))
    return v;
  PyErr_SetObject(PyExc_AttributeError, name);
  return NULL;
}


static PyObject *
Spec_extends(PyObject *self, PyObject *other)
{  
  PyObject *implied;

  implied = inst_attr(self, str_implied);
  if (implied == NULL)
    return NULL;

#ifdef Py_True
  if (PyDict_GetItem(implied, other) != NULL)
    {
      Py_INCREF(Py_True);
      return Py_True;
    }
  Py_INCREF(Py_False);
  return Py_False;
#else
  return PyInt_FromLong(PyDict_GetItem(implied, other) != NULL);
#endif
}

static char Spec_extends__doc__[] = 
"Test whether a specification is or extends another"
;

static char Spec_providedBy__doc__[] = 
"Test whether an interface is implemented by the specification"
;

static PyObject *
Spec_providedBy(PyObject *self, PyObject *ob)
{
  PyObject *decl, *item;

  decl = providedBy(NULL, ob);
  if (decl == NULL)
    return NULL;

  if (PyObject_TypeCheck(ob, &SpecType))
    item = Spec_extends(decl, self);
  else
    /* decl is probably a security proxy.  We have to go the long way
       around. 
    */
    item = PyObject_CallMethodObjArgs(decl, strisOrExtends, self, NULL);

  Py_DECREF(decl);
  return item;
}


static char Spec_implementedBy__doc__[] = 
"Test whether the specification is implemented by instances of a class"
;

static PyObject *
Spec_implementedBy(PyObject *self, PyObject *cls)
{
  PyObject *decl, *item;

  decl = implementedBy(NULL, cls);
  if (decl == NULL)
    return NULL;
  
  if (PyObject_TypeCheck(decl, &SpecType))
    item = Spec_extends(decl, self);
  else
    item = PyObject_CallMethodObjArgs(decl, strisOrExtends, self, NULL);

  Py_DECREF(decl);
  return item;
}

static struct PyMethodDef Spec_methods[] = {
	{"providedBy",  
         (PyCFunction)Spec_providedBy,		METH_O,
	 Spec_providedBy__doc__},
	{"implementedBy", 
         (PyCFunction)Spec_implementedBy,	METH_O,
	 Spec_implementedBy__doc__},
	{"isOrExtends",	(PyCFunction)Spec_extends,	METH_O,
	 Spec_extends__doc__},

	{NULL,		NULL}		/* sentinel */
};

static PyTypeObject SpecType = {
	PyObject_HEAD_INIT(NULL)
	/* ob_size           */ 0,
	/* tp_name           */ "_interface_coptimizations."
                                "SpecificationBase",
	/* tp_basicsize      */ 0,
	/* tp_itemsize       */ 0,
	/* tp_dealloc        */ (destructor)0,
	/* tp_print          */ (printfunc)0,
	/* tp_getattr        */ (getattrfunc)0,
	/* tp_setattr        */ (setattrfunc)0,
	/* tp_compare        */ (cmpfunc)0,
	/* tp_repr           */ (reprfunc)0,
	/* tp_as_number      */ 0,
	/* tp_as_sequence    */ 0,
	/* tp_as_mapping     */ 0,
	/* tp_hash           */ (hashfunc)0,
	/* tp_call           */ (ternaryfunc)0,
	/* tp_str            */ (reprfunc)0,
        /* tp_getattro       */ (getattrofunc)0,
        /* tp_setattro       */ (setattrofunc)0,
        /* tp_as_buffer      */ 0,
        /* tp_flags          */ Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        "Base type for Specification objects",
        /* tp_traverse       */ (traverseproc)0,
        /* tp_clear          */ (inquiry)0,
        /* tp_richcompare    */ (richcmpfunc)0,
        /* tp_weaklistoffset */ (long)0,
        /* tp_iter           */ (getiterfunc)0,
        /* tp_iternext       */ (iternextfunc)0,
        /* tp_methods        */ Spec_methods,
};

static PyObject *
OSD_descr_get(PyObject *self, PyObject *inst, PyObject *cls)
{
  PyObject *provides;

  if (inst == NULL)
    return getObjectSpecification(NULL, cls);

  provides = PyObject_GetAttr(inst, str__provides__);
  if (provides != NULL)
    return provides;
  PyErr_Clear();
  return implementedBy(NULL, cls);
}

static PyTypeObject OSDType = {
	PyObject_HEAD_INIT(NULL)
	/* ob_size           */ 0,
	/* tp_name           */ "_interface_coptimizations."
                                "ObjectSpecificationDescriptor",
	/* tp_basicsize      */ 0,
	/* tp_itemsize       */ 0,
	/* tp_dealloc        */ (destructor)0,
	/* tp_print          */ (printfunc)0,
	/* tp_getattr        */ (getattrfunc)0,
	/* tp_setattr        */ (setattrfunc)0,
	/* tp_compare        */ (cmpfunc)0,
	/* tp_repr           */ (reprfunc)0,
	/* tp_as_number      */ 0,
	/* tp_as_sequence    */ 0,
	/* tp_as_mapping     */ 0,
	/* tp_hash           */ (hashfunc)0,
	/* tp_call           */ (ternaryfunc)0,
	/* tp_str            */ (reprfunc)0,
        /* tp_getattro       */ (getattrofunc)0,
        /* tp_setattro       */ (setattrofunc)0,
        /* tp_as_buffer      */ 0,
        /* tp_flags          */ Py_TPFLAGS_DEFAULT
				| Py_TPFLAGS_BASETYPE ,
	"Object Specification Descriptor",
        /* tp_traverse       */ (traverseproc)0,
        /* tp_clear          */ (inquiry)0,
        /* tp_richcompare    */ (richcmpfunc)0,
        /* tp_weaklistoffset */ (long)0,
        /* tp_iter           */ (getiterfunc)0,
        /* tp_iternext       */ (iternextfunc)0,
        /* tp_methods        */ 0,
        /* tp_members        */ 0,
        /* tp_getset         */ 0,
        /* tp_base           */ 0,
        /* tp_dict           */ 0, /* internal use */
        /* tp_descr_get      */ (descrgetfunc)OSD_descr_get,
};

static PyObject *
CPB_descr_get(PyObject *self, PyObject *inst, PyObject *cls)
{
  PyObject *mycls, *implements;

  mycls = inst_attr(self, str_cls);
  if (mycls == NULL)
    return NULL;

  if (cls == mycls)
    {
      if (inst == NULL)
        {
          Py_INCREF(self);
          return OBJECT(self);
        }

      implements = inst_attr(self, str_implements);
      Py_XINCREF(implements);
      return implements;
    }
  
  PyErr_SetObject(PyExc_AttributeError, str__provides__);
  return NULL;
}

static PyTypeObject CPBType = {
	PyObject_HEAD_INIT(NULL)
	/* ob_size           */ 0,
	/* tp_name           */ "_interface_coptimizations."
                                "ClassProvidesBase",
	/* tp_basicsize      */ 0,
	/* tp_itemsize       */ 0,
	/* tp_dealloc        */ (destructor)0,
	/* tp_print          */ (printfunc)0,
	/* tp_getattr        */ (getattrfunc)0,
	/* tp_setattr        */ (setattrfunc)0,
	/* tp_compare        */ (cmpfunc)0,
	/* tp_repr           */ (reprfunc)0,
	/* tp_as_number      */ 0,
	/* tp_as_sequence    */ 0,
	/* tp_as_mapping     */ 0,
	/* tp_hash           */ (hashfunc)0,
	/* tp_call           */ (ternaryfunc)0,
	/* tp_str            */ (reprfunc)0,
        /* tp_getattro       */ (getattrofunc)0,
        /* tp_setattro       */ (setattrofunc)0,
        /* tp_as_buffer      */ 0,
        /* tp_flags          */ Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        "C Base class for ClassProvides",
        /* tp_traverse       */ (traverseproc)0,
        /* tp_clear          */ (inquiry)0,
        /* tp_richcompare    */ (richcmpfunc)0,
        /* tp_weaklistoffset */ (long)0,
        /* tp_iter           */ (getiterfunc)0,
        /* tp_iternext       */ (iternextfunc)0,
        /* tp_methods        */ 0,
        /* tp_members        */ 0,
        /* tp_getset         */ 0,
        /* tp_base           */ &SpecType,
        /* tp_dict           */ 0, /* internal use */
        /* tp_descr_get      */ (descrgetfunc)CPB_descr_get,
};


static struct PyMethodDef m_methods[] = {
  {"implementedBy", (PyCFunction)implementedBy, METH_O,
   "Interfaces implemented by instances of a class"},
  {"getObjectSpecification", (PyCFunction)getObjectSpecification, METH_O,
   "Get an object's interfaces (internal api)"},
  {"providedBy", (PyCFunction)providedBy, METH_O,
   "Get an object's interfaces"},
  
  {NULL,	 (PyCFunction)NULL, 0, NULL}		/* sentinel */
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
init_zope_interface_coptimizations(void)
{
  PyObject *m;

#define DEFINE_STRING(S) \
  if(! (str ## S = PyString_FromString(# S))) return

  DEFINE_STRING(__dict__);
  DEFINE_STRING(__implemented__);
  DEFINE_STRING(__provides__);
  DEFINE_STRING(__class__);
  DEFINE_STRING(__providedBy__);
  DEFINE_STRING(isOrExtends);
  DEFINE_STRING(extends);
  DEFINE_STRING(_implied);
  DEFINE_STRING(_implements);
  DEFINE_STRING(_cls);
#undef DEFINE_STRING
  
        
  /* Initialize types: */
  SpecType.tp_new = PyBaseObject_Type.tp_new;
  if (PyType_Ready(&SpecType) < 0)
    return;
  OSDType.tp_new = PyBaseObject_Type.tp_new;
  if (PyType_Ready(&OSDType) < 0)
    return;
  CPBType.tp_new = PyBaseObject_Type.tp_new;
  if (PyType_Ready(&CPBType) < 0)
    return;
  
  /* Create the module and add the functions */
  m = Py_InitModule3("_zope_interface_coptimizations", m_methods,
                     "C optimizations for zope.interface\n\n"
                     "$Id$");  
  if (m == NULL)
    return;
  
  /* Add types: */
  if (PyModule_AddObject(m, "SpecificationBase", (PyObject *)&SpecType) < 0)
    return;
  if (PyModule_AddObject(m, "ObjectSpecificationDescriptor", 
                         (PyObject *)&OSDType) < 0)
    return;
  if (PyModule_AddObject(m, "ClassProvidesBase", (PyObject *)&CPBType) < 0)
    return;
}

