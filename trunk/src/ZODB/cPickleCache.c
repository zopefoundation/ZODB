/*

  $Id: cPickleCache.c,v 1.1 1997/02/17 18:39:02 jim Exp $

  C implementation of a pickle jar cache.


     Copyright 

       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved.  Copyright in this software is owned by DCLC,
       unless otherwise indicated. Permission to use, copy and
       distribute this software is hereby granted, provided that the
       above copyright notice appear in all copies and that both that
       copyright notice and this permission notice appear. Note that
       any product, process or technology described in this software
       may be the subject of other Intellectual Property rights
       reserved by Digital Creations, L.C. and are not licensed
       hereunder.

     Trademarks 

       Digital Creations & DCLC, are trademarks of Digital Creations, L.C..
       All other trademarks are owned by their respective companies. 

     No Warranty 

       The software is provided "as is" without warranty of any kind,
       either express or implied, including, but not limited to, the
       implied warranties of merchantability, fitness for a particular
       purpose, or non-infringement. This software could include
       technical inaccuracies or typographical errors. Changes are
       periodically made to the software; these changes will be
       incorporated in new editions of the software. DCLC may make
       improvements and/or changes in this software at any time
       without notice.

     Limitation Of Liability 

       In no event will DCLC be liable for direct, indirect, special,
       incidental, economic, cover, or consequential damages arising
       out of the use of or inability to use this software even if
       advised of the possibility of such damages. Some states do not
       allow the exclusion or limitation of implied warranties or
       limitation of liability for incidental or consequential
       damages, so the above limitation or exclusion may not apply to
       you.

    If you have questions regarding this software,
    contact:
   
      Jim Fulton, jim@digicool.com
      Digital Creations L.C.  
   
      (540) 371-6909

***************************************************************************/
static char *what_string = "$Id: cPickleCache.c,v 1.1 1997/02/17 18:39:02 jim Exp $";

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define Py_ASSIGN(P,E) if(!PyObject_AssignExpression(&(P),(E))) return NULL

#ifdef __cplusplus
#define ARG(T,N) T N
#define ARGDECL(T,N)
#else
#define ARG(T,N) N
#define ARGDECL(T,N) T N;
#endif

#include "Python.h"

/* Declarations for objects of type cCache */

typedef struct {
  PyObject_HEAD
  PyObject *data;
  int position;
  int cache_size;
} ccobject;

staticforward PyTypeObject Cctype;

/* ---------------------------------------------------------------- */

static char cc_full_sweep__doc__[] = 
"Perform a full sweep of the cache, looking for objects that can be removed"
;

static int
fullgc(ARG(ccobject *, self))
     ARGDECL(ccobject *, self)
{
  PyObject *key, *v;
  int i, l;

  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    {
      if(v->ob_refcnt <= 1)
	UNLESS(-1 != PyDict_DelItem(self->data, key)) return -1;
    }
  self->position=0;
  return 0;
}

static int
maybegc(ARG(ccobject *, self))
     ARGDECL(ccobject *, self)
{
  int n, s, p;
  int r;
  PyObject *key=0, *v=0;

  s=PyDict_Size(self->data)-3;
  if(s < self->cache_size) return 0;
  n=s/self->cache_size;
  if(n < 3) n=3;
  while(--n >= 0)
    {
      if(PyDict_Next(self->data, &(self->position), &key, &v))
	{
	  if(v && key && v->ob_refcnt <= 1)
	    UNLESS(-1 != PyDict_DelItem(self->data, key)) return -1;
	}
      else
	self->position=0;
    }
  return 0;
}

static PyObject *
cc_full_sweep(ARG(ccobject *, self), ARG(PyObject *, args))
     ARGDECL(ccobject *, self)
     ARGDECL(PyObject *, args)
{
  UNLESS(PyArg_Parse(args, "")) return NULL;
  UNLESS(-1 != fullgc(self)) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef cc_methods[] = {
  {"full_sweep",	(PyCFunction)cc_full_sweep,	0,	cc_full_sweep__doc__},
  {NULL,		NULL}		/* sentinel */
};

/* ---------- */


static ccobject *
newccobject(ARG(int,cache_size))
     ARGDECL(int,cache_size)
{
  ccobject *self;
  
  UNLESS(self = PyObject_NEW(ccobject, &Cctype)) return NULL;
  if(self->data=PyDict_New())
    {
      self->position=0;
      self->cache_size=cache_size < 1 ? 1 : cache_size;
      return self;
    }
  Py_DECREF(self);
  return NULL;
}


static void
cc_dealloc(ARG(ccobject *, self))
     ARGDECL(ccobject *, self)
{
  Py_XDECREF(self->data);
  PyMem_DEL(self);
}

static PyObject *
cc_getattr(ARG(ccobject *, self), ARG(char *, name))
     ARGDECL(ccobject *, self)
     ARGDECL(char *,           name)
{
  PyObject *r;
  if(r=Py_FindMethod(cc_methods, (PyObject *)self, name))
    return r;
  PyErr_Clear();
  return PyObject_GetAttrString(self->data, name);
}

static PyObject *
cc_repr(ARG(ccobject *, self))
     ARGDECL(ccobject *, self)
{
  PyObject *s;

  return PyObject_Repr(self->data);
}

static PyObject *
cc_str(self)
	ccobject *self;
{
  return PyObject_Str(self->data);
}


/* Code to access cCache objects as mappings */

static int
cc_length(ARG(ccobject *, self))
     ARGDECL(ccobject *, self)
{
  return PyObject_Length(self->data);
}
  
static PyObject *
cc_subscript(ARG(ccobject *, self), ARG(PyObject *, key))
     ARGDECL(ccobject *, self)
     ARGDECL(PyObject *, key)
{
  PyObject *r;

  UNLESS(r=PyObject_GetItem(self->data, key)) 
  {
    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
  }
  UNLESS(-1 != maybegc(self))
    {
      Py_DECREF(r);
      return NULL;
    }
  return r;
}

static int
cc_ass_sub(ARG(ccobject *, self),
		 ARG(PyObject *, key), ARG(PyObject *, v))
     ARGDECL(ccobject *, self)
     ARGDECL(PyObject *,       key)
     ARGDECL(PyObject *,       v)
{
  if(v)
    {
      UNLESS(-1 != PyDict_SetItem(self->data,key, v)) return -1;
    }
  else
    {
      UNLESS(-1 != PyDict_DelItem(self->data,key)) return -1;
    }
  return maybegc(self);
}

static PyMappingMethods cc_as_mapping = {
  (inquiry)cc_length,		/*mp_length*/
  (binaryfunc)cc_subscript,	/*mp_subscript*/
  (objobjargproc)cc_ass_sub,	/*mp_ass_subscript*/
};

/* -------------------------------------------------------- */

static char Cctype__doc__[] = 
""
;

static PyTypeObject Cctype = {
  PyObject_HEAD_INIT(&PyType_Type)
  0,				/*ob_size*/
  "cPickleCache",		/*tp_name*/
  sizeof(ccobject),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /* methods */
  (destructor)cc_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)cc_getattr,	/*tp_getattr*/
  (setattrfunc)0,		/*tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)cc_repr,		/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &cc_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)cc_str,		/*tp_str*/

  /* Space for future expansion */
  0L,0L,0L,0L,
  Cctype__doc__ /* Documentation string */
};

/* End of code for cCache objects */
/* -------------------------------------------------------- */


static char cCM_new__doc__[] =
""
;

static PyObject *
cCM_new(ARG(PyObject *, self), ARG(PyObject *, args))
     ARGDECL(PyObject *, self)	/* Not used */
     ARGDECL(PyObject *, args)
{
  int cache_size=100;
  UNLESS(PyArg_ParseTuple(args, "|i", &cache_size)) return NULL;
  return (PyObject*)newccobject(cache_size);
}

/* List of methods defined in the module */

static struct PyMethodDef cCM_methods[] = {
  {"PickleCache",(PyCFunction)cCM_new,	1,	cCM_new__doc__},
  
  {NULL,		NULL}		/* sentinel */
};


/* Initialization function for the module (*must* be called initcCache) */

static char cCache_module_documentation[] = 
""
;

void
initcPickleCache()
{
  PyObject *m, *d;
  char *rev="$Revision: 1.1 $";

  /* Create the module and add the functions */
  m = Py_InitModule4("cPickleCache", cCM_methods,
		     cCache_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);
  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  
  if (PyErr_Occurred()) Py_FatalError("can't initialize module cCache");
}

/******************************************************************************
 $Log: cPickleCache.c,v $
 Revision 1.1  1997/02/17 18:39:02  jim
 *** empty log message ***


 ******************************************************************************/

