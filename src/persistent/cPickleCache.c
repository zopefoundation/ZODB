/*

  $Id: cPickleCache.c,v 1.4 1997/04/11 19:13:21 jim Exp $

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
static char *what_string = "$Id: cPickleCache.c,v 1.4 1997/04/11 19:13:21 jim Exp $";

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define Py_ASSIGN(P,E) if(!PyObject_AssignExpression(&(P),(E))) return NULL

#include "Python.h"
#include <time.h>

static PyObject *py_reload, *py__p_jar, *py__p_atime, *py__p___reinit__;


/* Declarations for objects of type cCache */

typedef struct {
  PyObject_HEAD
  PyObject *data;
  int position;
  int cache_size;
  int cache_age;
} ccobject;

staticforward PyTypeObject Cctype;

static PyObject *PATimeType=NULL;

typedef struct {
  PyObject_HEAD
  time_t value;
} PATimeobject;

/* ---------------------------------------------------------------- */


static int 
gc_item(ccobject *self, PyObject *key, PyObject *v, time_t now, time_t dt)
{
  PyObject *atime;

  if(v && key)
    {
      if(PyTuple_GET_ITEM(v,0)->ob_refcnt <= 1)
	{
	  UNLESS(-1 != PyDict_DelItem(self->data, key)) return -1;
	}
      else if(! dt ||
	      ((atime=PyTuple_GET_ITEM(v,1)) &&
	       now-((PATimeobject*)atime)->value >dt))
	{
	  /* We have a cPersistent object that hasn't been used in
	     a while.  Reinitialize it, hopefully freeing it's state.
	     */
	  v=PyTuple_GET_ITEM(v,0);
	  if(key=PyObject_GetAttr(v,py__p___reinit__))
	    {
	      ASSIGN(key,PyObject_CallObject(key,NULL));
	      UNLESS(key) return -1;
	      Py_DECREF(key);
	    }
	  PyErr_Clear();
	}
    }
  return 0;
}

static int
fullgc(ccobject *self)
{
  PyObject *key, *v;
  int i;
  time_t now, dt;

  i=PyDict_Size(self->data)-3/self->cache_size;
  if(i < 3) i=3;
  dt=self->cache_age*3/i;
  if(dt < 10) dt=10;
  now=time(NULL);

  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if(gc_item(self,key,v,now,dt) < 0) return -1;
  self->position=0;
  return 0;
}

static int
reallyfullgc(ccobject *self)
{
  PyObject *key, *v;
  int i;

  /* First time through should get refcounts to 1 */
  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if(gc_item(self,key,v,0,0) < 0) return -1;
  /* Second time through should free many objects */
  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if(gc_item(self,key,v,0,0) < 0) return -1;

  self->position=0;
  return 0;
}

static int
maybegc(ccobject *self, PyObject *thisv)
{
  int n, s;
  time_t now,dt;
  PyObject *key=0, *v=0;

  s=PyDict_Size(self->data)-3;
  if(s < self->cache_size) return 0;
  n=s/self->cache_size;
  if(n < 3) n=3;
  dt=3 * self->cache_age/n;
  if(dt < 60) dt=60;
  now=time(NULL);
  
  while(--n >= 0)
    {
      if(PyDict_Next(self->data, &(self->position), &key, &v))
	{
	  if(v != thisv && gc_item(self,key,v,now,dt) < 0) return -1;
	}
      else
	self->position=0;
    }
  return 0;
}

static PyObject *
cc_full_sweep(ccobject *self, PyObject *args)
{
  UNLESS(PyArg_Parse(args, "")) return NULL;
  UNLESS(-1 != fullgc(self)) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
cc_reallyfull_sweep(ccobject *self, PyObject *args)
{
  UNLESS(PyArg_Parse(args, "")) return NULL;
  UNLESS(-1 != reallyfullgc(self)) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef cc_methods[] = {
  {"full_sweep",	(PyCFunction)cc_full_sweep,	0,
   "Perform a full sweep of the cache, looking for objects that can be removed"
   },
  {"minimize",	(PyCFunction)cc_reallyfull_sweep,	0,
   "Try to free as many objects as possible"
   },
  {NULL,		NULL}		/* sentinel */
};

/* ---------- */


static ccobject *
newccobject(int cache_size, int cache_age)
{
  ccobject *self;
  
  UNLESS(self = PyObject_NEW(ccobject, &Cctype)) return NULL;
  if(self->data=PyDict_New())
    {
      self->position=0;
      self->cache_size=cache_size < 1 ? 1 : cache_size;
      self->cache_age=cache_size < 1 ? 1 : cache_age;
      return self;
    }
  Py_DECREF(self);
  return NULL;
}


static void
cc_dealloc(ccobject *self)
{
  Py_XDECREF(self->data);
  PyMem_DEL(self);
}

static PyObject *
cc_getattr(ccobject *self, char *name)
{
  PyObject *r;

  if(*name=='c')
    {
      if(strcmp(name,"cache_age")==0)
	return PyInt_FromLong(self->cache_age);
      if(strcmp(name,"cache_size")==0)
	return PyInt_FromLong(self->cache_size);
    }

  if(r=Py_FindMethod(cc_methods, (PyObject *)self, name))
    return r;
  PyErr_Clear();
  return PyObject_GetAttrString(self->data, name);
}

static int
cc_setattr(ccobject *self, char *name, PyObject *value)
{
  if(value)
    {
      int v;

      if(strcmp(name,"cache_age")==0)
	{
	  UNLESS(PyArg_Parse(value,"i",&v)) return -1;
	  if(v > 0)self->cache_age=v;
	  return 0;
	}

      if(strcmp(name,"cache_size")==0)
	{
	  UNLESS(PyArg_Parse(value,"i",&v)) return -1;
	  if(v > 0)self->cache_size=v;
	  return 0;
	}
    }
  PyErr_SetString(PyExc_AttributeError, name);
  return -1;
}

static PyObject *
cc_repr(ccobject *self)
{
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
cc_length(ccobject *self)
{
  return PyObject_Length(self->data);
}
  
static PyObject *
cc_subscript(ccobject *self, PyObject *key)
{
  PyObject *r;

  UNLESS(r=PyObject_GetItem(self->data, key)) 
  {
    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
  }
  UNLESS(-1 != maybegc(self,r))
    {
      Py_DECREF(r);
      return NULL;
    }
  ASSIGN(r,PySequence_GetItem(r,0));
  return r;
}

static int
cc_ass_sub(ccobject *self, PyObject *key, PyObject *v)
{
  if(v)
    {
      PyObject *t;

      /* Create a tuple to hold object and object access time  */
      UNLESS(t=PyTuple_New(2)) return -1;

      /* Save value as first item in tuple */
      Py_INCREF(v);
      PyTuple_SET_ITEM(t,0,v);

      /* Now get and save the access time */
      if(v=PyObject_GetAttr(v,py__p_atime))
	{
	  if(v->ob_type == (PyTypeObject *)PATimeType)
	    PyTuple_SET_ITEM(t,1,v);
	  else
	    Py_DECREF(v);
	}
      else
	PyErr_Clear();

      UNLESS(-1 != PyDict_SetItem(self->data,key,t)) return -1;
      return maybegc(self, t);
      Py_DECREF(t);
    }
  else
    {
      UNLESS(-1 != PyDict_DelItem(self->data,key)) return -1;
      return maybegc(self, NULL);
    }
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
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "cPickleCache",		/*tp_name*/
  sizeof(ccobject),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /* methods */
  (destructor)cc_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)cc_getattr,	/*tp_getattr*/
  (setattrfunc)cc_setattr,	/*tp_setattr*/
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

static PyObject *
cCM_new(PyObject *self, PyObject *args)
{
  int cache_size=100, cache_age=1000;
  UNLESS(PyArg_ParseTuple(args, "|ii", &cache_size, &cache_age)) return NULL;
  return (PyObject*)newccobject(cache_size,cache_age);
}

/* List of methods defined in the module */

static struct PyMethodDef cCM_methods[] = {
  {"PickleCache",(PyCFunction)cCM_new,	1,
   "PickleCache([size,age]) -- Create a pickle jar cache\n\n"
   "The cache will attempt to garbage collect items when the cache size is\n"
   "greater than the given size, which defaults to 100.  Normally, objects\n"
   "are garbage collected if their reference count is one, meaning that\n"
   "they are only referenced by the cache.  In some cases, objects that\n"
   "have not been accessed in 'age' seconds may be partially garbage\n"
   "collected, meaning that most of their state is freed.\n"
  },
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
  char *rev="$Revision: 1.4 $";

  Cctype.ob_type=&PyType_Type;

  if(PATimeType=PyImport_ImportModule("cPersistence"))
    ASSIGN(PATimeType,PyObject_GetAttrString(PATimeType,"atimeType"));
  UNLESS(PATimeType) PyErr_Clear();

  m = Py_InitModule4("cPickleCache", cCM_methods,
		     cCache_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  d = PyModule_GetDict(m);

  py_reload=PyString_FromString("reload");
  py__p_jar=PyString_FromString("_p_jar");
  py__p_atime=PyString_FromString("_p_atime");
  py__p___reinit__=PyString_FromString("_p___reinit__");

  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));

  
  if (PyErr_Occurred()) Py_FatalError("can't initialize module cCache");
}

/******************************************************************************
 $Log: cPickleCache.c,v $
 Revision 1.4  1997/04/11 19:13:21  jim
 Added code to be more conservative about GCing.
 Fixed setattr bugs.

 Revision 1.3  1997/03/28 20:18:34  jim
 Simplified reinit logic.

 Revision 1.2  1997/03/11 20:48:38  jim
 Added object-deactivation support.  This only works with cPersistent
 objects.

 Revision 1.1  1997/02/17 18:39:02  jim
 *** empty log message ***


 ******************************************************************************/

