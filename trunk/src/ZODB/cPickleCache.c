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
static char cPickleCache_doc_string[] = 
"Defines the PickleCache used by ZODB Connection objects.\n"
"\n"
"$Id: cPickleCache.c,v 1.35 2001/11/06 19:37:00 jeremy Exp $\n";

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define OBJECT(O) ((PyObject*)O)

/* Compute the current time in the units and range used for peristent
   objects. */
#define PER_TIME() ((long)(time(NULL) / 3)) % 65536

#define DONT_USE_CPERSISTENCECAPI
#include "cPersistence.h"
#include <time.h>

#undef Py_FindMethod

static PyObject *py_reload, *py__p_jar, *py__p_changed;

typedef struct {
  PyObject_HEAD
  PyObject *data;
  PyObject *jar;
  PyObject *setklassstate;
  int position;
  int cache_size;
  int cache_age;
  /* Cache statistics */
  int sum_deal;
  int sum_deac;
  double sum_age;
  int n, na;
  time_t last_check;		/* Time of last gc */
  double mean_age;
  double mean_deal;
  double mean_deac;
  double df, dfa;			/* Degees of freedom for above stats */
} ccobject;

#define WEIGHTING_PERIOD 600

/*
  How to compute weighted means?

  Assume we have two means, a current mean, M, and a mean as of some
  time d seconds in the past, Md.  The means have effective degrees
  of freedom, N, and Nd. Where Nd is adjusted by d is some fashion.
  The combined mean is (M*N+Md*Nd)/(N+Nd).  The degrees of freedom
  of the combined mean, Nc, is N+Nd.  Nd is computed by weighting
  an old degree of freedom with the weight: I/(I+d), where I is some
  suitably chosen constant, which we will call a "weighting period".
  
 */

staticforward PyTypeObject Cctype;

/* ---------------------------------------------------------------- */

static int 
gc_item(ccobject *self, PyObject *key, PyObject *v, long now, int dt)
{
  if (v && key)
    {
      self->n++;
      if(v->ob_refcnt <= 1)
	{
	  self->sum_deal++;
	  /* XXX The fact that this works will iterating over
	     self->data with PyDict_Next() is an accident of the
	     current Python dictionary implementation. */
	  return PyDict_DelItem(self->data, key);
	}

      if (dt >= 0 && 
	  (! PyExtensionClass_Check(v)) &&
	  ((cPersistentObject*)v)->jar == self->jar /* I'm paranoid */ &&
	  ((cPersistentObject*)v)->state == cPersistent_UPTODATE_STATE
	  )
	{
	  now -= ((cPersistentObject*)v)->atime;
	  if (now < 0) 
	      now += 65536;
	  self->na++;
	  self->sum_age += now;
	  if (now > dt)
	    {
	      /* We have a cPersistent object that hasn't been used in
		 a while.  Reinitialize it, hopefully freeing it's
		 state.
	      */
	      self->sum_deac++;
	      if (PyObject_SetAttr(v, py__p_changed, Py_None) < 0)
		PyErr_Clear();
	    }
	}
    }
  return 0;
}

static void
update_stats(ccobject *self, time_t now)
{
  double d, deal, deac;

  d=now-self->last_check;
  if(d < 1) return;

  self->df  *= WEIGHTING_PERIOD/(WEIGHTING_PERIOD+d);
  self->dfa *= WEIGHTING_PERIOD/(WEIGHTING_PERIOD+d);

  self->mean_age=((self->mean_age*self->dfa+self->sum_age)/
		  (self->dfa+self->na))*3;
  self->sum_age=0;

  deac=self->sum_deac/d;
  self->sum_deac=0;
  self->mean_deac=((self->mean_deac*self->dfa+deac)/
		   (self->dfa+self->na));
  self->sum_deac=0;

  self->dfa += self->na;
  self->na=0;

  deal=self->sum_deal/d;
  self->sum_deal=0;
  self->mean_deal=((self->mean_deal*self->df +deal)/
		   (self->df +self->n));
  self->sum_deal=0;

  self->df += self->n;
  self->n=0;

  self->last_check=now;
}

static int
fullgc(ccobject *self, int dt)
{
  PyObject *key, *v;
  int i;
  long now;

  if (self->cache_size < 1) 
      return 0;
  if ((i=PyDict_Size(self->data)) < 1) 
      return 0;

  now = PER_TIME();
  if (dt > 0)
      dt /= 3; 

  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if (gc_item(self, key, v, now, dt) < 0) 
	return -1;
  self->position=0;

  if(now-self->last_check > 1) update_stats(self, now);
  
  return 0;
}

static int
reallyfullgc(ccobject *self, int dt)
{
  PyObject *key, *v;
  int i, l, last;
  time_t now;

  if (self->cache_size < 1) 
      return 0;
  last = PyDict_Size(self->data);
  if (last < 0)
      return -1;

  now = PER_TIME();
  if (dt > 0)
      dt /= 3;

  /* First time through should get refcounts to 1 */
  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
      if (gc_item(self, key, v, now, dt) < 0) 
	  return -1;

  l = PyDict_Size(self->data);

  if (l < 0)
      return -1;

  while (l < last)
    {
      for (i=0; PyDict_Next(self->data, &i, &key, &v); )
	  if (gc_item(self, key, v, now, dt) < 0) 
	      return -1;
      last = l;
      l = PyDict_Size(self->data);
      if (l < 0)
	  return -1;
    }

  if(now-self->last_check > 1) update_stats(self, now);

  self->position=0;
  return 0;
}

static int
maybegc(ccobject *self, PyObject *thisv)
{
  int n, s, size, dt;
  long now;
  PyObject *key=0, *v=0;

  if (self->cache_size < 1) return 0;
  s=PyDict_Size(self->data);
  if (s < 1) return s;

  now = PER_TIME();

  size=self->cache_size;
  self->cache_size=0;

  /* Decide how many objects to look at */
  n=(s-size)/10;
  if (n < 3) n=3;

  /* Decide how much time to give them before deactivating them */
  s=8*size/s;
  if (s > 100) s=100;
  dt=(long)(self->cache_age*(0.2+0.1*s));

  /* Units are 3 seconds */
  dt /= 3; 

  if (dt < 1) dt=1;
  
  while (--n >= 0)
    {
      if (PyDict_Next(self->data, &(self->position), &key, &v))
	{
	  if (v != thisv && gc_item(self,key,v,now,dt) < 0)
	    {
	      self->cache_size=size;
	      return -1;
	    }
	}
      else
	self->position=0;
    }
  self->cache_size=size;

  if (now-self->last_check > 1) update_stats(self, now);

  return 0;
}

static PyObject *
cc_full_sweep(ccobject *self, PyObject *args)
{
  int dt = self->cache_age;
  UNLESS(PyArg_ParseTuple(args, "|i:full_sweep", &dt)) return NULL;
  if (dt < -1) 
    {
      PyErr_SetString(PyExc_ValueError, "age must be >= -1");
      return NULL;
    }
  if (fullgc(self, dt) == -1)
      return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
cc_reallyfull_sweep(ccobject *self, PyObject *args)
{
  int dt = self->cache_age;
  UNLESS(PyArg_ParseTuple(args, "|i:minimize", &dt)) return NULL;
  if (dt < -1) 
    {
      PyErr_SetString(PyExc_ValueError, "age must be >= -1");
      return NULL;
    }
  if (reallyfullgc(self, dt) == -1)
      return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
cc_incrgc(ccobject *self, PyObject *args)
{
  int n=1;

  UNLESS (PyArg_ParseTuple(args, "|i:incrgr",&n)) return NULL;

  for (; --n >= 0;)
    if(maybegc(self,NULL) < 0) return NULL;

  Py_INCREF(Py_None);
  return Py_None;
}

static void 
_invalidate(ccobject *self, PyObject *key)
{
  PyObject *v;

  if ((v=PyDict_GetItem(self->data, key)))
    {
      if (PyExtensionClass_Check(v))
	if(v->ob_refcnt <= 1)
	  {
	    self->sum_deal++;
	    if (PyDict_DelItem(self->data, key) < 0) 
	      PyErr_Clear();
	  }
	else
	  {
	    PyObject *t = PyTuple_New(1);
	    if (t)
	      {
		PyTuple_SET_ITEM(t, 0, v);
		v = PyObject_CallObject(self->setklassstate, t);
		PyTuple_SET_ITEM(t, 0, NULL);
		Py_DECREF(t);
	      }
	    else 
	      {
		v = t;
	      }
	    if (v) Py_DECREF(v);
	    else PyErr_Clear();
	  }
      else if (PyObject_DelAttr(v,py__p_changed) < 0)
	PyErr_Clear();
    }
  else 
    {
      if (PyErr_Occurred())
	PyErr_Clear();
    }
}

static PyObject *
cc_invalidate(ccobject *self, PyObject *args)
{
  PyObject *inv, *key, *v;
  int i;
  
  if (PyArg_ParseTuple(args, "O!", &PyDict_Type, &inv)) {
    for (i=0; PyDict_Next(inv, &i, &key, &v); ) 
      if (key==Py_None)
	{ /* Eek some nitwit invalidated everything! */
	  for (i=0; PyDict_Next(self->data, &i, &key, &v); )
	    _invalidate(self, key);
	  break;
	}
      else
	_invalidate(self, key);
    PyDict_Clear(inv);
  }
  else {
    PyErr_Clear();
    UNLESS (PyArg_ParseTuple(args, "O:invalidate", &inv)) return NULL;
    if (PyString_Check(inv))
      _invalidate(self, inv);
    else if (inv==Py_None)	/* All */
      for (i=0; PyDict_Next(self->data, &i, &key, &v); )
	_invalidate(self, key);
    else {
      int l;

      PyErr_Clear();
      if ((l=PyObject_Length(inv)) < 0) return NULL;
      for(i=l; --i >= 0; )
	{
	  UNLESS (key=PySequence_GetItem(inv, i)) return NULL;
	  _invalidate(self, key);
	  Py_DECREF(key);
	}
      PySequence_DelSlice(inv, 0, l);
    }
  }

  Py_INCREF(Py_None);
  return Py_None;
}
  
  
static PyObject *
cc_get(ccobject *self, PyObject *args)
{
  PyObject *r, *key, *d=0;

  UNLESS (PyArg_ParseTuple(args, "O|O:get", &key, &d)) return NULL;

  UNLESS (r=PyDict_GetItem(self->data, key))
    {
      if (d) 
	{
	  if (PyErr_Occurred())
	    PyErr_Clear();
	  r=d;
	}
      else
	{
	  PyErr_SetObject(PyExc_KeyError, key);
	  return NULL;
	}
    }

  Py_INCREF(r);
  return r;
}


static struct PyMethodDef cc_methods[] = {
  {"full_sweep", (PyCFunction)cc_full_sweep, METH_VARARGS,
   "full_sweep([age]) -- Perform a full sweep of the cache\n\n"
   "Make a single pass through the cache, removing any objects that are no\n"
   "longer referenced, and deactivating objects that have not been\n"
   "accessed in the number of seconds given by 'age'.  "
   "'age defaults to the cache age.\n"
   },
  {"minimize",	(PyCFunction)cc_reallyfull_sweep, METH_VARARGS,
   "minimize([age]) -- Remove as many objects as possible\n\n"
   "Make multiple passes through the cache, removing any objects that are no\n"
   "longer referenced, and deactivating objects that have not been\n"
   "accessed in the number of seconds given by 'age'.  'age defaults to 0.\n"
   },
  {"incrgc", (PyCFunction)cc_incrgc, METH_VARARGS,
   "incrgc() -- Perform incremental garbage collection"},
  {"invalidate", (PyCFunction)cc_invalidate, METH_VARARGS,
   "invalidate(oids) -- invalidate one, many, or all ids"},
  {"get", (PyCFunction)cc_get, METH_VARARGS,
   "get(key [, default]) -- get an item, or a default"},
  {NULL,		NULL}		/* sentinel */
};

static ccobject *
newccobject(PyObject *jar, int cache_size, int cache_age)
{
  ccobject *self;
  
  UNLESS(self = PyObject_NEW(ccobject, &Cctype)) return NULL;
  self->setklassstate=self->jar=NULL;
  if((self->data=PyDict_New()))
    {
      self->jar=jar; 
      Py_INCREF(jar);
      UNLESS (self->setklassstate=PyObject_GetAttrString(jar, "setklassstate"))
	return NULL;
      self->position=0;
      self->cache_size=cache_size;
      self->cache_age=cache_age < 1 ? 1 : cache_age;
      self->sum_deal=0;
      self->sum_deac=0;
      self->sum_age=0;
      self->mean_deal=0;
      self->mean_deac=0;
      self->mean_age=0;
      self->df=1;
      self->dfa=1;
      self->n=0;
      self->na=0;
      self->last_check=time(NULL);
      return self;
    }
  Py_DECREF(self);
  return NULL;
}

static void
cc_dealloc(ccobject *self)
{
  Py_XDECREF(self->data);
  Py_XDECREF(self->jar);
  Py_XDECREF(self->setklassstate);
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
      if(strcmp(name,"cache_mean_age")==0)
	return PyFloat_FromDouble(self->mean_age);
      if(strcmp(name,"cache_mean_deal")==0)
	return PyFloat_FromDouble(self->mean_deal);
      if(strcmp(name,"cache_mean_deac")==0)
	return PyFloat_FromDouble(self->mean_deac);
      if(strcmp(name,"cache_df")==0)
	return PyFloat_FromDouble(self->df);
      if(strcmp(name,"cache_dfa")==0)
	return PyFloat_FromDouble(self->dfa);
      if(strcmp(name,"cache_last_gc_time")==0)
	return PyFloat_FromDouble(self->last_check);
      if(strcmp(name,"cache_data")==0)
	{
	  Py_INCREF(self->data);
	  return self->data;
	}
    }
  if((*name=='h' && strcmp(name, "has_key")==0) ||
     (*name=='i' && strcmp(name, "items")==0) ||
     (*name=='k' && strcmp(name, "keys")==0)
     )
    return PyObject_GetAttrString(self->data, name);

  if((r=Py_FindMethod(cc_methods, (PyObject *)self, name)))
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
	  self->cache_size=v;
	  return 0;
	}
    }
  PyErr_SetString(PyExc_AttributeError, name);
  return -1;
}

static int
cc_length(ccobject *self)
{
  return PyObject_Length(self->data);
}
  
static PyObject *
cc_subscript(ccobject *self, PyObject *key)
{
  PyObject *r;

  UNLESS (r=PyDict_GetItem(self->data, key))
  {
    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
  }

  Py_INCREF(r);
  return r;
}

static int
cc_ass_sub(ccobject *self, PyObject *key, PyObject *v)
{
  if(v) 
    {
      if (PyExtensionClass_Check(v) 
	  ||
	  (PyExtensionInstance_Check(v) 
	   &&
	   (((PyExtensionClass*)(v->ob_type))->class_flags 
	    & PERSISTENT_TYPE_FLAG)
	   &&
	   (v->ob_type->tp_basicsize >= sizeof(cPersistentObject))
	   )
	  )	  
	return PyDict_SetItem(self->data, key, v);

      PyErr_SetString(PyExc_ValueError,
		      "Cache values must be persistent objects or classes.");
      return -1;
    }
  return PyDict_DelItem(self->data, key);
}

static PyMappingMethods cc_as_mapping = {
  (inquiry)cc_length,		/*mp_length*/
  (binaryfunc)cc_subscript,	/*mp_subscript*/
  (objobjargproc)cc_ass_sub,	/*mp_ass_subscript*/
};

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
  (reprfunc)0,   		/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &cc_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,  		/*tp_str*/

  /* Space for future expansion */
  0L,0L,0L,0L,
  ""
};

static PyObject *
cCM_new(PyObject *self, PyObject *args)
{
  int cache_size=100, cache_age=1000;
  PyObject *jar;

  UNLESS(PyArg_ParseTuple(args, "O|ii", &jar, &cache_size, &cache_age))
      return NULL;
  return (PyObject*)newccobject(jar, cache_size,cache_age);
}

static struct PyMethodDef cCM_methods[] = {
  {"PickleCache",(PyCFunction)cCM_new,	METH_VARARGS, ""},
  {NULL,		NULL}		/* sentinel */
};

void
initcPickleCache(void)
{
  PyObject *m, *d;
  char *rev="$Revision: 1.35 $";

  Cctype.ob_type=&PyType_Type;

  UNLESS(ExtensionClassImported) return;

  m = Py_InitModule4("cPickleCache", cCM_methods, cPickleCache_doc_string,
		     (PyObject*)NULL, PYTHON_API_VERSION);

  d = PyModule_GetDict(m);

  py_reload=PyString_FromString("reload");
  py__p_jar=PyString_FromString("_p_jar");
  py__p_changed=PyString_FromString("_p_changed");

  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
}
