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
static char cPickleCache_doc_string[] = 
"Defines the PickleCache used by ZODB Connection objects.\n"
"\n"
"$Id: cPickleCache.c,v 1.40 2002/02/11 23:40:42 gvanrossum Exp $\n";

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
    if (!(v && key))
	return 0;
    self->n++;

    /* If there is at most one reference to this object, then the
       cache has the only reference.  It can be removed. */
    if (v->ob_refcnt <= 1) {
	self->sum_deal++;
	/* XXX The fact that this works will iterating over
	   self->data with PyDict_Next() is an accident of the
	   current Python dictionary implementation. */
	return PyDict_DelItem(self->data, key);
    }

    if (dt >= 0 && 
	(!PyExtensionClass_Check(v)) &&
	((cPersistentObject*)v)->jar == self->jar /* I'm paranoid */ &&
	((cPersistentObject*)v)->state == cPersistent_UPTODATE_STATE) {
	now -= ((cPersistentObject*)v)->atime;
	if (now < 0) 
	    now += 65536;
	self->na++;
	self->sum_age += now;
	if (now > dt) {
	    /* We have a cPersistent object that hasn't been used in
	       a while.  Reinitialize it, hopefully freeing it's
	       state.
	    */
	    self->sum_deac++;
	    if (PyObject_SetAttr(v, py__p_changed, Py_None) < 0)
		PyErr_Clear();
	}
    }
    return 0;
}

static void
update_stats(ccobject *self, time_t now)
{
    double d, deal, deac;

    d = now - self->last_check;
    if(d < 1) 
	return;

    self->df *= WEIGHTING_PERIOD / (WEIGHTING_PERIOD + d);
    self->dfa *= WEIGHTING_PERIOD / (WEIGHTING_PERIOD + d);

    self->mean_age = ((self->mean_age * self->dfa + self->sum_age)/
		      (self->dfa + self->na)) * 3;
    self->sum_age = 0;

    deac = self->sum_deac / d;
    self->sum_deac = 0;
    self->mean_deac = ((self->mean_deac * self->dfa+deac)/
		       (self->dfa + self->na));
    self->sum_deac = 0;

    self->dfa += self->na;
    self->na = 0;

    deal=self->sum_deal/d;
    self->sum_deal = 0;
    self->mean_deal = ((self->mean_deal * self->df + deal)/
		       (self->df +self->n));
    self->sum_deal = 0;
    
    self->df += self->n;
    self->n = 0;
    
    self->last_check = now;
}

static int
check_size(ccobject *self)
{
    if (self->cache_size < 1) 
	return 0;
    return PyDict_Size(self->data);
}

static int
gc_all_items(ccobject *self, int now, int dt)
{
    PyObject *key, *v;
    int i;

    for(i = 0; PyDict_Next(self->data, &i, &key, &v); )
	if (gc_item(self, key, v, now, dt) < 0) 
	    return -1;
    return 0;
}

static int
fullgc(ccobject *self, int dt)
{
    long now;

    if (check_size(self) <= 0)
	return 0;

    now = PER_TIME();
    dt /= 3; 

    if (gc_all_items(self, now, dt) < 0)
	return -1;
    self->position = 0;

    if (now - self->last_check > 1) 
	update_stats(self, now);
  
  return 0;
}

static int
reallyfullgc(ccobject *self, int dt)
{
    int l, last;
    time_t now;

    last = check_size(self);
    if (last <= 0)
	return 0;

    now = PER_TIME();
    /* Units are 3 seconds */
    dt /= 3; 

    /* First time through should get refcounts to 1 */
    if (gc_all_items(self, now, dt) < 0)
	return -1;

    l = PyDict_Size(self->data);
    if (l < 0)
	return -1;

    /* Now continue to collect until the size of the cache stops
       decreasing. */
    while (l < last) {
	if (gc_all_items(self, now, dt) < 0)
	    return -1;
	last = l;
	l = PyDict_Size(self->data);
	if (l < 0)
	    return -1;
    }
    
    if (now - self->last_check > 1) 
	update_stats(self, now);
    
    self->position = 0;
    return 0;
}

static int
maybegc(ccobject *self, PyObject *thisv)
{
    int n, s, size, dt;
    long now;
    PyObject *key=0, *v=0;

    s = check_size(self);
    if (s <= 0)
	return 0;

    now = PER_TIME();

    size = self->cache_size;
    self->cache_size = 0;

    /* Decide how many objects to look at */
    n = (s - size) / 10;
    if (n < 3) 
	n = 3;

    /* Decide how much time to give them before deactivating them */
    s = 8 * size / s;
    if (s > 100) 
	s = 100;
    dt = (long)(self->cache_age * (0.2 + 0.1 * s));

    /* Units are 3 seconds */
    dt /= 3; 
    
    while (--n >= 0) {
	if (PyDict_Next(self->data, &(self->position), &key, &v)) {
	    if (v != thisv && gc_item(self, key, v, now, dt) < 0) {
		self->cache_size=size;
		return -1;
	    }
	}
	else
	    self->position = 0;
    }
    self->cache_size = size;
    
    if (now - self->last_check > 1) 
	update_stats(self, now);

    return 0;
}

static PyObject *
cc_full_sweep(ccobject *self, PyObject *args)
{
    int dt = self->cache_age;
    if (!PyArg_ParseTuple(args, "|i:full_sweep", &dt)) 
	return NULL;
    if (dt < -1) {
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
    if (!PyArg_ParseTuple(args, "|i:minimize", &dt)) 
	return NULL;
    if (dt < -1) {
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
  int n = 1;

  if (!PyArg_ParseTuple(args, "|i:incrgr", &n)) 
      return NULL;

  for (; --n >= 0;)
      if (maybegc(self, NULL) < 0) 
	  return NULL;
  
  Py_INCREF(Py_None);
  return Py_None;
}

static void 
_invalidate(ccobject *self, PyObject *key)
{
    PyObject *v = PyDict_GetItem(self->data, key);

    if (!v)
	return;
    if (PyExtensionClass_Check(v))
	if (v->ob_refcnt <= 1) {
	    self->sum_deal++;
	    if (PyDict_DelItem(self->data, key) < 0) 
		PyErr_Clear();
	} else {
	    PyObject *t = PyTuple_New(1);
	    if (t) {
		PyTuple_SET_ITEM(t, 0, v);
		v = PyObject_CallObject(self->setklassstate, t);
		/* Set tuple element to NULL so that deallocating the
		   tuple does not decref t.
		 */
		PyTuple_SET_ITEM(t, 0, NULL);
		Py_DECREF(t);
	    } else 
		v = t;
	    if (v) 
		Py_DECREF(v);
	    else
		PyErr_Clear();
	}
    else if (PyObject_DelAttr(v, py__p_changed) < 0)
	PyErr_Clear();
}

static void
_invalidate_all(ccobject *self)
{
    PyObject *key, *v;
    int i;

    for (i = 0; PyDict_Next(self->data, &i, &key, &v); )
	_invalidate(self, key);
}

static PyObject *
cc_invalidate(ccobject *self, PyObject *args)
{
    PyObject *inv, *key, *v;
    int i;

    if (!PyArg_ParseTuple(args, "O:invalidate", &inv))
	return NULL;
    if (PyDict_Check(inv)) {
	for (i = 0; PyDict_Next(inv, &i, &key, &v); ) 
	    if (key == Py_None) { 
                /* Eek some nitwit invalidated everything! */
		_invalidate_all(self);
		break;
	    }
	    else
		_invalidate(self, key);
	PyDict_Clear(inv);
    } else if (PyString_Check(inv))
	_invalidate(self, inv);
    else if (inv == Py_None)	/* All */
	_invalidate_all(self);
    else {
	int l = PyObject_Length(inv);
	    
	if (l < 0)
	    return NULL;
	for (i = l; --i >= 0; ) {
	    key = PySequence_GetItem(inv, i);
	    if (!key)
		return NULL;
	    _invalidate(self, key);
	    Py_DECREF(key);
	}
	PySequence_DelSlice(inv, 0, l);
    }
    
    Py_INCREF(Py_None);
    return Py_None;
}
  
  
static PyObject *
cc_get(ccobject *self, PyObject *args)
{
    PyObject *r, *key, *d = NULL;

    if (!PyArg_ParseTuple(args, "O|O:get", &key, &d)) 
	return NULL;
    
    r = PyDict_GetItem(self->data, key);
    if (!r) {
	if (d)
	    r = d;
	else {
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
  
    self = PyObject_NEW(ccobject, &Cctype);
    if (!self)
	return NULL;
    self->setklassstate = self->jar = NULL;
    self->data = PyDict_New();
    if (self->data) {
	self->jar=jar; 
	Py_INCREF(jar);
	self->setklassstate = PyObject_GetAttrString(jar, "setklassstate");
	if (!self->setklassstate) {
	    Py_DECREF(jar);
	    Py_DECREF(self->data);
	    goto error;
	}
	self->position = 0;
	self->cache_size = cache_size;
	self->cache_age = cache_age < 1 ? 1 : cache_age;
	self->sum_deal = 0;
	self->sum_deac = 0;
	self->sum_age = 0;
	self->mean_deal = 0;
	self->mean_deac = 0;
	self->mean_age = 0;
	self->df = 1;
	self->dfa = 1;
	self->n = 0;
	self->na = 0;
	self->last_check = time(NULL);
	return self;
    }
 error:
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

    if (*name == 'c') {
	if(strcmp(name, "cache_age") == 0)
	    return PyInt_FromLong(self->cache_age);
	if(strcmp(name, "cache_size") == 0)
	    return PyInt_FromLong(self->cache_size);
	if(strcmp(name, "cache_mean_age") == 0)
	    return PyFloat_FromDouble(self->mean_age);
	if(strcmp(name, "cache_mean_deal") == 0)
	    return PyFloat_FromDouble(self->mean_deal);
	if(strcmp(name, "cache_mean_deac") == 0)
	    return PyFloat_FromDouble(self->mean_deac);
	if(strcmp(name, "cache_df") == 0)
	    return PyFloat_FromDouble(self->df);
	if(strcmp(name, "cache_dfa") == 0)
	    return PyFloat_FromDouble(self->dfa);
	if(strcmp(name, "cache_last_gc_time") == 0)
	    return PyFloat_FromDouble(self->last_check);
	if(strcmp(name, "cache_data") == 0) {
	    Py_INCREF(self->data);
	    return self->data;
	}
    }
    if ((strcmp(name, "has_key") == 0)
	|| (strcmp(name, "items") == 0)
	|| (strcmp(name, "keys") == 0))
	return PyObject_GetAttrString(self->data, name);
    
    r = Py_FindMethod(cc_methods, (PyObject *)self, name);
    if (!r) {
	PyErr_Clear();
	return PyObject_GetAttrString(self->data, name);
    }
    return r;
}

static int
cc_setattr(ccobject *self, char *name, PyObject *value)
{
  if (value) {
      int v;

      if (strcmp(name, "cache_age") == 0) {
	  v = PyInt_AsLong(value);
	  if (v == -1 && PyErr_Occurred())
	      return -1;
	  if (v > 0)
	      self->cache_age = v;
	  return 0;
      }

      if (strcmp(name, "cache_size") == 0) {
	  v = PyInt_AsLong(value);
	  if (v == -1 && PyErr_Occurred())
	      return -1;
	  self->cache_size = v;
	  return 0;
      }
  }
  PyErr_SetString(PyExc_AttributeError, name);
  return -1;
}

static int
cc_length(ccobject *self)
{
    return PyDict_Size(self->data);
}
  
static PyObject *
cc_subscript(ccobject *self, PyObject *key)
{
  PyObject *r;

  r = PyDict_GetItem(self->data, key);
  if (!r) {
      PyErr_SetObject(PyExc_KeyError, key);
      return NULL;
  }

  Py_INCREF(r);
  return r;
}

static int
cc_ass_sub(ccobject *self, PyObject *key, PyObject *v)
{
  if (v) {
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
};

static PyObject *
cCM_new(PyObject *self, PyObject *args)
{
  int cache_size=100, cache_age=1000;
  PyObject *jar;

  if (!PyArg_ParseTuple(args, "O|ii", &jar, &cache_size, &cache_age))
      return NULL;
  return (PyObject *)newccobject(jar, cache_size, cache_age);
}

static struct PyMethodDef cCM_methods[] = {
  {"PickleCache",(PyCFunction)cCM_new,	METH_VARARGS, ""},
  {NULL,		NULL}		/* sentinel */
};

void
initcPickleCache(void)
{
  PyObject *m;

  Cctype.ob_type = &PyType_Type;

  if (!ExtensionClassImported) 
      return;

  m = Py_InitModule4("cPickleCache", cCM_methods, cPickleCache_doc_string,
		     (PyObject*)NULL, PYTHON_API_VERSION);

  py_reload = PyString_InternFromString("reload");
  py__p_jar = PyString_InternFromString("_p_jar");
  py__p_changed = PyString_InternFromString("_p_changed");
}
