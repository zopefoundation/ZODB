/*

  $Id: cPickleCache.c,v 1.12 1997/12/15 15:25:09 jim Exp $

  C implementation of a pickle jar cache.


     Copyright 

       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved.

***************************************************************************/
static char *what_string = "$Id: cPickleCache.c,v 1.12 1997/12/15 15:25:09 jim Exp $";

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define Py_ASSIGN(P,E) if(!PyObject_AssignExpression(&(P),(E))) return NULL
#define OBJECT(O) ((PyObject*)O)

#include "cPersistence.h"
#include <time.h>

#undef Py_FindMethod

static PyObject *py_reload, *py__p_jar, *py__p_atime, *py__p___reinit__;


/* Declarations for objects of type cCache */

typedef struct {
  PyObject_HEAD
  PyObject *data;
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

static PyObject *PATimeType=NULL;

/* ---------------------------------------------------------------- */


static int 
gc_item(ccobject *self, PyObject *key, PyObject *v, time_t now, time_t dt)
{
  time_t t;

  if(v && key)
    {
      self->n++;
      if(v->ob_type==(PyTypeObject*)PATimeType)
	{
	  if(((PATimeobject*)v)->object->ob_refcnt <= 1)
	    {
	      self->sum_deal++;
	      UNLESS(-1 != PyDict_DelItem(self->data, key)) return -1;
	    }
	  else
	    {
	      t=((PATimeobject*)v)->object->atime;
	      if(t != (time_t)1)
		{
		  self->na++;
		  t=now-t;
		  self->sum_age += t;
		  if((! dt || t > dt))
		    {
		      /* We have a cPersistent object that hasn't been used in
			 a while.  Reinitialize it, hopefully freeing it's
			 state.
			 */
		      v=(PyObject*)(((PATimeobject*)v)->object);
		      if(((cPersistentObject*)v)->state !=
			 cPersistent_UPTODATE_STATE) return 0;
		      self->sum_deac++;
		      if(key=PyObject_GetAttr(v,py__p___reinit__))
			{
			  ASSIGN(key,PyObject_CallObject(key,NULL));
			  UNLESS(key) return -1;
			  Py_DECREF(key);
			}
		      PyErr_Clear();
		    }
		}
	    }
	}
      else if(v->ob_refcnt <= 1)
	{
	  self->sum_deal++;
	  UNLESS(-1 != PyDict_DelItem(self->data, key)) return -1;
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
		  (self->dfa+self->na));
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
fullgc(ccobject *self, int idt)
{
  PyObject *key, *v;
  int i;
  time_t now, dt;

  if(self->cache_size < 1) return 0;
  i=PyDict_Size(self->data)-3/self->cache_size;
  if(i < 3) i=3;
  dt=self->cache_age*3/i;
  if(dt < 10) dt=10;
  now=time(NULL);
  if(idt) dt=idt;

  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if(gc_item(self,key,v,now,dt) < 0) return -1;
  self->position=0;

  if(now-self->last_check > 1) update_stats(self, now);
  
  return 0;
}

static PyObject *
ccitems(ccobject *self, PyObject *args)
{
  PyObject *r, *key, *v, *item=0;
  int i;

  UNLESS(PyArg_ParseTuple(args,"")) return NULL;
  UNLESS(r=PyList_New(0)) return NULL;

  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    {
      if(key && v)
	{
	  if(v->ob_type==(PyTypeObject*)PATimeType)
	    {
	      ASSIGN(item, Py_BuildValue("OO",key,((PATimeobject*)v)->object));
	    }
	  else
	    {
	      ASSIGN(item, Py_BuildValue("OO",key,v));
	    }
	  UNLESS(item) goto err;
	  if(PyList_Append(r,item) < 0) goto err;
	}
    }
  Py_XDECREF(item);
  return r;

err:
  Py_XDECREF(item);
  Py_DECREF(r);
  return NULL;
}

static int
reallyfullgc(ccobject *self, int dt)
{
  PyObject *key, *v;
  int i, l, last;
  time_t now;

  if((last=PyDict_Size(self->data)) < 0) return -1;

  now=time(NULL);
  /* First time through should get refcounts to 1 */
  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    if(gc_item(self,key,v,now,dt) < 0) return -1;

  if((l=PyDict_Size(self->data)) < 0) return -1;
  while(l < last)
    {
      for(i=0; PyDict_Next(self->data, &i, &key, &v); )
	if(gc_item(self,key,v,now,dt) < 0) return -1;
      last=l;
      if((l=PyDict_Size(self->data)) < 0) return -1;
    }

  if(now-self->last_check > 1) update_stats(self, now);

  self->position=0;
  return 0;
}

static int
maybegc(ccobject *self, PyObject *thisv)
{
  int n, s, size;
  time_t now,dt;
  PyObject *key=0, *v=0;

  /*printf("m");*/

  if(self->cache_size < 1) return 0;
  s=PyDict_Size(self->data)-3;
  if(s < self->cache_size) return 0;
  size=self->cache_size;
  self->cache_size=0;
  n=s/size;
  if(n < 3) n=3;
  dt=(long)(self->cache_age*(0.2+0.8*size/s));
  if(dt < 10) dt=10;
  now=time(NULL);
  
  while(--n >= 0)
    {
      if(PyDict_Next(self->data, &(self->position), &key, &v))
	{
	  if(v != thisv && gc_item(self,key,v,now,dt) < 0)
	    {
	      self->cache_size=size;
	      return -1;
	    }
	}
      else
	self->position=0;
    }
  self->cache_size=size;

  if(now-self->last_check > 1) update_stats(self, now);

  return 0;
}

static PyObject *
cc_full_sweep(ccobject *self, PyObject *args)
{
  int dt=0;
  UNLESS(PyArg_ParseTuple(args, "|i", &dt)) return NULL;
  UNLESS(-1 != fullgc(self,dt)) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
cc_reallyfull_sweep(ccobject *self, PyObject *args)
{
  int dt=0;
  UNLESS(PyArg_ParseTuple(args, "|i", &dt)) return NULL;
  UNLESS(-1 != reallyfullgc(self,dt)) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
cc_report(ccobject *self, PyObject *args)
{
  PyObject *key, *v, *t=0;
  int i;

  if(args) PyArg_ParseTuple(args,"|O", &t);
  
  for(i=0; PyDict_Next(self->data, &i, &key, &v); )
    {
       if(v->ob_type==(PyTypeObject*)PATimeType
	  && (
	      (t && OBJECT(((PATimeobject*)v)->object->ob_type) == t)
	      || ! t))
	 printf("%d\t%p\t%s\t%ld\t%d\t%ld\n",
		(((PATimeobject*)v)->object->oid),
		((PATimeobject*)v)->object,
		((PATimeobject*)v)->object->ob_type->tp_name,
		(long)(((PATimeobject*)v)->object->ob_refcnt),
		(((PATimeobject*)v)->object->state),
		(long)(((PATimeobject*)v)->object->atime) );
       else if((t && OBJECT(((PATimeobject*)v)->object->ob_type) == t)
	       || ! t)
	 printf("%d\t%p\t%s\t%ld\t%d\n",
		(((cPersistentObject*)v)->oid),
		v,
		v->ob_type->tp_name,
		(long)(v->ob_refcnt),
		(((cPersistentObject*)v)->state)
		);
    }
  if(args) Py_INCREF(Py_None);
  return Py_None;
}

static struct PyMethodDef cc_methods[] = {
  {"full_sweep",	(PyCFunction)cc_full_sweep,	1,
   "full_sweep([age]) -- Perform a full sweep of the cache\n\n"
   "Make a single pass through the cache, removing any objects that are no\n"
   "longer referenced, and deactivating objects that have not been\n"
   "accessed in the number of seconds given by 'age'.  "
   "'age defaults to the cache age.\n"
   },
  {"report",	(PyCFunction)cc_report,	1, ""},
  {"minimize",	(PyCFunction)cc_reallyfull_sweep,	1,
   "minimize([age]) -- Remove as many objects as possible\n\n"
   "Make multiple passes through the cache, removing any objects that are no\n"
   "longer referenced, and deactivating objects that have not been\n"
   "accessed in the number of seconds given by 'age'.  'age defaults to 0.\n"
   },
  {"items",	(PyCFunction)ccitems,	1,
   "items() -- Return the cache items."
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
    }
  if(*name=='h' && strcmp(name, "has_key")==0)
    return PyObject_GetAttrString(self->data, name);

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
	  self->cache_size=v;
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
  if(r->ob_type==(PyTypeObject *)PATimeType)
    {
      Py_DECREF(r);
      r=(PyObject*)(((PATimeobject*)r)->object);
      Py_INCREF(r);
    }
  return r;
}

static int
cc_ass_sub(ccobject *self, PyObject *key, PyObject *v)
{

  if(v)
    {
      int r;
      PyObject *t=0;

      /* Now get and save the access time */
      if(t=PyObject_GetAttr(v,py__p_atime))
	{
	  if(t->ob_type != (PyTypeObject *)PATimeType)
	    {
	      Py_DECREF(t);
	      t=0;
	    }
	  else
	    v=t;
	}
      else
	PyErr_Clear();

      r=PyDict_SetItem(self->data,key,v);
      Py_XDECREF(t);
      if(r < 0) return -1;
      return maybegc(self, v);
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
  char *rev="$Revision: 1.12 $";

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

  
#include "dcprotect.h"
  
  if (PyErr_Occurred()) Py_FatalError("can't initialize module cCache");
}

/******************************************************************************
 $Log: cPickleCache.c,v $
 Revision 1.12  1997/12/15 15:25:09  jim
 Cleaned up to avoid VC++ warnings.

 Revision 1.11  1997/12/10 22:20:43  jim
 Added has_key method.

 Revision 1.10  1997/07/18 14:30:18  jim
 Added reporting method for use during debugging.

 Revision 1.9  1997/07/16 20:18:40  jim
 *** empty log message ***

 Revision 1.8  1997/06/30 15:27:51  jim
 Added machinery to track cache statistics.
 Fixed bug in garbage collector, which had a nasty habit
 of activating inactive objects so that it could deactivate them.

 Revision 1.7  1997/05/30 14:29:47  jim
 Added new algorithm for adjusting cache age based on cache size.  Not,
 if the cache size gets really big, the cache age can drop to as low as
 20% of the configured cache age.  Also made the "minimize" method more
 agressive.

 Revision 1.6  1997/04/22 02:45:24  jim
 Changed object header layout and added sticky feature.

 Revision 1.5  1997/04/15 19:03:29  jim
 Fixed leak introduced in last revision. :-(

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

