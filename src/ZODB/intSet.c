/***********************************************************
     Copyright 

       Copyright 1997 Digital Creations, L.L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved. 

******************************************************************/


static char intSet_module_documentation[] = 
""
"\n$Id: intSet.c,v 1.3 1997/10/01 02:45:58 jim Exp $"
;

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <malloc.h>
#include <time.h>
#include "cPersistence.h"

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define RETURN_NONE Py_INCREF(Py_None); return Py_None

#define MIN_INTSET_ALLOC 8

#define INTSET_DATA_TYPE int
#define INTSET_DATA_FLAG "l"

typedef struct {
  cPersistent_HEAD
  int size, len;
  INTSET_DATA_TYPE *data;
} intSet;


staticforward PyExtensionClass intSetType;

#define OBJECT(O) ((PyObject*)(O))
#define INTSET(O) ((intSet*)(O))

static PyObject *
_PER_RETURN(intSet *self, PyObject *r)
{
  PER_ALLOW_DEACTIVATION(self);
  return r;
}

static int
_PER_INT_RETURN(intSet *self, int r)
{
  PER_ALLOW_DEACTIVATION(self);
  return r;
}

/* We want to be sticky most of the time */
#define PER_RETURN(O,R) R
#define PER_INT_RETURN(O,R) R
#undef PER_ALLOW_DEACTIVATION
#define PER_ALLOW_DEACTIVATION(O)

static PyObject *
intSet_has_key(intSet *self, PyObject *args)
{
  int min, max, i, l;
  INTSET_DATA_TYPE k, key;

  UNLESS(PyArg_ParseTuple(args,INTSET_DATA_FLAG,&key)) return NULL;

  PER_USE_OR_RETURN(self, NULL);
  
  for(min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      k=self->data[i];
      if(k == key) return PER_RETURN(self, PyInt_FromLong(1));
      if(k > key) max=i;
      else min=i;
    }
  return PER_RETURN(self, PyInt_FromLong(0));
}


static int
intSet_grow(intSet *self, int l)
{
  int g;
  INTSET_DATA_TYPE *data;

  if(self->data)
    {
      g=self->size*2;
      if(g < l) g=l;
      UNLESS(data=realloc(self->data, sizeof(INTSET_DATA_TYPE)*g))
	{
	  PyErr_NoMemory();
	  return -1;
	}
      self->data=data;
      self->size=g;
    }
  else
    {
      g=l < MIN_INTSET_ALLOC ? MIN_INTSET_ALLOC : l;
      UNLESS(self->data=malloc(sizeof(INTSET_DATA_TYPE)*g))
	{
	  PyErr_NoMemory();
	  return -1;
	}
      self->size=g;
    }
  return 0;
}  

static INTSET_DATA_TYPE
intSet_modify(intSet *self, INTSET_DATA_TYPE ikey, int add)
{
  int min, max, i, l;
  INTSET_DATA_TYPE *data, k;
  
  PER_USE_OR_RETURN(self, NULL);

  data=self->data;

  for(min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      k=data[i];
      if(k == ikey) 
	{
	  if(! add)
	    {
	      data+=i;
	      self->len--;
	      if(i < (self->len))
		memmove(data, data+1, (self->len-i)*sizeof(INTSET_DATA_TYPE));
	      if(PER_CHANGED(self) < 0) return PER_INT_RETURN(self, -1);
	    }

	  return PER_INT_RETURN(self, 0);
	}
      if(k > ikey) max=i;
      else min=i;
    }
  if(!add) return PER_INT_RETURN(self, 0);
  if(self->len >= self->size && intSet_grow(self,self->len+1) < 0)
    return PER_INT_RETURN(self, -1);
  if(max != i) i++;
  data=self->data+i;
  if(self->len > i)
    memmove(data+1,data,(self->len-i)*sizeof(INTSET_DATA_TYPE));
  *data=ikey;
  self->len++;
  if(PER_CHANGED(self) < 0) return PER_INT_RETURN(self, -1);
  return PER_INT_RETURN(self, ikey);
}

static PyObject *
intSet_insert(intSet *self, PyObject *args)
{
  INTSET_DATA_TYPE key;

  UNLESS(PyArg_ParseTuple(args,INTSET_DATA_FLAG,&key)) return NULL;

  if(intSet_modify(self, key, 1) < 0) return NULL;
  RETURN_NONE;
}

static PyObject *
intSet_remove(intSet *self, PyObject *args)
{
  INTSET_DATA_TYPE key;

  UNLESS(PyArg_ParseTuple(args,INTSET_DATA_FLAG,&key)) return NULL;

  if(intSet_modify(self, key, 0) < 0) return NULL;
  RETURN_NONE;
}
  
static PyObject *
intSet_clear(intSet *self, PyObject *args)
{
  self->len=0;
  if(PER_CHANGED(self) < 0) return PER_RETURN(self, NULL);
  RETURN_NONE;
}

static PyObject *
intSet___getstate__(intSet *self, PyObject *args)
{
  PyObject *r=0;
  int i, l;
  char *c;
  INTSET_DATA_TYPE *d;

  PER_USE_OR_RETURN(self, NULL);

  l=self->len;
  UNLESS(r=PyString_FromStringAndSize(NULL,l*4)) goto err;
  UNLESS(c=PyString_AsString(r)) goto err;
  d=self->data;
  for(i=0; i < l; i++, *d++)
    {
      *c++ = (int)( *d        & 0xff);
      *c++ = (int)((*d >> 8)  & 0xff);
      *c++ = (int)((*d >> 16) & 0xff);
      *c++ = (int)((*d >> 24) & 0xff);
    }
  
  return PER_RETURN(self, r);

err:
  Py_DECREF(r);
  return PER_RETURN(self, NULL);
}

static PyObject *
intSet___setstate__(intSet *self, PyObject *args)
{
  PyObject *data;
  int i, l, v;
  char *c;
  INTSET_DATA_TYPE k;

  PER_PREVENT_DEACTIVATION(self); 

  UNLESS(PyArg_ParseTuple(args,"O",&data)) return PER_RETURN(self, NULL);
  UNLESS(c=PyString_AsString(data)) return PER_RETURN(self, NULL);

  if((l=PyString_Size(data)) < 0) return PER_RETURN(self, NULL);
  l/=4;

  intSet_clear(self, NULL);
  if(l > self->size && intSet_grow(self,l) < 0)
    return PER_RETURN(self, NULL);

  PyErr_Clear();

  for(i=0; i < l; i++)
    {
      v  = ((int)(unsigned char)*c++)      ;
      v |= ((int)(unsigned char)*c++) <<  8;
      v |= ((int)(unsigned char)*c++) << 16;
      v |= ((int)(unsigned char)*c++) << 24;
      self->data[i]=v;
    }

  self->len=l;

  Py_INCREF(Py_None);
  return PER_RETURN(self, Py_None);
}

static PyObject *
intSet_set_operation(intSet *self, PyObject *other,
		     int cpysrc, int cpyboth, int cpyoth)
{
  intSet *r=0, *o;
  int i, l, io, lo, ir;
  INTSET_DATA_TYPE *d, *od, v, vo, dif;
  
  if(other->ob_type != self->ob_type)
    {
      PyErr_SetString(PyExc_TypeError,
		      "intSet set operations require same-type operands");
      return NULL;
    }
  o=INTSET(other);

  PER_USE_OR_RETURN(self, NULL);
  PER_USE_OR_RETURN(other, NULL);

  od=o->data;

  d=self->data;

  UNLESS(r=INTSET(PyObject_CallObject(OBJECT(self->ob_type), NULL)))
    goto err;

  for(i=0, l=self->len, io=0, lo=o->len; i < l && io < lo; )
    {
      v=d[i];
      vo=od[io];
      if(v < vo)
	{
	  if(cpysrc)
	    {
	      if(r->len >= r->size && intSet_grow(r,0) < 0) goto err;
	      r->data[r->len]=v;
	      r->len++;
	    }
	  i++;
	}
      else if(v==vo)
	{
	  if(cpyboth)
	    {
	      if(r->len >= r->size && intSet_grow(r,0) < 0) goto err;
	      r->data[r->len]=v;
	      r->len++;
	    }
	  i++;
	  io++;
	}
      else
	{
	  if(cpyoth)
	    {
	      if(r->len >= r->size && intSet_grow(r,0) < 0) goto err;
	      r->data[r->len]=vo;
	      r->len++;
	    }
	  io++;
	}
    }
  if(cpysrc && i < l)
    {
      l-=i;
      if(r->len+l > r->size && intSet_grow(r,r->len+l) < 0) goto err;
      memcpy(r->data+r->len, d+i, l*sizeof(INTSET_DATA_TYPE));
      r->len += l;
    }
  else if(cpyoth && io < lo)
    {
      lo-=io;
      if(r->len+lo > r->size && intSet_grow(r,r->len+lo) < 0) goto err;
      memcpy(r->data+r->len, od+io, lo*sizeof(INTSET_DATA_TYPE));
      r->len += lo;
    }

  return OBJECT(r);

err:
  PER_ALLOW_DEACTIVATION(self);
  PER_ALLOW_DEACTIVATION(o);
  Py_DECREF(r);
  return NULL;
}

static PyObject *
intSet_add(intSet *self, PyObject *other)
{
  return intSet_set_operation(self,other,1,1,1);
}

static PyObject *
intSet_union(intSet *self, PyObject *args)
{
  PyObject *other;

  UNLESS(PyArg_ParseTuple(args,"O",&other)) return NULL;
  return intSet_set_operation(self,other,1,1,1);
}

static PyObject *
intSet_intersection(intSet *self, PyObject *args)
{
  PyObject *other;

  UNLESS(PyArg_ParseTuple(args,"O",&other)) return NULL;
  return intSet_set_operation(self,other,0,1,0);
}

static PyObject *
intSet_difference(intSet *self, PyObject *args)
{
  PyObject *other;

  UNLESS(PyArg_ParseTuple(args,"O",&other)) return NULL;
  return intSet_set_operation(self,other,1,0,0);
}

static PyObject *
intSet__p___reinit__(intSet *self, PyObject *args)
{
  /* Note that this implementation is broken, in that it doesn't
     account for subclass needs. */
  Py_INCREF(Py_None);
  return Py_None;
}


static struct PyMethodDef intSet_methods[] = {
  {"has_key",	(PyCFunction)intSet_has_key,	METH_VARARGS,
   "has_key(id) -- Test whether the set has the given id"},
  {"insert",	(PyCFunction)intSet_insert,	METH_VARARGS,
   "insert(id,[ignored]) -- Add an id to the set"},
  {"remove",	(PyCFunction)intSet_remove,	METH_VARARGS,
   "remove(id) -- Remove an id from the set"},
  {"clear",	(PyCFunction)intSet_clear,	METH_VARARGS,
   "clear() -- Remove all of the ids from the set"},
  {"union",	(PyCFunction)intSet_union,	METH_VARARGS,
   "union(other) -- Return the union of the set with another set"},
  {"intersection",	(PyCFunction)intSet_intersection, METH_VARARGS,
   "intersection(other) -- "
   "Return the intersection of the set with another set"},
  {"difference",	(PyCFunction)intSet_difference,	METH_VARARGS,
   "difference(other) -- Return the difference of the set with another set"
  },
  {"__getstate__",	(PyCFunction)intSet___getstate__, METH_VARARGS,
   "__getstate__() -- get the persistent state"},  	 
  {"__setstate__",	(PyCFunction)intSet___setstate__, METH_VARARGS,
   "__setstate__() -- set the persistent state"},  	 
  {"_p___reinit__",	(PyCFunction)intSet__p___reinit__, METH_VARARGS,
   "_p___reinit__(oid,jar,copy) -- Reinitialize from a newly created copy"},
  {NULL,		NULL}		/* sentinel */
};

static void
intSet_dealloc(intSet *self)
{
  free(self->data);
  PER_DEL(self);
  PyMem_DEL(self);
}

static PyObject *
intSet_getattr(intSet *self, PyObject *name)
{
  return Py_FindAttr((PyObject *)self, name);
}

/* Code to handle accessing intSet objects as sequence objects */

static int
intSet_length(intSet *self)
{
  PER_USE_OR_RETURN(self,-1);
  return PER_INT_RETURN(self,self->len);
}

static PyObject *
intSet_repeat(intSet *self, int n)
{
  PyErr_SetString(PyExc_TypeError,
		  "intSet objects do not support repetition");
  return NULL;
}

static PyObject *
intSet_item(intSet *self, int i)
{
  PyObject *e;

  PER_USE_OR_RETURN(self,NULL);

  if(i >= 0 && i < self->len)
    return PER_RETURN(self,PyInt_FromLong(self->data[i]));
  UNLESS(e=PyInt_FromLong(i)) goto err;
  PyErr_SetObject(PyExc_IndexError, e);
  Py_DECREF(e);
err:
  PER_ALLOW_DEACTIVATION(self)
  return NULL;
}

static PyObject *
intSet_slice(intSet *self, int ilow, int ihigh)
{
  PyErr_SetString(PyExc_TypeError,
		  "intSet objects do not support slicing");
  return NULL;
}

static int
intSet_ass_item(intSet *self, int i, PyObject *v)
{
  PyErr_SetString(PyExc_TypeError,
		  "intSet objects do not support item assignment");
  return -1;
}

static int
intSet_ass_slice(PyListObject *self, int ilow, int ihigh, PyObject *v)
{
  PyErr_SetString(PyExc_TypeError,
		  "intSet objects do not support slice assignment");
  return -1;
}

static PySequenceMethods intSet_as_sequence = {
	(inquiry)intSet_length,		/*sq_length*/
	(binaryfunc)intSet_add,		/*sq_concat*/
	(intargfunc)intSet_repeat,		/*sq_repeat*/
	(intargfunc)intSet_item,		/*sq_item*/
	(intintargfunc)intSet_slice,		/*sq_slice*/
	(intobjargproc)intSet_ass_item,	/*sq_ass_item*/
	(intintobjargproc)intSet_ass_slice,	/*sq_ass_slice*/
};

static PyExtensionClass intSetType = {
  PyObject_HEAD_INIT(NULL)
  0,			/*ob_size*/
  "intSet",		/*tp_name*/
  sizeof(intSet),	/*tp_basicsize*/
  0,			/*tp_itemsize*/
  /* methods */
  (destructor)intSet_dealloc,	/*tp_dealloc*/
  (printfunc)0,		/*tp_print*/
  (getattrfunc)0,	/*obsolete tp_getattr*/
  (setattrfunc)0,	/*obsolete tp_setattr*/
  (cmpfunc)0,		/*tp_compare*/
  (reprfunc)0,		/*tp_repr*/
  0,			/*tp_as_number*/
  &intSet_as_sequence,	/*tp_as_sequence*/
  0,			/*tp_as_mapping*/
  (hashfunc)0,		/*tp_hash*/
  (ternaryfunc)0,	/*tp_call*/
  (reprfunc)0,		/*tp_str*/
  (getattrofunc)intSet_getattr,			/*tp_getattro*/
  0,			/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "A set of integers", 
  METHOD_CHAIN(intSet_methods),
};

static struct PyMethodDef module_methods[] = {
  {NULL,		NULL}		/* sentinel */
};

void
initintSet()
{
  PyObject *m, *d;
  char *rev="$Revision: 1.3 $";

  UNLESS(ExtensionClassImported) return;

  if(cPersistenceCAPI=PyCObject_Import("cPersistence","CAPI"))
    {
      static PyMethodChain m;
      m.methods=intSetType.methods.methods;
      intSetType.methods.methods=cPersistenceCAPI->methods->methods;
      intSetType.methods.link=&m;
      intSetType.tp_getattro=cPersistenceCAPI->getattro;
      intSetType.tp_setattro=cPersistenceCAPI->setattro;
    }
  else return;

  /* Create the module and add the functions */
  m = Py_InitModule4("intSet", module_methods,
		     intSet_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);

  PyExtensionClass_Export(d,"intSet",intSetType);

  PyDict_SetItemString(d, "__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  
	
  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module intSet");
}

/**************************************************************************

  Revision Log:

  $Log: intSet.c,v $
  Revision 1.3  1997/10/01 02:45:58  jim
  Minor reformat.

  Revision 1.2  1997/09/08 18:41:59  jim
  Added logic to save data in binary form.

  Revision 1.1  1997/08/05 14:55:22  jim
  *** empty log message ***



**************************************************************************/
