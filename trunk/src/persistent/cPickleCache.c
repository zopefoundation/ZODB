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

/*

Objects are stored under three different regimes:

Regime 1: Persistent Classes

Persistent Classes are part of ZClasses. They are stored in the
self->data dictionary, and are never garbage collected.

The klass_items() method returns a sequence of (oid,object) tuples for
every Persistent Class, which should make it possible to implement
garbage collection in Python if necessary.

Regime 2: Ghost Objects

There is no benefit to keeping a ghost object which has no external
references, therefore a weak reference scheme is used to ensure that
ghost objects are removed from memory as soon as possible, when the
last external reference is lost.

Ghost objects are stored in the self->data dictionary. Normally a
dictionary keeps a strong reference on its values, however this
reference count is 'stolen'.

This weak reference scheme leaves a dangling reference, in the
dictionary, when the last external reference is lost. To clean up this
dangling reference the persistent object dealloc function calls
self->cache->_oid_unreferenced(self->oid). The cache looks up the oid
in the dictionary, ensures it points to an object whose reference
count is zero, then removes it from the dictionary. Before removing
the object from the dictionary it must temporarily resurrect the
object in much the same way that class instances are resurrected
before their __del__ is called.

Since ghost objects are stored under a different regime to non-ghost
objects, an extra ghostify function in cPersistenceAPI replaces
self->state=GHOST_STATE assignments that were common in other
persistent classes (such as BTrees).

Regime 3: Non-Ghost Objects

Non-ghost objects are stored in two data structures: the dictionary
mapping oids to objects and a doubly-linked list that encodes the
order in which the objects were accessed.  The dictionary reference is
borrowed, as it is for ghosts.  The list reference is a new reference;
the list stores recently used objects, even if they are otherwise
unreferenced, to avoid loading the object from the database again.

The doubly-link-list nodes contain next and previous pointers linking
together the cache and all non-ghost persistent objects.

The node embedded in the cache is the home position. On every
attribute access a non-ghost object will relink itself just behind the
home position in the ring. Objects accessed least recently will
eventually find themselves positioned after the home position.

Occasionally other nodes are temporarily inserted in the ring as
position markers. The cache contains a ring_lock flag which must be
set and unset before and after doing so. Only if the flag is unset can
the cache assume that all nodes are either his own home node, or nodes
from persistent objects. This assumption is useful during the garbage
collection process.

The number of non-ghost objects is counted in self->non_ghost_count.
The garbage collection process consists of traversing the ring, and
deactivating (that is, turning into a ghost) every object until
self->non_ghost_count is down to the target size, or until it
reaches the home position again.

Note that objects in the sticky or changed states are still kept in
the ring, however they can not be deactivated. The garbage collection
process must skip such objects, rather than deactivating them.

*/

static char cPickleCache_doc_string[] =
"Defines the PickleCache used by ZODB Connection objects.\n"
"\n"
"$Id: cPickleCache.c,v 1.80 2003/04/02 16:50:49 jeremy Exp $\n";

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define OBJECT(O) ((PyObject*)O)

#define DONT_USE_CPERSISTENCECAPI
#include "cPersistence.h"
#include <time.h>
#include <stddef.h>
#undef Py_FindMethod

static PyObject *py__p_oid, *py_reload, *py__p_jar, *py__p_changed;

/* Do we want 'engine noise'.... abstract debugging output useful for
   visualizing cache behavior */
#if 0
#define ENGINE_NOISE(A) printf(A)
#else
#define ENGINE_NOISE(A) ((void)A)
#endif

/* This object is the pickle cache.  The CACHE_HEAD macro guarantees
   that layout of this struct is the same as the start of
   ccobject_head in cPersistence.c */
typedef struct {
    CACHE_HEAD
    int klass_count;                         /* count of persistent classes */
    PyObject *data;                          /* oid -> object dict */
    PyObject *jar;                           /* Connection object */
    PyObject *setklassstate;                 /* ??? */
    int cache_size;                          /* target number of items in cache */

    /* Most of the time the ring contains only:
       * many nodes corresponding to persistent objects
       * one 'home' node from the cache.
    In some cases it is handy to temporarily add other types
    of node into the ring as placeholders. 'ring_lock' is a boolean
    indicating that someone has already done this. Currently this
    is only used by the garbage collection code. */

    int ring_lock;

    /* 'cache_drain_resistance' controls how quickly the cache size will drop
    when it is smaller than the configured size. A value of zero means it will
    not drop below the configured size (suitable for most caches). Otherwise,
    it will remove cache_non_ghost_count/cache_drain_resistance items from
    the cache every time (suitable for rarely used caches, such as those
    associated with Zope versions. */

    int cache_drain_resistance;

} ccobject;

static int cc_ass_sub(ccobject *self, PyObject *key, PyObject *v);

/* ---------------------------------------------------------------- */

#define OBJECT_FROM_RING(SELF, HERE, CTX) \
    ((cPersistentObject *)(((char *)here) - offsetof(cPersistentObject, ring)))

static int
scan_gc_items(ccobject *self,int target)
{
    /* This function must only be called with the ring lock held,
       because it places a non-object placeholder in the ring.
    */

    cPersistentObject *object;
    int error;
    CPersistentRing placeholder;
    CPersistentRing *here = self->ring_home.next;

    /* Scan through the ring until we either find the ring_home (i.e. start
     * of the ring, or we've ghosted enough objects to reach the target
     * size.
     */
    while (1) {
	/* back to the home position. stop looking */
        if (here == &self->ring_home)
            return 0;

        /* At this point we know that the ring only contains nodes
	   from persistent objects, plus our own home node. We know
	   this because the ring lock is held.  We can safely assume
	   the current ring node is a persistent object now we know it
	   is not the home */
        object = OBJECT_FROM_RING(self, here, "scan_gc_items");
        if (!object) 
	    return -1;

	/* we are small enough */
        if (self->non_ghost_count <= target)
            return 0;
        else if (object->state == cPersistent_UPTODATE_STATE) {
            /* deactivate it. This is the main memory saver. */

            /* Add a placeholder; a dummy node in the ring.  We need
	       to do this to mark our position in the ring.  It is
	       possible that the PyObject_SetAttr() call below will
	       invoke an __setattr__() hook in Python.  If it does,
	       another thread might run; if that thread accesses a
	       persistent object and moves it to the head of the ring,
	       it might cause the gc scan to start working from the
	       head of the list.
	    */

            placeholder.next = here->next;
            placeholder.prev = here;
            here->next->prev = &placeholder;
            here->next = &placeholder;

            ENGINE_NOISE("G");

            /* In Python, "obj._p_changed = None" spells, ghostify */
            error = PyObject_SetAttr((PyObject *)object, py__p_changed, 
				     Py_None);


            /* unlink the placeholder */
            placeholder.next->prev = placeholder.prev;
            placeholder.prev->next = placeholder.next;

            here = placeholder.next;

            if (error)
                return -1; /* problem */
        }
        else {
            ENGINE_NOISE(".");
            here = here->next;
        }
    }
}

static PyObject *
lockgc(ccobject *self, int target_size)
{
    /* This is thread-safe because of the GIL, and there's nothing
     * in between checking the ring_lock and acquiring it that calls back
     * into Python.
     */
    if (self->ring_lock) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    ENGINE_NOISE("<");
    self->ring_lock = 1;
    if (scan_gc_items(self, target_size)) {
        self->ring_lock = 0;
        return NULL;
    }
    self->ring_lock = 0;
    ENGINE_NOISE(">\n");

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
cc_incrgc(ccobject *self, PyObject *args)
{
    int n = 1;
    int starting_size = self->non_ghost_count;
    int target_size = self->cache_size;

    if (self->cache_drain_resistance >= 1) {
        /* This cache will gradually drain down to a small size. Check
           a (small) number of objects proportional to the current size */

        int target_size_2 = (starting_size - 1 
			     - starting_size / self->cache_drain_resistance);
        if (target_size_2 < target_size)
            target_size = target_size_2;
    }

    if (!PyArg_ParseTuple(args, "|i:incrgc", &n)) 
	return NULL;

    return lockgc(self, target_size);
}

static PyObject *
cc_full_sweep(ccobject *self, PyObject *args)
{
    int dt = 0;
    if (!PyArg_ParseTuple(args, "|i:full_sweep", &dt)) 
	return NULL;
    if (dt == 0)
	return lockgc(self, 0);
    else
	return cc_incrgc(self, args);
}

static PyObject *
cc_minimize(ccobject *self, PyObject *args)
{
    int ignored;
    if (!PyArg_ParseTuple(args, "|i:minimize", &ignored)) 
	return NULL;
    return lockgc(self, 0);
}

static void
_invalidate(ccobject *self, PyObject *key)
{
    PyObject *v = PyDict_GetItem(self->data, key);

    if (!v)
	return;
    if (PyExtensionClass_Check(v)) {
	if (v->ob_refcnt <= 1) {
	    self->klass_count--;
	    if (PyDict_DelItem(self->data, key) < 0)
		PyErr_Clear();
	}
	else {
	    v = PyObject_CallFunction(self->setklassstate, "O", v);
	    if (v) 
		Py_DECREF(v);
	    else 
		PyErr_Clear();
	}
    } else {
	if (PyObject_DelAttr(v, py__p_changed) < 0)
	    PyErr_Clear();
    }
}

static PyObject *
cc_invalidate(ccobject *self, PyObject *args)
{
  PyObject *inv, *key, *v;
  int i = 0;

  if (PyArg_ParseTuple(args, "O!", &PyDict_Type, &inv)) {
      while (PyDict_Next(inv, &i, &key, &v))
	  _invalidate(self, key);
      PyDict_Clear(inv);
  }
  else {
      PyErr_Clear();
      if (!PyArg_ParseTuple(args, "O:invalidate", &inv)) 
	  return NULL;
      if (PyString_Check(inv))
	  _invalidate(self, inv);
      else {
	  int l;
	  
	  PyErr_Clear();
	  l = PyObject_Length(inv);
	  if (l < 0)
	      return NULL;
	  for (i=l; --i >= 0; ) {
	      key = PySequence_GetItem(inv, i);
	      if (!key)
		  return NULL;
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
    PyObject *r, *key, *d = NULL;

    if (!PyArg_ParseTuple(args, "O|O:get", &key, &d)) 
	return NULL;

    r = PyDict_GetItem(self->data, key);
    if (!r) {
	if (d) {
	    r = d;
	} else {
	    PyErr_SetObject(PyExc_KeyError, key);
	    return NULL;
	}
    }
    Py_INCREF(r);
    return r;
}

static PyObject *
cc_klass_items(ccobject *self, PyObject *args)
{
    PyObject *l,*k,*v;
    int p = 0;

    if (!PyArg_ParseTuple(args, ":klass_items")) 
	return NULL;

    l = PyList_New(PyDict_Size(self->data));
    if (l == NULL) 
	return NULL;

    while (PyDict_Next(self->data, &p, &k, &v)) {
        if(PyExtensionClass_Check(v)) {
	    v = Py_BuildValue("OO", k, v);
	    if (v == NULL) {
		Py_DECREF(l);
		return NULL;
	    }
	    if (PyList_Append(l, v) < 0) {
		Py_DECREF(v);
		Py_DECREF(l);
		return NULL;
	    }
	    Py_DECREF(v);
        }
    }

    return l;
}

static PyObject *
cc_lru_items(ccobject *self, PyObject *args)
{
    PyObject *l;
    CPersistentRing *here;

    if (!PyArg_ParseTuple(args, ":lru_items")) 
	return NULL;

    if (self->ring_lock) {
	/* When the ring lock is held, we have no way of know which
	   ring nodes belong to persistent objects, and which a
	   placeholders. */
        PyErr_SetString(PyExc_ValueError,
		".lru_items() is unavailable during garbage collection");
        return NULL;
    }

    l = PyList_New(0);
    if (l == NULL) 
	return NULL;

    here = self->ring_home.next;
    while (here != &self->ring_home) {
        PyObject *v;
        cPersistentObject *object = OBJECT_FROM_RING(self, here, "cc_items");

        if (object == NULL) {
            Py_DECREF(l);
            return NULL;
        }
	v = Py_BuildValue("OO", object->oid, object);
	if (v == NULL) {
            Py_DECREF(l);
            return NULL;
	}
	if (PyList_Append(l, v) < 0) {
	    Py_DECREF(v);
            Py_DECREF(l);
            return NULL;
	}
        Py_DECREF(v);
        here = here->next;
    }

    return l;
}

static int
cc_oid_unreferenced(ccobject *self, PyObject *oid)
{
    /* This is called by the persistent object deallocation function
       when the reference count on a persistent object reaches
       zero. We need to fix up our dictionary; its reference is now
       dangling because we stole its reference count. Be careful to
       not release the global interpreter lock until this is
       complete. */

    PyObject *v;

    v = PyDict_GetItem(self->data, oid);
    if (v == NULL) {
	PyErr_SetObject(PyExc_KeyError, oid);
	return -1;
    }

    assert(v->ob_refcnt == 0);
    /* Need to be very hairy here because a dictionary is about
       to decref an already deleted object. 
    */

#ifdef Py_TRACE_REFS
    /* This is called from the deallocation function after the
       interpreter has untracked the reference.  Track it again.
     */
    _Py_NewReference(v);
    /* Don't increment total refcount as a result of the 
       shenanigans played in this function.  The _Py_NewReference()
       call above creates artificial references to v.
    */
    _Py_RefTotal--;
    /* XXX it may be a problem that v->ob_type is still NULL? 
       I don't understand what this comment means.  --jeremy */
    assert(v->ob_type);
#else
    Py_INCREF(v);
#endif
    assert(v->ob_refcnt == 1);
    /* Incremement the refcount again, because delitem is going to
       DECREF it.  If it's refcount reached zero again, we'd call back to
       the dealloc function that called us.
    */
    Py_INCREF(v);

    /* XXX Should we call _Py_ForgetReference() on error exit? */
    if (PyDict_DelItem(self->data, oid) < 0) 
	return -1;
    Py_DECREF((ccobject *)((cPersistentObject *)v)->cache);

    if (v->ob_refcnt != 1) {
        PyErr_SetString(PyExc_ValueError,
			"refcount is not 1 after removal from dict");
        return -1;
    }

    /* Undo the temporary resurrection.
       Don't DECREF the object, because this function is called from
       the object's dealloc function. If the refcnt reaches zero, it
       will all be invoked recursively.
     */
    _Py_ForgetReference(v);

    return 0;
}

static PyObject *
cc_ringlen(ccobject *self, PyObject *args)
{
    CPersistentRing *here;
    int c = 0;

    if (!PyArg_ParseTuple(args, ":ringlen"))
	return NULL;
    for (here = self->ring_home.next; here != &self->ring_home;
	 here = here->next)
	c++;
    return PyInt_FromLong(c);
}

static struct PyMethodDef cc_methods[] = {
  {"lru_items", (PyCFunction)cc_lru_items, METH_VARARGS,
   "List (oid, object) pairs from the lru list, as 2-tuples.\n"
   },
  {"klass_items", (PyCFunction)cc_klass_items, METH_VARARGS,
   "List (oid, object) pairs of cached persistent classes.\n"
   },
  {"full_sweep", (PyCFunction)cc_full_sweep, METH_VARARGS,
   "full_sweep([age]) -- Perform a full sweep of the cache\n\n"
   "Supported for backwards compatibility.  If the age argument is 0,\n"
   "behaves like minimize().  Otherwise, behaves like incrgc()."
   },
  {"minimize",	(PyCFunction)cc_minimize, METH_VARARGS,
   "minimize([ignored]) -- Remove as many objects as possible\n\n"
   "Ghostify all objects that are not modified.  Takes an optional\n"
   "argument, but ignores it."
   },
  {"incrgc", (PyCFunction)cc_incrgc, METH_VARARGS,
   "incrgc([n]) -- Perform incremental garbage collection\n\n"
   "Some other implementations support an optional parameter 'n' which\n"
   "indicates a repetition count; this value is ignored.\n"},
  {"invalidate", (PyCFunction)cc_invalidate, METH_VARARGS,
   "invalidate(oids) -- invalidate one, many, or all ids"},
  {"get", (PyCFunction)cc_get, METH_VARARGS,
   "get(key [, default]) -- get an item, or a default"},
  {"ringlen", (PyCFunction)cc_ringlen, METH_VARARGS,
   "ringlen() -- Returns number of non-ghost items in cache."},
  {NULL,		NULL}		/* sentinel */
};

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
	return PyInt_FromLong(0);   /* this cache does not use this value */
      if(strcmp(name,"cache_size")==0)
	return PyInt_FromLong(self->cache_size);
      if(strcmp(name,"cache_drain_resistance")==0)
	return PyInt_FromLong(self->cache_drain_resistance);
      if(strcmp(name,"cache_non_ghost_count")==0)
	return PyInt_FromLong(self->non_ghost_count);
      if(strcmp(name,"cache_klass_count")==0)
	return PyInt_FromLong(self->klass_count);
      if(strcmp(name,"cache_data")==0)
	{
	  /* now a copy of our data; the ring is too fragile */
	  return PyDict_Copy(self->data);
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
	  /* this cache doesnt use the age */
	  return 0;
	}

      if(strcmp(name,"cache_size")==0)
	{
	  UNLESS(PyArg_Parse(value,"i",&v)) return -1;
	  self->cache_size=v;
	  return 0;
	}

      if(strcmp(name,"cache_drain_resistance")==0)
	{
	  UNLESS(PyArg_Parse(value,"i",&v)) return -1;
	  self->cache_drain_resistance=v;
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

    r = PyDict_GetItem(self->data, key);
    if (r == NULL) {
	PyErr_SetObject(PyExc_KeyError, key);
	return NULL;
    }
    Py_INCREF(r);

    return r;
}

static int
cc_add_item(ccobject *self, PyObject *key, PyObject *v)
{
    int result;
    PyObject *oid, *object_again, *jar;
    cPersistentObject *p;

    if (PyExtensionClass_Check(v)) {
        /* Its a persistent class, such as a ZClass. Thats ok. */
    }
    else if( PyExtensionInstance_Check(v) &&
             (((PyExtensionClass*)(v->ob_type))->class_flags & PERSISTENT_TYPE_FLAG) &&
             (v->ob_type->tp_basicsize >= sizeof(cPersistentObject)) ) {
        /* Its and instance of a persistent class, (ie Python classeses that
        derive from Persistence.Persistent, BTrees, etc). Thats ok. */
    }
    else {
	PyErr_SetString(PyExc_TypeError, 
			"Cache values must be persistent objects.");
	return -1;
    }

    /* Can't access v->oid directly because the object might be a
     *  persistent class.
     */
    oid = PyObject_GetAttr(v, py__p_oid);
    if (oid == NULL)
	return -1;
    if (!PyString_Check(oid)) {
        PyErr_Format(PyExc_TypeError,
                     "Cached object oid must be a string, not a %s",
		     oid->ob_type->tp_name);
	return -1;
    }
    /*  we know they are both strings.
     *  now check if they are the same string.
     */
    result = PyObject_Compare(key, oid);
    if (PyErr_Occurred()) {
	Py_DECREF(oid);
	return -1;
    } 
    Py_DECREF(oid);
    if (result) {
	PyErr_SetString(PyExc_ValueError, "Cache key does not match oid");
	return -1;
    }

    /* useful sanity check, but not strictly an invariant of this class */
    jar = PyObject_GetAttr(v, py__p_jar);
    if (jar == NULL)
        return -1;
    if (jar==Py_None) {
        Py_DECREF(jar);
        PyErr_SetString(PyExc_ValueError,
                        "Cached object jar missing");
	return -1;
    }
    Py_DECREF(jar);

    object_again = PyDict_GetItem(self->data, key);
    if (object_again) {
	if (object_again != v) {
	    PyErr_SetString(PyExc_ValueError,
		    "Can not re-register object under a different oid");
	    return -1;
	} else {
	    /* re-register under the same oid - no work needed */
	    return 0;
	}
    }

    if (PyExtensionClass_Check(v)) {
	if (PyDict_SetItem(self->data, key, v) < 0) 
	    return -1;
	self->klass_count++;
	return 0;
    } else {
	PerCache *cache = ((cPersistentObject *)v)->cache;
	if (cache) {
	    if (cache != (PerCache *)self)
		/* This object is already in a different cache. */
		PyErr_SetString(PyExc_ValueError, 
				"Cache values may only be in one cache.");
	    return -1;
	} 
	/* else:
	   
	   This object is already one of ours, which is ok.  It
	   would be very strange if someone was trying to register
	   the same object under a different key. 
	*/
    }
    
    if (PyDict_SetItem(self->data, key, v) < 0) 
	return -1;
    /* the dict should have a borrowed reference */
    Py_DECREF(v);
    
    p = (cPersistentObject *)v;
    Py_INCREF(self);
    p->cache = (PerCache *)self;
    if (p->state >= 0) {
	/* insert this non-ghost object into the ring just 
	   behind the home position. */
	self->non_ghost_count++;
	p->ring.next = &self->ring_home;
	p->ring.prev =  self->ring_home.prev;
	self->ring_home.prev->next = &p->ring;
	self->ring_home.prev = &p->ring;
	/* this list should have a new reference to the object */
	Py_INCREF(v);
    }
    return 0;
}

static int
cc_del_item(ccobject *self, PyObject *key)
{
    PyObject *v;
    cPersistentObject *p;

    /* unlink this item from the ring */
    v = PyDict_GetItem(self->data, key);
    if (v == NULL)
	return -1;

    if (PyExtensionClass_Check(v)) {
	self->klass_count--;
    } else {
	p = (cPersistentObject *)v;
	if (p->state >= 0) {
	    self->non_ghost_count--;
	    p->ring.next->prev = p->ring.prev;
	    p->ring.prev->next = p->ring.next;
	    p->ring.prev = NULL;
	    p->ring.next = NULL;
	    /* The DelItem below will account for the reference
	       held by the list. */
	} else {
	    /* This is a ghost object, so we havent kept a reference
	       count on it.  For it have stayed alive this long
	       someone else must be keeping a reference to
	       it. Therefore we need to temporarily give it back a
	       reference count before calling DelItem below */
	    Py_INCREF(v);
	}

	Py_DECREF((PyObject *)p->cache);
	p->cache = NULL;
    }

    if (PyDict_DelItem(self->data, key) < 0) {
	PyErr_SetString(PyExc_RuntimeError,
			"unexpectedly couldn't remove key in cc_ass_sub");
	return -1;
    }

    return 0;
}

static int
cc_ass_sub(ccobject *self, PyObject *key, PyObject *v)
{
    if (!PyString_Check(key)) {
	PyErr_Format(PyExc_TypeError,
                     "cPickleCache key must be a string, not a %s",
		     key->ob_type->tp_name);
	return -1;
    }
    if (v)
	return cc_add_item(self, key, v);
    else
	return cc_del_item(self, key);
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
    (printfunc)0,		/*tp_print*/
    (getattrfunc)cc_getattr,	/*tp_getattr*/
    (setattrfunc)cc_setattr,	/*tp_setattr*/
    (cmpfunc)0,			/*tp_compare*/
    (reprfunc)0,   		/*tp_repr*/
    0,				/*tp_as_number*/
    0,				/*tp_as_sequence*/
    &cc_as_mapping,		/*tp_as_mapping*/
    (hashfunc)0,		/*tp_hash*/
    (ternaryfunc)0,		/*tp_call*/
    (reprfunc)0,  		/*tp_str*/
};

static ccobject *
newccobject(PyObject *jar, int cache_size)
{
    ccobject *self;
  
    self = PyObject_NEW(ccobject, &Cctype);
    if (self == NULL)
	return NULL;
    self->setklassstate = self->jar = NULL;
    self->data = PyDict_New();
    if (self->data == NULL) {
	Py_DECREF(self);
	return NULL;
    }
    self->setklassstate = PyObject_GetAttrString(jar, "setklassstate");
    if (self->setklassstate == NULL) {
	Py_DECREF(self);
	return NULL;
    }
    self->jar = jar; 
    Py_INCREF(jar);
    self->cache_size = cache_size;
    self->non_ghost_count = 0;
    self->klass_count = 0;
    self->cache_drain_resistance = 0;
    self->ring_lock = 0;
    self->ring_home.next = &self->ring_home;
    self->ring_home.prev = &self->ring_home;
    return self;
}

static PyObject *
cCM_new(PyObject *self, PyObject *args)
{
    int cache_size=100;
    PyObject *jar;

    if (!PyArg_ParseTuple(args, "O|i", &jar, &cache_size))
	return NULL;
    return (PyObject*)newccobject(jar, cache_size);
}

static struct PyMethodDef cCM_methods[] = {
    {"PickleCache", (PyCFunction)cCM_new, METH_VARARGS},
    {NULL, NULL} /* sentinel */
};

void
initcPickleCache(void)
{
    PyObject *m, *d;
    cPersistenceCAPIstruct *capi;

    Cctype.ob_type = &PyType_Type;

    if (!ExtensionClassImported) 
	return;

    capi = (cPersistenceCAPIstruct *)PyCObject_Import("cPersistence", "CAPI");
    if (!capi)
	return;
    capi->percachedel = (percachedelfunc)cc_oid_unreferenced;

    m = Py_InitModule4("cPickleCache", cCM_methods, cPickleCache_doc_string,
		       (PyObject*)NULL, PYTHON_API_VERSION);

    py_reload = PyString_InternFromString("reload");
    py__p_jar = PyString_InternFromString("_p_jar");
    py__p_changed = PyString_InternFromString("_p_changed");
    py__p_oid = PyString_InternFromString("_p_oid");

    d = PyModule_GetDict(m);

    PyDict_SetItemString(d, "cache_variant", PyString_FromString("stiff/c"));
}
