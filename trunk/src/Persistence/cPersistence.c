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
static char cPersistence_doc_string[] = 
"Defines Persistent mixin class for persistent objects.\n"
"\n"
"$Id: cPersistence.c,v 1.63 2002/09/30 16:02:32 gvanrossum Exp $\n";

#include "cPersistence.h"

struct ccobject_head_struct {
    CACHE_HEAD
};

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)
#define OBJECT(V) ((PyObject*)(V))

static PyObject *py_keys, *py_setstate, *py___dict__, *py_timeTime;
static PyObject *py__p_changed, *py__p_deactivate;
static PyObject *py___getattr__, *py___setattr__, *py___delattr__;

static PyObject *TimeStamp;

#ifdef DEBUG_LOG
static PyObject *debug_log=0;
static int idebug_log=0;

static void *
call_debug(char *event, cPersistentObject *self)
{
  PyObject *r;

  /*
  printf("%s %p\n",event,self->ob_type->tp_name);
  */
  r=PyObject_CallFunction(debug_log,"s(sOi)",event,
			  self->ob_type->tp_name, self->oid,
			  self->state);
  Py_XDECREF(r);
}
#endif

static int
init_strings(void)
{
#define INIT_STRING(S) if (! (py_ ## S = PyString_FromString(#S))) return -1;
  INIT_STRING(keys);
  INIT_STRING(setstate);
  INIT_STRING(timeTime);
  INIT_STRING(__dict__);
  INIT_STRING(_p_changed);
  INIT_STRING(_p_deactivate);
  INIT_STRING(__getattr__);
  INIT_STRING(__setattr__);
  INIT_STRING(__delattr__);
#undef INIT_STRING
  return 0;
}

static int
checknoargs(PyObject *args)
{
  if (!PyTuple_Check(args))
    return 0;
  if (PyTuple_GET_SIZE(args) != 0) {
    PyErr_Format(PyExc_TypeError, 
		 "function takes exactly 0 arguments (%d given)",
		 PyTuple_GET_SIZE(args));
    return 0;
  }
  return 1;
}

static PyObject *
callmethod(PyObject *self, PyObject *name)
{
  self=PyObject_GetAttr(self,name);
  if (self)
    ASSIGN(self,PyObject_CallObject(self,NULL));
  return self;
}

static PyObject *
callmethod1(PyObject *self, PyObject *name, PyObject *arg)
{
  self = PyObject_GetAttr(self, name);
  UNLESS(self) return NULL;
  name = PyTuple_New(1);
  UNLESS(name) 
    {
      Py_DECREF(self);
      return NULL;
    }
  PyTuple_SET_ITEM(name, 0, arg);
  ASSIGN(self, PyObject_CallObject(self, name));
  PyTuple_SET_ITEM(name, 0, NULL);
  Py_DECREF(name);
  return self;
}


static void ghostify(cPersistentObject*);

/* Load the state of the object, unghostifying it.  Upon success, return 1.
 * If an error occurred, re-ghostify the object and return 0.
 */
static int
unghostify(cPersistentObject *self)
{
    if (self->state < 0 && self->jar) {
        PyObject *r;

        /* XXX Is it ever possibly to not have a cache? */
        if (self->cache) {
	    CPersistentRing *home = &self->cache->ring_home;
            /* Create a node in the ring for this unghostified object. */
            self->cache->non_ghost_count++;
            self->ring.next = home;
            self->ring.prev = home->prev;
	    home->prev->next = &self->ring;
	    home->prev = &self->ring;
            Py_INCREF(self);
        }
        self->state = cPersistent_CHANGED_STATE;
        /* Call the object's __setstate__() */
        r = callmethod1(self->jar, py_setstate, (PyObject*)self);
        if (r == NULL) {
            ghostify(self);
            return 0;
        }
        self->state = cPersistent_UPTODATE_STATE;
        Py_DECREF(r);
    }
    return 1;
}

/****************************************************************************/

staticforward PyExtensionClass Pertype;

static void
accessed(cPersistentObject *self)
{
    /* Do nothing unless the object is in a cache and not a ghost. */
    if (self->cache && self->state >= 0) {
	CPersistentRing *home = &self->cache->ring_home;
	self->ring.prev->next = self->ring.next;
	self->ring.next->prev = self->ring.prev;
	self->ring.next = home;
	self->ring.prev = home->prev;
	home->prev->next = &self->ring;
	home->prev = &self->ring; 
    }
}

static void
ghostify(cPersistentObject *self)
{
    /* are we already a ghost? */
    if (self->state == cPersistent_GHOST_STATE)
        return;
    /* XXX is it ever possible to not have a cache? */
    if (self->cache == NULL) {
        self->state = cPersistent_GHOST_STATE;
        return;
    }
    /* if we're ghostifying an object, we better have some non-ghosts */
    assert(self->cache->non_ghost_count > 0);

    self->cache->non_ghost_count--;
    self->ring.next->prev = self->ring.prev;
    self->ring.prev->next = self->ring.next;
    self->ring.prev = NULL;
    self->ring.next = NULL;
    self->state = cPersistent_GHOST_STATE;

    /* We remove the reference to the just ghosted object that the ring
     * holds.  Note that the dictionary of oids->objects has an uncounted
     * reference, so if the ring's reference was the only one, this frees
     * the ghost object.  Note further that the object's dealloc knows to
     * inform the dictionary that it is going away.
     */
    Py_DECREF(self);
}

static void
deallocated(cPersistentObject *self)
{
    if (self->state >= 0) 
	ghostify(self);
    if (self->cache) {
	/* XXX This function shouldn't be able to fail? If not, maybe
	   it shouldn't set an exception either.
	*/
	if (cPersistenceCAPI->percachedel(self->cache, self->oid) < 0)
	    PyErr_Clear(); /* I don't think this should ever happen */
    }
    Py_XDECREF(self->jar);
    Py_XDECREF(self->oid);
}

static int
changed(cPersistentObject *self)
{
  if ((self->state == cPersistent_UPTODATE_STATE ||
       self->state == cPersistent_STICKY_STATE)
       && self->jar)
    {
	PyObject *meth, *arg, *result;
	static PyObject *s_register;

	if (s_register == NULL) 
	    s_register = PyString_InternFromString("register");
	meth = PyObject_GetAttr((PyObject *)self->jar, s_register);
	if (meth == NULL)
	    return -1;
	arg = PyTuple_New(1);
	if (arg == NULL) {
	    Py_DECREF(meth);
	    return -1;
	}
	PyTuple_SET_ITEM(arg, 0, (PyObject *)self);
	result = PyEval_CallObject(meth, arg);
	PyTuple_SET_ITEM(arg, 0, NULL);
	Py_DECREF(arg);
	Py_DECREF(meth);
	if (result == NULL)
	    return -1;
	Py_DECREF(result);

	self->state = cPersistent_CHANGED_STATE;
    }

  return 0;
}

static PyObject *
Per___changed__(cPersistentObject *self, PyObject *args)
{
    PyObject *v = NULL;

    if (args && !PyArg_ParseTuple(args, "|O:__changed__", &v)) 
	return NULL;
    if (!v) 
	return PyObject_GetAttr(OBJECT(self), py__p_changed);

    if (PyObject_IsTrue(v)) {
	if (changed(self) < 0) 
	    return NULL;
    }
    else if (self->state >= 0) 
	self->state = cPersistent_UPTODATE_STATE;

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
Per__p_deactivate(cPersistentObject *self, PyObject *args)
{
  PyObject *dict,*dict2=NULL;

#ifdef DEBUG_LOG
  if (idebug_log < 0) call_debug("reinit",self);
#endif

  if (args && !checknoargs(args))
    return NULL;

  if (self->state==cPersistent_UPTODATE_STATE && self->jar &&
      HasInstDict(self) && (dict=INSTANCE_DICT(self)))
    {
      dict2 = PyDict_Copy(dict);
      PyDict_Clear(dict);
      /* Note that we need to set to ghost state unless we are 
	 called directly. Methods that override this need to
         do the same! */
      ghostify(self);
    }

  /* need to delay releasing the last reference on instance attributes
     until after we have finished accounting for losing our state */
  if (dict2)
  {
      PyDict_Clear(dict2);
      Py_DECREF(dict2);
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/* Load the object's state if necessary and become sticky */
static int
Per_setstate(cPersistentObject *self)
{
    if (!unghostify(self))
        return -1;
    self->state = cPersistent_STICKY_STATE;
    return 0;
}

static PyObject *
Per__getstate__(cPersistentObject *self, PyObject *args)
{
    PyObject *__dict__, *d=0;

    if (!checknoargs(args))
        return NULL;

#ifdef DEBUG_LOG
    if (idebug_log < 0)
        call_debug("get",self);
#endif

    if (!unghostify(self))
        return NULL;

    if (HasInstDict(self) && (__dict__=INSTANCE_DICT(self))) {
        PyObject *k, *v;
        int pos;
        char *ck;
	  
        for(pos=0; PyDict_Next(__dict__, &pos, &k, &v); ) {
            if (PyString_Check(k) && (ck=PyString_AS_STRING(k)) &&
               (*ck=='_' && ck[1]=='v' && ck[2]=='_'))
	    {
                if ((d=PyDict_New()) == NULL)
                    goto err;
                
                for (pos=0; PyDict_Next(__dict__, &pos, &k, &v); )
                    UNLESS(PyString_Check(k) && (ck=PyString_AS_STRING(k)) &&
                           (*ck=='_' && ck[1]=='v' && ck[2]=='_'))
                    {
                        if (PyDict_SetItem(d,k,v) < 0)
                            goto err;
                    }
                return d;
	    }
	}
    }
    else
        __dict__ = Py_None;

    Py_INCREF(__dict__);
    return __dict__;
  err:
    Py_XDECREF(d);
    return NULL;
}  

static PyObject *
Per__setstate__(cPersistentObject *self, PyObject *args)
{
  PyObject *__dict__, *v, *keys=0, *key=0, *e=0;
  int l, i;

  if (HasInstDict(self))
    {

       UNLESS(PyArg_ParseTuple(args, "O", &v)) return NULL;
#ifdef DEBUG_LOG
       if (idebug_log < 0) call_debug("set",self);
#endif
       if (v!=Py_None)
	 {
	   __dict__=INSTANCE_DICT(self);
	   
	   if (PyDict_Check(v))
	     {
	       for(i=0; PyDict_Next(v, &i, &key, &e);)
		 if (PyDict_SetItem(__dict__, key, e) < 0)
		   return NULL;
	     }
	   else
	     {
	       UNLESS(keys=callmethod(v,py_keys)) goto err;
	       UNLESS(-1 != (l=PyObject_Length(keys))) goto err;
	       
	       for(i=0; i < l; i++)
		 {
		   UNLESS_ASSIGN(key,PySequence_GetItem(keys,i)) goto err;
		   UNLESS_ASSIGN(e,PyObject_GetItem(v,key)) goto err;
		   UNLESS(-1 != PyDict_SetItem(__dict__,key,e)) goto err;
		 }
	       
	       Py_XDECREF(key);
	       Py_XDECREF(e);
	       Py_DECREF(keys);
	     }
	 }
    }
  Py_INCREF(Py_None);
  return Py_None;
err:
  Py_XDECREF(key);
  Py_XDECREF(e);
  Py_XDECREF(keys);
  return NULL;
}  


static struct PyMethodDef Per_methods[] = {
  {"__changed__",	(PyCFunction)Per___changed__,	METH_VARARGS,
   "DEPRECATED: use self._p_changed=1"},
  {"_p_deactivate",	(PyCFunction)Per__p_deactivate,	METH_VARARGS,
   "_p_deactivate(oid) -- Deactivate the object"},
  {"__getstate__",	(PyCFunction)Per__getstate__,	METH_VARARGS,
   "__getstate__() -- Return the state of the object" },
  {"__setstate__",	(PyCFunction)Per__setstate__,	METH_VARARGS,
   "__setstate__(v) -- Restore the saved state of the object from v" },
  
  {NULL,		NULL}		/* sentinel */
};

/* ---------- */

static void
Per_dealloc(cPersistentObject *self)
{
#ifdef DEBUG_LOG
  if (idebug_log < 0) call_debug("del",self);
#endif
  deallocated(self);
  Py_XDECREF(self->cache);
  Py_DECREF(self->ob_type);
  PyObject_DEL(self);
}

static PyObject *
orNothing(PyObject *v)
{
  if (! v) v=Py_None;
  Py_INCREF(v);
  return v;
}

static PyObject *
Per_getattr(cPersistentObject *self, PyObject *oname, char *name,
	 PyObject *(*getattrf)(PyObject *, PyObject*))
{
  char *n=name;

  if (n && *n++=='_')
    if (*n++=='p' && *n++=='_')
      {
	switch(*n++)
	  {
	  case 'o':
	    if (*n++=='i' && *n++=='d' && ! *n) return orNothing(self->oid);
	    break;
	  case 'j':
	    if (*n++=='a' && *n++=='r' && ! *n) return orNothing(self->jar);
	    break;
	  case 'c':
	    if (strcmp(n,"hanged")==0)
	      {
		if (self->state < 0)
		  {
		    Py_INCREF(Py_None);
		    return Py_None;
		  }
		return PyInt_FromLong(self->state ==
				      cPersistent_CHANGED_STATE);
	      }
	    break;
	  case 's':
	    if (strcmp(n,"erial")==0)
	      return PyString_FromStringAndSize(self->serial, 8);
	    if (strcmp(n,"elf")==0) 
	      return orNothing(OBJECT(self));
	    break;
	  case 'm':
	    if (strcmp(n,"time")==0)
	      {
                  if (!unghostify(self))
                      return NULL;

		accessed(self);

		if (self->serial[7]=='\0' && self->serial[6]=='\0' &&
		    self->serial[5]=='\0' && self->serial[4]=='\0' &&
		    self->serial[3]=='\0' && self->serial[2]=='\0' &&
		    self->serial[1]=='\0' && self->serial[0]=='\0')
		  {
		    Py_INCREF(Py_None);
		    return Py_None;
		  }
		
		oname=PyString_FromStringAndSize(self->serial, 8);
		if (! oname) return oname;

		ASSIGN(oname, PyObject_CallFunction(TimeStamp, "O", oname));
		if (! oname) return oname;
		ASSIGN(oname, PyObject_GetAttr(oname, py_timeTime));
		if (! oname) return oname;
		ASSIGN(oname, PyObject_CallObject(oname, NULL));
		return oname;
	      }
	    break;
	  }

	return getattrf((PyObject *)self, oname);
      }
  if (! (name && *name++=='_' && *name++=='_' &&
	(strcmp(name,"dict__")==0 || strcmp(name,"class__")==0
	 || strcmp(name, "of__")==0)))
    {
        if (!unghostify(self))
            return NULL;

      accessed(self);
    }

  return getattrf((PyObject *)self, oname);
}

static PyObject*
Per_getattro(cPersistentObject *self, PyObject *name)
{
  char *s=NULL;
  PyObject *r;

  if (PyString_Check(name))
    UNLESS(s=PyString_AS_STRING(name)) return NULL;

  r = Per_getattr(self, name, s, PyExtensionClassCAPI->getattro);
  if (! r && self->state != cPersistent_GHOST_STATE &&
      (((PyExtensionClass*)(self->ob_type))->class_flags 
       & EXTENSIONCLASS_USERGETATTR_FLAG)
      )
    {
      PyErr_Clear();

      r=PyObject_GetAttr(OBJECT(self), py___getattr__);
      if (r) 
	{
	  ASSIGN(r, PyObject_CallFunction(r, "O", name));
	}
      else PyErr_SetObject(PyExc_AttributeError, name);
    }
 
  return r;  
}

static int
_setattro(cPersistentObject *self, PyObject *oname, PyObject *v,
	     int (*setattrf)(PyObject *, PyObject*, PyObject*))
{
  char *name = "";

  if (oname == NULL)
      return -1;
  if (!PyString_Check(oname)) 
      return -1;
  name = PyString_AS_STRING(oname);
  if (name == NULL)
      return -1;
	
  if (*name == '_' && name[1] == 'p' && name[2] == '_') {
      if (strcmp(name + 3, "oid") == 0) {
	  if (self->cache) {
	      int result;
	      if (v == NULL) {
		  PyErr_SetString(PyExc_ValueError,
				  "can not delete oid of cached object");
		  return -1;
	      }
	      if (PyObject_Cmp(self->oid, v, &result) < 0)
		  return -1;
	      if (result) {
		  PyErr_SetString(PyExc_ValueError,
				  "can not change oid of cached object");
		  return -1;
	      }
	  }
	  Py_XINCREF(v);
	  ASSIGN(self->oid, v);
	  return 0;
      }
      else if (strcmp(name + 3, "jar") == 0) {
	  if (self->cache && self->jar) {
	      int result;
	      if (v == NULL) {
		  PyErr_SetString(PyExc_ValueError,
				  "can not delete jar of cached object");
		  return -1;
	      }
	      if (PyObject_Cmp(self->jar, v, &result) < 0)
		  return -1;
	      if (result) {
		  PyErr_SetString(PyExc_ValueError,
				  "can not change jar of cached object");
		  return -1;
	      }
	  }
	  Py_XINCREF(v);
	  ASSIGN(self->jar, v);
	  return 0;
      }
      else if (strcmp(name + 3, "serial") == 0) {
	  if (v) {
	      if (PyString_Check(v) && PyString_GET_SIZE(v) == 8)
		  memcpy(self->serial, PyString_AS_STRING(v), 8);
	      else {
		  PyErr_SetString(PyExc_ValueError,
				  "_p_serial must be an 8-character string");
		  return -1;
	      }
	  } else 
	      memset(self->serial, 0, 8);
	  return 0;
      }
      else if (strcmp(name+3, "changed") == 0) {
	  int deactivate = 0;
	  if (!v)
	    {
	      /* delatter is used to invalidate the object
	         *even* if it has changed.
	       */
	      if (self->state != cPersistent_GHOST_STATE)
		self->state = cPersistent_UPTODATE_STATE;
	      deactivate = 1;
	    }
	  else if (v == Py_None)
	      deactivate = 1;
	  if (deactivate)
	    {
	      PyObject *res;
	      PyObject *meth = PyObject_GetAttr(OBJECT(self), 
						py__p_deactivate);
	      if (meth == NULL)
		  return -1;
	      res = PyObject_CallObject(meth, NULL);
	      if (res) {
		  Py_DECREF(res);
	      } 
	      else {
		  /* an error occured in _p_deactivate().  

		  It's not clear what we should do here.  The code is
		  obviously ignoring the exception, but it shouldn't
		  return 0 for a getattr and set an exception.  The
		  simplest change is to clear the exception, but that
		  simply masks the error. 

		  XXX We'll print an error to stderr just like
		  exceptions in __del__().  It would probably be
		  better to log it but that would be painful from C.
		  */
		  PyErr_WriteUnraisable(meth);
	      }
	      Py_DECREF(meth);
	      return 0;
	    }
	  if (PyObject_IsTrue(v)) return changed(self);
	  if (self->state >= 0) self->state=cPersistent_UPTODATE_STATE;
	  return 0;
	}
    }
  else
    {
        if (!unghostify(self))
            return -1;
      
      accessed(self);

      if ((! (*name=='_' && name[1]=='v' && name[2]=='_'))
	 && (self->state != cPersistent_CHANGED_STATE && self->jar)
	 && setattrf
	 )
	if (changed(self) < 0) return -1;
    }

  if (setattrf)
    return setattrf((PyObject*)self,oname,v);
  
  return 1;			/* Ready for user setattr */
}

static int
Per_setattro(cPersistentObject *self, PyObject *oname, PyObject *v)
{
  int r;
  PyObject *m;

  if (v && (((PyExtensionClass*)self->ob_type)->class_flags 
	    & EXTENSIONCLASS_USERSETATTR_FLAG))
    {
      r = _setattro(self, oname, v, NULL);
      if (r < 1) return r;

      m=PyObject_GetAttr(OBJECT(self), py___setattr__);
      if (m) 
	{
	  ASSIGN(m, PyObject_CallFunction(m, "OO", oname, v));
	}
      else PyErr_SetObject(PyExc_AttributeError, oname);
    }
  else if (!v && (((PyExtensionClass*)self->ob_type)->class_flags 
		  & EXTENSIONCLASS_USERDELATTR_FLAG)
	   )
    {
      r=_setattro(self,oname, v, NULL);
      if (r < 1) return r;

      m=PyObject_GetAttr(OBJECT(self), py___delattr__);
      if (m) 
      {
	ASSIGN(m, PyObject_CallFunction(m, "O", oname));
      }
      else PyErr_SetObject(PyExc_AttributeError, oname);
    }
  else
    return _setattro(self,oname, v, PyExtensionClassCAPI->setattro);

  if (m) 
    {
      Py_DECREF(m);
      return 0;
    }
  
  return -1;
}

static PyExtensionClass Pertype = {
	PyObject_HEAD_INIT(NULL)
	0,				/*ob_size*/
	"Persistent",			/*tp_name*/
	sizeof(cPersistentObject),	/*tp_basicsize*/
	0,				/*tp_itemsize*/
	/* methods */
	(destructor)Per_dealloc,	/*tp_dealloc*/
	(printfunc)0,			/*tp_print*/
	(getattrfunc)0,			/*tp_getattr*/
	(setattrfunc)0,	       		/*tp_setattr*/
	(cmpfunc)0,			/*tp_compare*/
	(reprfunc)0,			/*tp_repr*/
	0,				/*tp_as_number*/
	0,				/*tp_as_sequence*/
	0,				/*tp_as_mapping*/
	(hashfunc)0,			/*tp_hash*/
	(ternaryfunc)0,			/*tp_call*/
	(reprfunc)0,			/*tp_str*/
	(getattrofunc)Per_getattro,	/*tp_getattr with object key*/
	(setattrofunc)Per_setattro,	/*tp_setattr with object key*/
	/* Space for future expansion */
	0L,0L,
        "Base class for objects that are stored in their own database records",
	METHOD_CHAIN(Per_methods),
	PERSISTENCE_FLAGS,
};

static PyExtensionClass Overridable = {
	PyObject_HEAD_INIT(NULL)
	0,				/*ob_size*/
	"Overridable",			/*tp_name*/
	sizeof(cPersistentObject),	/*tp_basicsize*/
	0,				/*tp_itemsize*/
	/* methods */
	(destructor)Per_dealloc,	/*tp_dealloc*/
	(printfunc)0,			/*tp_print*/
	(getattrfunc)0,			/*tp_getattr*/
	(setattrfunc)0,	       		/*tp_setattr*/
	(cmpfunc)0,			/*tp_compare*/
	(reprfunc)0,			/*tp_repr*/
	0,				/*tp_as_number*/
	0,				/*tp_as_sequence*/
	0,				/*tp_as_mapping*/
	(hashfunc)0,			/*tp_hash*/
	(ternaryfunc)0,			/*tp_call*/
	(reprfunc)0,			/*tp_str*/
	(getattrofunc)Per_getattro,	/*tp_getattr with object key*/
	(setattrofunc)Per_setattro,	/*tp_setattr with object key*/
	/* Space for future expansion */
	0L,0L,
        "Hacked base class used in Zope's App.Uninstalled.BrokenClass\n\n"
        "Not sure if this is still needed",
	METHOD_CHAIN(Per_methods),
	EXTENSIONCLASS_BASICNEW_FLAG | PERSISTENT_TYPE_FLAG
};

/* End of code for Persistent objects */
/* -------------------------------------------------------- */

/* List of methods defined in the module */

#ifdef DEBUG_LOG
static PyObject *
set_debug_log(PyObject *ignored, PyObject *args)
{
  Py_INCREF(args);
  ASSIGN(debug_log, args);
  if (debug_log) idebug_log=-1;
  else idebug_log=0;
  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static struct PyMethodDef cP_methods[] = {
#ifdef DEBUG_LOG
  {"set_debug_log", (PyCFunction)set_debug_log, METH_VARARGS,
   "set_debug_log(callable) -- Provide a function to log events\n"
   "\n"
   "The function will be called with an event name and a persistent object.\n"
  },
#endif
  {NULL,		NULL}		/* sentinel */
};


/* Initialization function for the module (*must* be called initcPersistence) */

typedef int (*intfunctionwithpythonarg)(PyObject*);

static cPersistenceCAPIstruct
truecPersistenceCAPI = {
  &(Pertype.methods),
  (getattrofunc)Per_getattro,	/*tp_getattr with object key*/
  (setattrofunc)Per_setattro,	/*tp_setattr with object key*/
  changed,
  accessed,
  ghostify,
  deallocated,
  (intfunctionwithpythonarg)Per_setstate,
  (pergetattr)Per_getattr,
  NULL
};

void
initcPersistence(void)
{
  PyObject *m, *d, *s;

  s = PyString_FromString("ZODB.TimeStamp");
  if (s == NULL)
      return;
  m = PyImport_Import(s);
  if (m == NULL) {
      Py_DECREF(s);
      return;
  }
  TimeStamp = PyObject_GetAttr(m, s);
  Py_DECREF(m);
  Py_DECREF(s);

  if (init_strings() < 0) 
      return;

  m = Py_InitModule4("cPersistence", cP_methods, cPersistence_doc_string,
		     (PyObject*)NULL, PYTHON_API_VERSION);

  
  d = PyModule_GetDict(m);

  PyExtensionClass_Export(d, "Persistent",  Pertype);
  PyExtensionClass_Export(d, "Overridable", Overridable);

  cPersistenceCAPI = &truecPersistenceCAPI;
  s = PyCObject_FromVoidPtr(cPersistenceCAPI, NULL);
  PyDict_SetItemString(d, "CAPI", s);
  Py_XDECREF(s);
}
