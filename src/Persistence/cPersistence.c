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
static char *what_string = "$Id: cPersistence.c,v 1.40 2001/02/15 16:18:22 jim Exp $";

#include <string.h>
#include "cPersistence.h"

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

static void
init_strings()
{
#define INIT_STRING(S) py_ ## S = PyString_FromString(#S)
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
}

static PyObject *
callmethod(PyObject *self, PyObject *name)
{
  if(self=PyObject_GetAttr(self,name))
    ASSIGN(self,PyObject_CallObject(self,NULL));
  return self;
}

static PyObject *
callmethod1(PyObject *self, PyObject *name, PyObject *arg)
{
  if((self=PyObject_GetAttr(self,name)) && (name=PyTuple_New(1)))
    {
      PyTuple_SET_ITEM(name, 0, arg);
      ASSIGN(self,PyObject_CallObject(self,name));
      PyTuple_SET_ITEM(name, 0, NULL);
      Py_DECREF(name);
    }
  return self;
}

static PyObject *
callmethod2(PyObject *self, PyObject *name, PyObject *arg, PyObject *arg2)
{
  if((self=PyObject_GetAttr(self,name)) && (name=PyTuple_New(2)))
    {
      PyTuple_SET_ITEM(name, 0, arg);
      PyTuple_SET_ITEM(name, 1, arg2);
      ASSIGN(self,PyObject_CallObject(self,name));
      PyTuple_SET_ITEM(name, 0, NULL);
      PyTuple_SET_ITEM(name, 1, NULL);
      Py_DECREF(name);
    }
  return self;
}

static PyObject *
callmethod3(PyObject *self, PyObject *name,
	    PyObject *arg, PyObject *arg2, PyObject *arg3)
{
  if((self=PyObject_GetAttr(self,name)) && (name=PyTuple_New(3)))
    {
      PyTuple_SET_ITEM(name, 0, arg);
      PyTuple_SET_ITEM(name, 1, arg2);
      PyTuple_SET_ITEM(name, 2, arg3);
      ASSIGN(self,PyObject_CallObject(self,name));
      PyTuple_SET_ITEM(name, 0, NULL);
      PyTuple_SET_ITEM(name, 1, NULL);
      PyTuple_SET_ITEM(name, 2, NULL);
      Py_DECREF(name);
    }
  return self;
}

#define UPDATE_STATE_IF_NECESSARY(self, ER)                      \
if(self->state < 0 && self->jar)                                 \
{								 \
  PyObject *r;							 \
      								 \
  self->state=cPersistent_CHANGED_STATE; 	                 \
  UNLESS(r=callmethod1(self->jar,py_setstate,(PyObject*)self))   \
    {                                                            \
      self->state=cPersistent_GHOST_STATE;                       \
      return ER;                                                 \
    }								 \
  self->state=cPersistent_UPTODATE_STATE;			 \
  Py_DECREF(r);							 \
}


static PyObject *
#ifdef HAVE_STDARG_PROTOTYPES
/* VARARGS 2 */
PyString_BuildFormat(char *stringformat, char *format, ...)
#else
/* VARARGS */
PyString_BuildFormat(va_alist) va_dcl
#endif
{
  va_list va;
  PyObject *args=0, *retval=0, *v=0;
#ifdef HAVE_STDARG_PROTOTYPES
  va_start(va, format);
#else
  PyObject *ErrType;
  char *stringformat, *format;
  va_start(va);
  ErrType = va_arg(va, PyObject *);
  stringformat   = va_arg(va, char *);
  format   = va_arg(va, char *);
#endif
  
  args = Py_VaBuildValue(format, va);
  va_end(va);
  if(! args) return NULL;
  if(!(retval=PyString_FromString(stringformat))) return NULL;

  v=PyString_Format(retval, args);
  Py_DECREF(retval);
  Py_DECREF(args);
  return v;
}

/****************************************************************************/

staticforward PyExtensionClass Pertype;
staticforward PyExtensionClass TPertype;

static int
changed(cPersistentObject *self)
{
  static PyObject *builtins=0, *get_transaction=0, *py_register=0;
  PyObject *T;
  
  if ((self->state == cPersistent_UPTODATE_STATE ||
       self->state == cPersistent_STICKY_STATE)
       && self->jar)
    {
      UNLESS (get_transaction)
	{
	  UNLESS (py_register=PyString_FromString("register")) return -1;
	  UNLESS (T=PyImport_ImportModule("__main__")) return -1;
	  ASSIGN(T,PyObject_GetAttrString(T,"__builtins__"));
	  UNLESS (T) return -1;
	  builtins=T;
	  UNLESS (get_transaction=PyObject_GetAttrString(builtins,
							 "get_transaction"))
	    PyErr_Clear();
	}
      if (get_transaction)
	{    
	  UNLESS (T=PyObject_CallObject(get_transaction,NULL)) return -1;
	  ASSIGN(T,PyObject_GetAttr(T,py_register));
	  UNLESS (T) return -1;
	  ASSIGN(T, PyObject_CallFunction(T,"O",self));
	  if (T) Py_DECREF(T);
	  else return -1;
	}

      self->state=cPersistent_CHANGED_STATE;
    }

  return 0;
}

static PyObject *
Per___changed__(cPersistentObject *self, PyObject *args)
{
  PyObject *v=0;

  if (args && ! PyArg_ParseTuple(args, "|O",&v)) return NULL;
  if (! v) return PyObject_GetAttr(OBJECT(self), py__p_changed);

  if (PyObject_IsTrue(v)) 
    {
      if (changed(self) < 0) return NULL;
    }
  else if (self->state >= 0) self->state=cPersistent_UPTODATE_STATE;

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
Per__p_deactivate(cPersistentObject *self, PyObject *args)
{
  PyObject *init=0, *dict;

#ifdef DEBUG_LOG
  if (idebug_log < 0) call_debug("reinit",self);
#endif

  if (args && ! PyArg_ParseTuple(args,"")) return NULL;

  if (self->state==cPersistent_UPTODATE_STATE && self->jar &&
      HasInstDict(self) && (dict=INSTANCE_DICT(self)))
    {
      PyDict_Clear(dict);
      /* Note that we need to set to ghost state unless we are 
	 called directly. Methods that override this need to
         do the same! */
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}

/* Load the object's state if necessary and become sticky */
static int
Per_setstate(cPersistentObject *self)
{
  UPDATE_STATE_IF_NECESSARY(self, -1);
  self->state=cPersistent_STICKY_STATE;
  return 0;
}

static PyObject *
Per__getstate__(self,args)
     cPersistentObject *self;
     PyObject *args;
{
  PyObject *__dict__, *d=0;

  UNLESS(PyArg_ParseTuple(args, "")) return NULL;

#ifdef DEBUG_LOG
  if(idebug_log < 0) call_debug("get",self);
#endif

  UPDATE_STATE_IF_NECESSARY(self, NULL);

  if(HasInstDict(self) && (__dict__=INSTANCE_DICT(self)))
    {
      PyObject *k, *v;
      int pos;
      char *ck;
	  
      for(pos=0; PyDict_Next(__dict__, &pos, &k, &v); )
	{
	  if(PyString_Check(k) && (ck=PyString_AsString(k)) &&
	     (*ck=='_' && ck[1]=='v' && ck[2]=='_'))
	    {
	      UNLESS(d=PyDict_New()) goto err;
	      for(pos=0; PyDict_Next(__dict__, &pos, &k, &v); )
		UNLESS(PyString_Check(k) && (ck=PyString_AsString(k)) &&
		       (*ck=='_' && ck[1]=='v' && ck[2]=='_'))
		  if(PyDict_SetItem(d,k,v) < 0) goto err;
	      return d;
	    }
	}
    }
  else
    __dict__=Py_None;

  Py_INCREF(__dict__);
  return __dict__;

err:
  Py_XDECREF(d);
  return NULL;
}  

static PyObject *
Per__setstate__(self,args)
     cPersistentObject *self;
     PyObject *args;
{
  PyObject *__dict__, *v, *keys=0, *key=0, *e=0;
  int l, i;

  if(HasInstDict(self))
    {

       UNLESS(PyArg_ParseTuple(args, "O", &v)) return NULL;
#ifdef DEBUG_LOG
       if(idebug_log < 0) call_debug("set",self);
#endif
       if(v!=Py_None)
	 {
	   __dict__=INSTANCE_DICT(self);
	   
	   if(PyDict_Check(v))
	     {
	       for(i=0; PyDict_Next(v,&i,&key,&e);)
		 if(PyObject_SetItem(__dict__,key,e) < 0)
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
		   UNLESS(-1 != PyObject_SetItem(__dict__,key,e)) goto err;
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
Per_dealloc(self)
	cPersistentObject *self;
{
#ifdef DEBUG_LOG
  if(idebug_log < 0) call_debug("del",self);
#endif
  Py_XDECREF(self->jar);
  Py_XDECREF(self->oid);
  Py_DECREF(self->ob_type);
  PyMem_DEL(self);
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

  if(n && *n++=='_')
    if(*n++=='p' && *n++=='_')
      {
	switch(*n++)
	  {
	  case 'o':
	    if(*n++=='i' && *n++=='d' && ! *n) return orNothing(self->oid);
	    break;
	  case 'j':
	    if(*n++=='a' && *n++=='r' && ! *n) return orNothing(self->jar);
	    break;
	  case 'c':
	    if(strcmp(n,"hanged")==0)
	      {
		if(self->state < 0)
		  {
		    Py_INCREF(Py_None);
		    return Py_None;
		  }
		return PyInt_FromLong(self->state ==
				      cPersistent_CHANGED_STATE);
	      }
	    break;
	  case 's':
	    if(strcmp(n,"erial")==0)
	      return PyString_FromStringAndSize(self->serial, 8);
	    if(strcmp(n,"elf")==0) 
	      return orNothing(OBJECT(self));
	    break;
	  case 'm':
	    if(strcmp(n,"time")==0)
	      {
		UPDATE_STATE_IF_NECESSARY(self, NULL);

		self->atime=((long)(time(NULL)/3))%65536;

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
  if(! (name && *name++=='_' && *name++=='_' &&
	(strcmp(name,"dict__")==0 || strcmp(name,"class__")==0
	 || strcmp(name, "of__")==0)))
    {
      UPDATE_STATE_IF_NECESSARY(self, NULL);

      self->atime=((long)(time(NULL)/3))%65536;
    }

  return getattrf((PyObject *)self, oname);
}

static PyObject*
Per_getattro(cPersistentObject *self, PyObject *name)
{
  char *s=NULL;
  PyObject *r;

  if (PyString_Check(name))
    UNLESS(s=PyString_AsString(name)) return NULL;

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
bad_delattr()
{
  PyErr_SetString(PyExc_AttributeError,
		  "delete undeletable attribute");
  return -1;
}

static int
_setattro(cPersistentObject *self, PyObject *oname, PyObject *v,
	     int (*setattrf)(PyObject *, PyObject*, PyObject*))
{
  char *name="";

  UNLESS(oname) return -1;
  if(PyString_Check(oname)) UNLESS(name=PyString_AsString(oname)) return -1;
	
  if(*name=='_' && name[1]=='p' && name[2]=='_')
    {
      if(name[3]=='o' && name[4]=='i' && name[5]=='d' && ! name[6])
	{
	  Py_XINCREF(v);
	  ASSIGN(self->oid, v);
	  return 0;
	}
      if(name[3]=='j' && name[4]=='a' && name[5]=='r' && ! name[6])
	{
	  Py_XINCREF(v);
	  ASSIGN(self->jar, v);
	  return 0;
	}
      if(name[3]=='s' && strcmp(name+4,"erial")==0)
	{
	  if (v)
	    {
	      if (PyString_Check(v) && PyString_Size(v)==8)
		memcpy(self->serial, PyString_AS_STRING(v), 8);
	      else
		{
		  PyErr_SetString(PyExc_ValueError,
				  "_p_serial must be an 8-character string");
		  return -1;
		}
	    }
	  else
	    memset(self->serial, 0, 8);
	  return 0;
	}
      if(name[3]=='c' && strcmp(name+4,"hanged")==0) 
	{
	  if (! v)
	    {
	      /* delatter is used to invalidate the object
	         *even* if it has changed.
	       */
	      if (self->state != cPersistent_GHOST_STATE)
		self->state = cPersistent_UPTODATE_STATE;
	      v=Py_None;
	    }
	  if (v==Py_None)
	    {
	      v=PyObject_GetAttr(OBJECT(self), py__p_deactivate);
	      if (v) ASSIGN(v, PyObject_CallObject(v, NULL));
	      if (v) Py_DECREF(v);
	      self->state=cPersistent_GHOST_STATE;
	      return 0;
	    }
	  if (PyObject_IsTrue(v)) return changed(self);
	  if (self->state >= 0) self->state=cPersistent_UPTODATE_STATE;
	  return 0;
	}
    }
  else
    {
      UPDATE_STATE_IF_NECESSARY(self, -1);
      
      /* Record access times */
      self->atime=((long)(time(NULL)/3))%65536;

      if((! (*name=='_' && name[1]=='v' && name[2]=='_'))
	 && (self->state != cPersistent_CHANGED_STATE && self->jar)
	 && setattrf
	 )
	if(changed(self) < 0) return -1;
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
	    & EXTENSIONCLASS_USERSETATTR_FLAG)
      )
    {
      r=_setattro(self,oname, v, NULL);
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
	0L,0L,"",
	METHOD_CHAIN(Per_methods),
	PERSISTENCE_FLAGS,
};

static PyExtensionClass Overridable = {
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
	0L,0L,"",
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
  if(debug_log) idebug_log=-1;
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
  (intfunctionwithpythonarg)Per_setstate,
  (pergetattr)Per_getattr,
  (persetattr)_setattro,
};

void
initcPersistence()
{
  PyObject *m, *d;
  char *rev="$Revision: 1.40 $";

  TimeStamp=PyString_FromString("TimeStamp");
  if (! TimeStamp) return;
  ASSIGN(TimeStamp, PyImport_Import(TimeStamp));
  if (! TimeStamp) return;
  ASSIGN(TimeStamp, PyObject_GetAttrString(TimeStamp, "TimeStamp"));
  if (! TimeStamp) return;
  
  m = Py_InitModule4("cPersistence", cP_methods,
		     "",
		     (PyObject*)NULL,PYTHON_API_VERSION);

  init_strings();

  d = PyModule_GetDict(m);
  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  PyExtensionClass_Export(d, "Persistent",  Pertype);
  PyExtensionClass_Export(d, "Overridable", Overridable);

  cPersistenceCAPI=&truecPersistenceCAPI;
  PyDict_SetItemString(d, "CAPI",
		       PyCObject_FromVoidPtr(cPersistenceCAPI,NULL));

  if (PyErr_Occurred())
    Py_FatalError("can't initialize module cDocumentTemplate");
}
