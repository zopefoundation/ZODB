/***********************************************************************

  $Id: cPersistence.c,v 1.25 1998/11/11 02:00:56 jim Exp $

  C Persistence Module

     Copyright 

       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved. 


*****************************************************************************/
static char *what_string = "$Id: cPersistence.c,v 1.25 1998/11/11 02:00:56 jim Exp $";

#include <time.h>
#include "cPersistence.h"

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)

static PyObject *py_keys, *py_setstate, *py___dict__;

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
  r=PyObject_CallFunction(debug_log,"s(ss#i)",event,
			  self->ob_type->tp_name, self->oid, 8,
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
  INIT_STRING(__dict__);
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
  self->state=cPersistent_STICKY_STATE; 	                 \
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
  if (v && ! PyObject_IsTrue(v))
    {
      PyErr_SetString(PyExc_TypeError,
			 "Only true arguments are allowed.");
      return NULL;
    }
  if (changed(self) < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
Per__p_deactivate(cPersistentObject *self, PyObject *args)
{
  PyObject *init=0, *copy, *dict;

#ifdef DEBUG_LOG
  if (idebug_log < 0) call_debug("reinit",self);
#endif

  if (args && ! PyArg_ParseTuple(args,"")) return NULL;

  if (self->state==cPersistent_UPTODATE_STATE && self->jar &&
      HasInstDict(self) && (dict=INSTANCE_DICT(self)))
    {
      PyDict_Clear(dict);
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}

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
  Py_DECREF(__dict__);
  Py_XDECREF(d);
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
  PyMem_DEL(self);
}

static PyObject *
Per_getattr(cPersistentObject *self, PyObject *oname, char *name,
	 PyObject *(*getattrf)(PyObject *, PyObject*))
{
  char *n=name;

  if(*n++=='_')
    if(*n++=='p' && *n++=='_')
      {
	switch(*n++)
	  {
	  case 'o':
	    if(*n++=='i' && *n++=='d' && ! *n)
	      return PyString_FromStringAndSize(self->oid, 8);
	    break;
	  case 'j':
	    if(*n++=='a' && *n++=='r' && ! *n)
	      {
		if(self->jar)
		  {
		    Py_INCREF(self->jar);
		    return self->jar;
		  }
		else
		  {
		    Py_INCREF(Py_None);
		    return Py_None;
		  }
	      }
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
	  }

	return getattrf((PyObject *)self, oname);
      }
  if(! (*name++=='_' && *name++=='_' &&
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
  char *s;

  UNLESS(s=PyString_AsString(name)) return NULL;
  return Per_getattr(self,name,s, PyExtensionClassCAPI->getattro);
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
	  if (! v) return bad_delattr();
	  if (PyString_Check(v) && PyString_GET_SIZE(v)==8)
	    memcpy(self->oid, PyString_AS_STRING(v), 8);
	  else
	    {
	      PyErr_SetString(PyExc_AttributeError,
			      "_p_oid must be an 8-character string");
	      return -1;
	    }
	  return 0;
	}
      if(name[3]=='j' && name[4]=='a' && name[5]=='r' && ! name[6])
	{
	  ASSIGN(self->jar, v);
	  Py_XINCREF(self->jar);
	  return 0;
	}
      if(strcmp(name+3,"changed")==0) 
	{
	  if (! v) return bad_delattr();
	  if (v==Py_None)
	    {
	      if (Per__p_deactivate(self, NULL)) Py_DECREF(Py_None);
	      return 0;
	    }
	  if (PyObject_IsTrue(v)) return changed(self);
	  if (self->state >= 0) self->state=cPersistent_UPTODATE_STATE;
	  return 0;
	}
    }
  else
    {
      PyObject *r;

      UPDATE_STATE_IF_NECESSARY(self, -1);
      
      /* Record access times */
      self->atime=((long)(time(NULL)/3))%65536;

      if(! (*name=='_' && name[1]=='v' && name[2]=='_')
	 && self->state != cPersistent_CHANGED_STATE && self->jar)
	if(changed(self) < 0) return -1;
    }

  return setattrf((PyObject*)self,oname,v);
}

static int
Per_setattro(cPersistentObject *self, PyObject *oname, PyObject *v)
{
  return _setattro(self,oname, v, PyExtensionClassCAPI->setattro);
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
	EXTENSIONCLASS_BASICNEW_FLAG,
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
  char *rev="$Revision: 1.25 $";

  m = Py_InitModule4("cPersistence", cP_methods,
		     "",
		     (PyObject*)NULL,PYTHON_API_VERSION);

  init_strings();

  d = PyModule_GetDict(m);
  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  PyExtensionClass_Export(d,"Persistent",Pertype);

  cPersistenceCAPI=&truecPersistenceCAPI;
  PyDict_SetItemString(d, "CAPI",
		       PyCObject_FromVoidPtr(cPersistenceCAPI,NULL));

  if (PyErr_Occurred())
    Py_FatalError("can't initialize module cDocumentTemplate");
}
