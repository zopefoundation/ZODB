/*

  $Id: cPersistence.c,v 1.2 1997/02/21 20:49:09 jim Exp $

  C Persistence Module

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


  Full description

  $Log: cPersistence.c,v $
  Revision 1.2  1997/02/21 20:49:09  jim
  Added logic to treat attributes starting with _v_ as volatile.
  Changes in these attributes to not make the object thing it's been
  saved and these attributes are not saved by the default __getstate__
  method.

  Revision 1.1  1997/02/14 20:24:55  jim
  *** empty log message ***


*/
static char *what_string = "$Id: cPersistence.c,v 1.2 1997/02/21 20:49:09 jim Exp $";

#include <time.h>
#include "Python.h"
#include "ExtensionClass.h"

#define ASSIGN(V,E) {PyObject *__e; __e=(E); Py_XDECREF(V); (V)=__e;}
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E) UNLESS(V)

static PyObject *py_store, *py_oops, *py_keys, *py_setstate, *py___changed__,
  *py___dict__, *py_one;

static void
init_strings()
{
#define INIT_STRING(S) py_ ## S = PyString_FromString(#S)
  INIT_STRING(store);
  INIT_STRING(oops);
  INIT_STRING(keys);
  INIT_STRING(setstate);
  INIT_STRING(__changed__);
  INIT_STRING(__dict__);
  py_one=PyInt_FromLong(1);
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

/* ----------------------------------------------------- */

/* Declarations for objects of type Persistent */

typedef struct {
  PyObject_HEAD
  PyObject *oid;
  PyObject *jar;
  PyObject *rtime;
  time_t atime;
  int state;			
#define GHOST_STATE -1
#define UPTODATE_STATE 0
#define CHANGED_STATE 1
} Perobject;

staticforward PyExtensionClass Pertype;
staticforward PyExtensionClass TPertype;

/* ---------------------------------------------------------------- */

/* ---------------------------------------------------------------- */

static char Per___changed____doc__[] = 
"__changed__([flag]) -- Flag or determine whether an object has changed\n"
"	\n"
"If a value is specified, then it should indicate whether an\n"
"object has or has not been changed.  If no value is specified,\n"
"then the return value will indicate whether the object has\n"
"changed.\n"
;

static PyObject *changed_args=(PyObject*)Per___changed____doc__;

static PyObject *
Per___changed__(self, args)
	Perobject *self;
	PyObject *args;
{
  PyObject *o;

  if(args)
    {
      UNLESS(PyArg_Parse(args, "O", &o)) return NULL;
      if(self->state != GHOST_STATE) self->state=PyObject_IsTrue(o);

      Py_INCREF(Py_None);
      return Py_None;
    }
  else
    return PyInt_FromLong(self->state == CHANGED_STATE);
}

static char Per___save____doc__[] = 
"__save__() -- Update the object in a persistent database."
;

static PyObject *
Per___save__(self, args)
	Perobject *self;
	PyObject *args;
{
  if(self->oid && self->jar && self->state != GHOST_STATE)
    return callmethod1(self->jar,py_store,(PyObject*)self);
  Py_INCREF(Py_None);
  return Py_None;
}


static char Per___inform_commit____doc__[] = 
"__inform_commit__(transaction,start_time) -- Commit object changes"
;

static char Per___inform_abort____doc__[] = 
"__inform_abort__(transaction,start_time) -- Abort object changes"
;

static PyObject *
Per___inform_abort__(self, args)
	Perobject *self;
	PyObject *args;
{
  PyObject *transaction, *start_time;

  UNLESS(PyArg_Parse(args, "(OO)", &transaction, &start_time)) return NULL;
  if(self->oid && self->jar && self->state != GHOST_STATE)
    {
      args=callmethod2(self->jar,py_oops,(PyObject*)self,start_time);
      if(args)
	Py_DECREF(args);
      else
	PyErr_Clear();
    }
  Py_INCREF(Py_None);
  return Py_None;
}

static char Per__p___init____doc__[] = 
"_p___init__(oid,jar) -- Initialize persistence management data"
;

static PyObject *
Per__p___init__(self, args)
     Perobject *self;
     PyObject *args;
{
  PyObject *oid, *jar;

  UNLESS(PyArg_Parse(args, "(OO)", &oid, &jar)) return NULL;
  Py_INCREF(oid);
  Py_INCREF(jar);
  ASSIGN(self->oid, oid);
  ASSIGN(self->jar, jar);
  self->state=GHOST_STATE;
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *
Per__getstate__(self,args)
     Perobject *self;
     PyObject *args;
{
  PyObject *__dict__, *d=0;

  UNLESS(PyArg_Parse(args, "")) return NULL;

  /* Update state, if necessary */
  if(self->state==GHOST_STATE && self->jar)
    {
      PyObject *r;
      
      self->state=UPTODATE_STATE;
      UNLESS(r=callmethod1(self->jar,py_setstate,(PyObject*)self))
	{
	  self->state=GHOST_STATE;
	  return NULL;
	}
      Py_DECREF(r);
    }

  UNLESS(__dict__=PyObject_GetAttr((PyObject*)self,py___dict__))
    return NULL;

  if(PyDict_Check(__dict__))
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
	      Py_DECREF(__dict__);
	      return d;
	    }
	} 
    }    
  return __dict__;
err:
  Py_DECREF(__dict__);
  Py_XDECREF(d);
}  

static PyObject *
Per__setstate__(self,args)
     Perobject *self;
     PyObject *args;
{
  PyObject *__dict__, *v, *keys=0, *key=0, *e=0;
  int l, i;

  UNLESS(PyArg_Parse(args, "O", &v)) return NULL;
  self->state=UPTODATE_STATE;
  if(v!=Py_None)
    {
      UNLESS(__dict__=PyObject_GetAttr((PyObject*)self,py___dict__))
	goto err;
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
      Py_DECREF(__dict__);
    }
  Py_INCREF(Py_None);
  return Py_None;
err:
  self->state=GHOST_STATE;
  Py_XDECREF(key);
  Py_XDECREF(e);
  Py_XDECREF(keys);
  Py_DECREF(__dict__);
  return NULL;
}  


static struct PyMethodDef Per_methods[] = {
  {"__changed__",	(PyCFunction)Per___changed__,	0,	Per___changed____doc__},
  {"__save__",	(PyCFunction)Per___save__,	0,	Per___save____doc__},
  {"__inform_commit__",	(PyCFunction)Per___save__,	0,	Per___inform_commit____doc__},
  {"__inform_abort__",	(PyCFunction)Per___inform_abort__,	0,	Per___inform_abort____doc__},
  {"_p___init__",	(PyCFunction)Per__p___init__,	0,	Per__p___init____doc__},
  {"__getstate__",	(PyCFunction)Per__getstate__,	0,
   "__getstate__() -- Return the state of the object" },
  {"__setstate__",	(PyCFunction)Per__setstate__,	0,
   "__setstate__(v) -- Restore the saved state of the object from v" },
  
  {NULL,		NULL}		/* sentinel */
};

/* ---------- */

static void
Per_dealloc(self)
	Perobject *self;
{
  Py_XDECREF(self->oid);
  Py_XDECREF(self->jar);

  Py_XDECREF(self->rtime);
  PyMem_DEL(self);
}

static PyObject *
Per_getattr(Perobject *self, PyObject *oname, char *name)
{
  char *n=name;

  if(*n++=='_')
    if(*n++=='p' && *n++=='_')
      {
	switch(*n++)
	  {
	  case 'o':
	    if(*n++=='i' && *n++=='d' && ! *n)
	      {
		if(self->oid)
		  {
		    Py_INCREF(self->oid);
		    return self->oid;
		  }
		else
		  {
		    Py_INCREF(Py_None);
		    return Py_None;
		  }
	      }
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
	      return PyInt_FromLong(self->state == CHANGED_STATE);
	  case 'r':
	    if(strcmp(n,"ead_time")==0) 
	      {
		if(self->rtime)
		  {
		    Py_INCREF(self->rtime);
		    return self->rtime;
		  }
		else
		  return PyFloat_FromDouble(0.0);
	      }
	  }
      }
  if(! (*name++=='_' && *name++=='_' && strcmp(name,"dict__")==0))
    {
      /* Update state, if necessary */
      if(self->state==GHOST_STATE && self->jar)
	{
	  PyObject *r;
	  
	  self->state=UPTODATE_STATE;
	  UNLESS(r=callmethod1(self->jar,py_setstate,(PyObject*)self))
	    {
	      self->state=GHOST_STATE;
	      return NULL;
	    }
	  Py_DECREF(r);
	}
      
      /* Record access times */
      self->atime=time(NULL);
    }

  return Py_FindAttr((PyObject *)self, oname);
}

static PyObject*
Per_getattro(Perobject *self, PyObject *name)
{
  char *s;

  UNLESS(s=PyString_AsString(name)) return NULL;
  return Per_getattr(self,name,s);
}

static int
Per_setattro(Perobject *self, PyObject *oname, PyObject *v)
{
  char *name="";

  UNLESS(oname) return -1;
  if(PyString_Check(oname)) UNLESS(name=PyString_AsString(oname)) return -1;
	
  if(*name=='_' && name[1]=='p' && name[2]=='_')
    {
      if(name[3]=='o' && name[4]=='i' && name[5]=='d' && ! name[6])
	{
	  ASSIGN(self->oid, v);
	  Py_INCREF(self->oid);
	  return 0;
	}
      if(name[3]=='j' && name[4]=='a' && name[5]=='r' && ! name[6])
	{
	  ASSIGN(self->jar, v);
	  Py_INCREF(self->jar);
	  return 0;
	}
      if(strcmp(name+3,"changed")==0) 
	{
	  self->state=PyObject_IsTrue(v);
	  return 0;
	}
      if(strcmp(name+3,"read_time")==0) 
	{
	  ASSIGN(self->rtime, v);
	  Py_INCREF(self->rtime);
	  return 0;
	}
    }
  else
    {
      PyObject *r;

      /* Update state, if necessary */
      if(self->state==GHOST_STATE && self->jar)
	{
	  
	  self->state=UPTODATE_STATE;
	  UNLESS(r=callmethod1(self->jar,py_setstate,(PyObject*)self))
	    {
	      self->state=GHOST_STATE;
	      return -1;
	    }
	  Py_DECREF(r);
	}
      
      /* Record access times */
      self->atime=time(NULL);

      if(! (*name=='_' && name[1]=='v' && name[2]=='_')
	 && self->state != CHANGED_STATE && self->jar)
	{
	  UNLESS(r=callmethod1((PyObject*)self,py___changed__,py_one))
	    return -1;
	  Py_DECREF(r);
	}
    }

  return PyEC_SetAttr((PyObject*)self,oname,v);
}

static char Pertype__doc__[] = 
"Persistent object support mix-in class\n"
"\n"
"When a persistent object is loaded from a database, the object's\n"
"data is not immediately loaded.  Loading of the objects data is\n"
"defered until an attempt is made to access an attribute of the\n"
"object. \n"
"\n"
"The object also tries to keep track of whether it has changed.  It\n"
"is easy for this to be done incorrectly.  For this reason, methods\n"
"of subclasses that change state other than by setting attributes\n"
"should: 'self.__changed__(1)' to flag instances as changed.\n"
"\n"
"Data are not saved automatically.  To save an object's state, call\n"
"the object's '__save__' method.\n"
"\n"
"You must not override the object's '__getattr__' and '__setattr__'\n"
"methods.  If you override the objects '__getstate__' method, then\n"
"you must be careful not to include any attributes with names\n"
"starting with '_p_' in the state.\n"
;

static PyExtensionClass Pertype = {
	PyObject_HEAD_INIT(NULL)
	0,				/*ob_size*/
	"Persistent",			/*tp_name*/
	sizeof(Perobject),		/*tp_basicsize*/
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
	Pertype__doc__, 		/* Documentation string */
	METHOD_CHAIN(Per_methods)
};

/* End of code for Persistent objects */
/* -------------------------------------------------------- */

/* List of methods defined in the module */

static struct PyMethodDef cP_methods[] = {
	
	{NULL,		NULL}		/* sentinel */
};


/* Initialization function for the module (*must* be called initcPersistence) */

static char cPersistence_module_documentation[] = 
""
;

void
initcPersistence()
{
	PyObject *m, *d;

	/* Create the module and add the functions */
	m = Py_InitModule4("cPersistence", cP_methods,
		cPersistence_module_documentation,
		(PyObject*)NULL,PYTHON_API_VERSION);

	init_strings();

	/* Add some symbolic constants to the module */
	d = PyModule_GetDict(m);

	/* XXXX Add constants here */
	PyExtensionClass_Export(d,"Persistent",Pertype);

	/* Check for errors */
	CHECK_FOR_ERRORS("can't initialize module dt");
}

