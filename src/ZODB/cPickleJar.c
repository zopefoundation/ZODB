/***********************************************************
     Copyright 

       Copyright 1997 Digital Creations, L.L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved. 

 ***********************************************************/


static char cPickleJar_module_documentation[] = 
""
"\n$Id: cPickleJar.c,v 1.3 1997/12/15 16:36:15 jim Exp $"
;

#include "ExtensionClass.h"

static PyObject *ErrorObject;

/* ----------------------------------------------------- */

static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define OBJECT(O) ((PyObject*)(O))
#define LIST(O) ((PyListObject*)(O))


static PyObject *Pickler, *StringIO, *arg0, *arg1,
  *py__p_oid, *py__p_jar, *py_new_oid, *py__p_changed, *py_persistent_id,
  *py_db, *py_store, *py_seek, *py_getvalue, *py_cache, *py_dump,
  *py_clear_memo, *py___class__, *py___getinitargs__,
  *py___getstate__, *py___changed__, *py_info;

/* Declarations for objects of type pid */

typedef struct {
  PyObject_HEAD
  PyObject *jar;
  PyObject *stack;  
} pidobject;

staticforward PyTypeObject PidType;
#define PID(O) ((pidobject*)(O))

static int
po(PyObject *o)
{
  int r;
  r=PyObject_Print(o, stderr, 0);
  fprintf(stderr,"\n");
  fflush(stderr);
  return r;
}

static pidobject *
newpid(PyObject *jar, PyObject *stack)
{
  pidobject *self;
	
  UNLESS(self = PyObject_NEW(pidobject, &PidType)) return NULL;
  Py_INCREF(jar);
  self->jar=jar;
  Py_INCREF(stack);
  self->stack=stack;
  return self;
}

static void
pid_dealloc(pidobject *self)
{
  Py_DECREF(self->jar);
  Py_DECREF(self->stack);
  PyMem_DEL(self);
}

static PyObject *
pid_plan(pidobject *self, PyObject *object)
{
  PyObject *oid=0, *jar=0, *cls=0;

  UNLESS(oid=PyObject_GetAttr(object, py__p_oid)) goto not_persistent;
  if(oid != Py_None) UNLESS(jar=PyObject_GetAttr(object, py__p_jar)) goto err;
  if(jar != self->jar)
    {
      UNLESS_ASSIGN(oid, PyObject_GetAttr(self->jar, py_new_oid)) goto err;
      UNLESS_ASSIGN(oid, PyObject_CallObject(oid, NULL)) goto err;
      if(PyObject_SetAttr(object, py__p_oid, oid) < 0) goto err;
      if(PyObject_SetAttr(object, py__p_jar, self->jar) < 0) goto err;
      if(PyList_Append(self->stack, object) < 0) goto err;
    }
  else
    {
      UNLESS_ASSIGN(jar,PyObject_GetAttr(object, py__p_changed)) goto err;
      if(PyObject_IsTrue(jar) && PyList_Append(self->stack, object) < 0)
	goto err;
    }
  Py_XDECREF(jar);

  UNLESS(cls=PyObject_GetAttr(object, py___class__)) goto err;
  UNLESS(jar=PyObject_GetAttr(cls, py___getinitargs__))
    {
      PyErr_Clear();
      UNLESS_ASSIGN(oid, Py_BuildValue("OO", oid, cls)) goto err;
    }
  else Py_DECREF(jar);
  Py_DECREF(cls);

  return oid;

err:
  Py_XDECREF(jar);
  Py_XDECREF(cls);
  Py_DECREF(oid);
  return NULL;

not_persistent:
  PyErr_Clear();
  Py_INCREF(Py_None);
  return Py_None;
}  

static PyObject *
pid_call(pidobject *self, PyObject *args, PyObject *kw)
{
  PyObject *object;

  UNLESS(PyArg_ParseTuple(args,"O",&object)) return NULL;
  object=pid_plan(self,object);
  return object;
}  

static PyTypeObject PidType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "pid",			/*tp_name*/
  sizeof(pidobject),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /**************** methods *******************/
  (destructor)pid_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)0,			/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  0,				/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)pid_call,	/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,		/*tp_getattro*/
  (setattrofunc)0,		/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "internal type used in pickle jars"
};

static int 
call_sub(PyObject *sub, PyObject *args)
{
  UNLESS(sub=PyObject_CallObject(sub,args)) return -1;
  Py_DECREF(sub);
  return 0;
}

static PyObject *
pj_store(PyObject *self, PyObject *args)
{
  PyObject *object, *T=0, *stack, *state=0, *topoid=0, *file=0, *pickler=0,
    *store=0, *seek=0, *cache=0, *dump=0, *clear_memo=0,
    *o=0, *oid=0, *r=0;
  int l;

  UNLESS(PyArg_ParseTuple(args,"O|O",&object,&T)) return NULL;
  args=NULL;
  UNLESS(stack=PyList_New(0)) return NULL;
  UNLESS(state=OBJECT(newpid(self,stack))) goto err;
  UNLESS(topoid=pid_plan(PID(state),object)) goto err;
  if((l=PyList_Size(stack)) < 0) goto err;
  if(! l)
    {
      Py_DECREF(stack);
      Py_DECREF(state);
      return topoid;
    }
  if(T)
    {
      UNLESS(T=PyObject_GetAttr(T,py_info)) goto err;
      UNLESS_ASSIGN(T,PyObject_CallObject(T,NULL)) goto err;
    }
  else UNLESS(T=PyString_FromString("")) goto err;

  UNLESS(file=PyObject_CallObject(StringIO, NULL)) goto err;
  UNLESS(pickler=PyObject_CallFunction(Pickler,"Oi",file,1)) goto err;
  if(PyObject_SetAttr(pickler, py_persistent_id, state) < 0) goto err;
  UNLESS(store=PyObject_GetAttr(self,py_db)) goto err;
  UNLESS_ASSIGN(store,PyObject_GetAttr(store,py_store)) goto err;
  UNLESS(seek=PyObject_GetAttr(file,py_seek)) goto err;
  UNLESS_ASSIGN(file, PyObject_GetAttr(file, py_getvalue)) goto err;
  UNLESS(cache=PyObject_GetAttr(self,py_cache)) goto err;
  UNLESS(dump=PyObject_GetAttr(pickler, py_dump)) goto err;
  UNLESS(clear_memo=PyObject_GetAttr(pickler, py_clear_memo)) goto err;
  
  while(l)
    {
      ASSIGN(o,PyList_GET_ITEM(LIST(stack),l-1));
      Py_INCREF(o);
      if(PyList_SetSlice(stack,l-1,l,NULL) < 0) goto err;
      UNLESS_ASSIGN(oid,PyObject_GetAttr(o,py__p_oid)) goto err;

      UNLESS_ASSIGN(state,PyObject_GetAttr(o,py___class__)) goto err;
      ASSIGN(args,PyObject_GetAttr(state,py___getinitargs__));
      if(args)
	{
	  UNLESS_ASSIGN(args, PyObject_CallObject(args, NULL)) goto err;
	}
      else
	{
	  PyErr_Clear();
	  args=Py_None;
	  Py_INCREF(args);
	}
      UNLESS_ASSIGN(args, Py_BuildValue("OO",state,args)) goto err;
      UNLESS_ASSIGN(args, Py_BuildValue("(O)",args)) goto err;
      if(call_sub(seek,arg0) < 0) goto err;
      if(call_sub(clear_memo,NULL) < 0) goto err;
      UNLESS_ASSIGN(state, PyObject_CallObject(dump, args));

      UNLESS_ASSIGN(state, PyObject_GetAttr(o, py___getstate__)) goto err;
      UNLESS_ASSIGN(state, PyObject_CallObject(state, NULL)) goto err;
      UNLESS_ASSIGN(state, Py_BuildValue("(O)",state)) goto err;
      UNLESS_ASSIGN(state, PyObject_CallObject(dump,state)) goto err;

      UNLESS_ASSIGN(state, PyObject_CallObject(file,arg1)) goto err;
      UNLESS_ASSIGN(state, Py_BuildValue("OOO",oid,state,T)) goto err;
      UNLESS_ASSIGN(state, PyObject_CallObject(store,state)) goto err;

      if(PyObject_SetItem(cache,oid,o) < 0) goto err;
      UNLESS_ASSIGN(o, PyObject_GetAttr(o, py___changed__)) goto err;
      UNLESS_ASSIGN(o, PyObject_CallObject(o, arg0)) goto err;
      
      if((l=PyList_Size(stack)) < 0) goto err;
    }

  r=topoid;
  topoid=NULL;

err:
  Py_DECREF(stack);
  Py_XDECREF(T);
  Py_XDECREF(state);
  Py_XDECREF(topoid);
  Py_XDECREF(file);
  Py_XDECREF(pickler);
  Py_XDECREF(store);
  Py_XDECREF(seek);
  Py_XDECREF(cache);
  Py_XDECREF(dump);
  Py_XDECREF(clear_memo);
  Py_XDECREF(o);
  Py_XDECREF(oid);

  return r;
}

/* End of code for pid objects */
/* -------------------------------------------------------- */


/* List of methods defined in the module */

static struct PyMethodDef Module_Level__methods[] = {
  
  {NULL, (PyCFunction)NULL, 0, NULL}		/* sentinel */
};

/* Initialization function for the module (*must* be called initcPickleJar) */

static struct PyMethodDef PickleJar_methods[] = {
  {"store",(PyCFunction)pj_store,1,""},  
  {NULL,		NULL}		/* sentinel */
};

void
initcPickleJar()
{
  PyObject *m, *d;
  char *rev="$Revision: 1.3 $";
  PURE_MIXIN_CLASS(PickleJar,"",PickleJar_methods);

  UNLESS(Pickler=PyImport_ImportModule("cPickle")) return;
  UNLESS_ASSIGN(Pickler, PyObject_GetAttrString(Pickler, "Pickler")) return;
  UNLESS(StringIO=PyImport_ImportModule("cStringIO")) return;
  UNLESS_ASSIGN(StringIO, PyObject_GetAttrString(StringIO, "StringIO")) return;
#define INIT_STRING(S) if(!(py_ ## S = PyString_FromString(#S))) return

  INIT_STRING(_p_oid);
  INIT_STRING(_p_jar);
  INIT_STRING(new_oid);
  INIT_STRING(_p_changed);
  INIT_STRING(persistent_id);
  INIT_STRING(db);
  INIT_STRING(store);
  INIT_STRING(seek);
  INIT_STRING(getvalue);
  INIT_STRING(cache);
  INIT_STRING(dump);
  INIT_STRING(clear_memo);
  INIT_STRING(__class__);
  INIT_STRING(__getinitargs__);
  INIT_STRING(__getstate__);
  INIT_STRING(__changed__);
  INIT_STRING(info);

  UNLESS(arg0=Py_BuildValue("(i)",0)) return;
  UNLESS(arg1=Py_BuildValue("(i)",1)) return;

  /* Create the module and add the functions */
  m = Py_InitModule4("cPickleJar", Module_Level__methods,
		     cPickleJar_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);
  PyExtensionClass_Export(d,"PickleJar",PickleJarType);

  PidType.ob_type=&PyType_Type;

  PyDict_SetItemString(d, "__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  	
  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module cPickleJar");
}

/*****************************************************************************
Revision Log:

  $Log: cPickleJar.c,v $
  Revision 1.3  1997/12/15 16:36:15  jim
  Updated to use new pickling protocol.

  Revision 1.2  1997/09/08 18:59:55  jim
  Added binary output flag. Waaaaaaa!

  Revision 1.1  1997/05/08 19:28:05  jim
  *** empty log message ***

  $Revision 1.1  1997/02/24 23:25:42  jim
  $initial
  $

*****************************************************************************/
