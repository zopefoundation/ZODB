/*

 Copyright (c) 2003 Zope Corporation and Contributors.
 All Rights Reserved.

 This software is subject to the provisions of the Zope Public License,
 Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
 THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
 WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
 FOR A PARTICULAR PURPOSE.

*/
/* 

 Reusable pickle support code

 This is "includeware", meant to be used through a C include

 */

/* It's a dang shame we can't inherit __get/setstate__ from object :( */

static PyObject *str__slotnames__, *copy_reg_slotnames, *__newobj__;
static PyObject *str__getnewargs__, *str__getstate__;

static int
pickle_setup(void)
{
  PyObject *copy_reg;
  int r = -1;

#define DEFINE_STRING(S) \
  if(! (str ## S = PyString_FromString(# S))) return -1
  DEFINE_STRING(__slotnames__);
  DEFINE_STRING(__getnewargs__);
  DEFINE_STRING(__getstate__);
#undef DEFINE_STRING

  copy_reg = PyImport_ImportModule("copy_reg");
  if (copy_reg == NULL)
    return -1;

  copy_reg_slotnames = PyObject_GetAttrString(copy_reg, "_slotnames");
  if (copy_reg_slotnames == NULL)
    goto end;

  __newobj__ = PyObject_GetAttrString(copy_reg, "__newobj__");
  if (__newobj__ == NULL)
    goto end;

  r = 0;
 end:
  Py_DECREF(copy_reg);
  return r;
}

static PyObject *
pickle_slotnames(PyTypeObject *cls)
{
  PyObject *slotnames;

  slotnames = PyDict_GetItem(cls->tp_dict, str__slotnames__);
  if (slotnames != NULL) 
    {
      Py_INCREF(slotnames);
      return slotnames;
    }

  slotnames = PyObject_CallFunctionObjArgs(copy_reg_slotnames, (PyObject*)cls, 
                                           NULL);
  if (slotnames != NULL &&
      slotnames != Py_None &&
      ! PyList_Check(slotnames))
    {
      PyErr_SetString(PyExc_TypeError,
                      "copy_reg._slotnames didn't return a list or None");
      Py_DECREF(slotnames);
      slotnames = NULL;
    }
  
  return slotnames;
}

static PyObject *
pickle_copy_dict(PyObject *state)
{
  PyObject *copy, *key, *value;
  char *ckey;
  int pos = 0, nr;

  copy = PyDict_New();
  if (copy == NULL)
    return NULL;

  if (state == NULL)
    return copy;

  while ((nr = PyDict_Next(state, &pos, &key, &value))) 
    {
      if (nr < 0)
        goto err;

      if (key && PyString_Check(key))
        {
          ckey = PyString_AS_STRING(key);
          if (*ckey == '_' &&
              (ckey[1] == 'v' || ckey[1] == 'p') &&
              ckey[2] == '_')
            /* skip volatile and persistent */
            continue;
        }

      if (key != NULL && value != NULL &&
          (PyObject_SetItem(copy, key, value) < 0)
          )
        goto err;
    }
  
  return copy;
 err:
  Py_DECREF(copy);
  return NULL;
}


static char pickle___getstate__doc[] =
"Get the object serialization state\n"
"\n"
"If the object has no assigned slots and has no instance dictionary, then \n"
"None is returned.\n"
"\n"
"If the object has no assigned slots and has an instance dictionary, then \n"
"the a copy of the instance dictionary is returned. The copy has any items \n"
"with names starting with '_v_' or '_p_' ommitted.\n"
"\n"
"If the object has assigned slots, then a two-element tuple is returned.  \n"
"The first element is either None or a copy of the instance dictionary, \n"
"as described above. The second element is a dictionary with items \n"
"for each of the assigned slots.\n"
;

static PyObject *
pickle___getstate__(PyObject *self)
{
  PyObject *slotnames=NULL, *slots=NULL, *state=NULL;
  PyObject **dictp;
  int n=0;

  slotnames = pickle_slotnames(self->ob_type);
  if (slotnames == NULL)
    return NULL;

  dictp = _PyObject_GetDictPtr(self);
  if (dictp)
    state = pickle_copy_dict(*dictp);
  else 
    {
      state = Py_None;
      Py_INCREF(state);
    }

  if (slotnames != Py_None)
    {
      int i;

      slots = PyDict_New();
      if (slots == NULL)
        goto end;

      for (i = 0; i < PyList_GET_SIZE(slotnames); i++) 
        {
          PyObject *name, *value;
          char *cname;

          name = PyList_GET_ITEM(slotnames, i);
          if (PyString_Check(name))
            {
              cname = PyString_AS_STRING(name);
              if (*cname == '_' &&
                  (cname[1] == 'v' || cname[1] == 'p') &&
                  cname[2] == '_')
                /* skip volatile and persistent */
                continue;
            }

          value = PyObject_GetAttr(self, name);
          if (value == NULL)
            PyErr_Clear();
          else 
            {
              int err = PyDict_SetItem(slots, name, value);
              Py_DECREF(value);
              if (err)
                goto end;
              n++;
            }
        }
    }

  if (n) 
    state = Py_BuildValue("(NO)", state, slots);

 end:
  Py_XDECREF(slotnames);
  Py_XDECREF(slots);
  
  return state;
}

static int
pickle_setattrs_from_dict(PyObject *self, PyObject *dict)
{
  PyObject *key, *value;
  int pos = 0;
  
  if (! PyDict_Check(dict))
    {
      PyErr_SetString(PyExc_TypeError, "Expected dictionary");
      return -1;
    }
  
  while (PyDict_Next(dict, &pos, &key, &value)) 
    {
      if (key != NULL && value != NULL &&
          (PyObject_SetAttr(self, key, value) < 0)
          )
        return -1;
    }
  return 0;
}

static char pickle___setstate__doc[] =
"Set the object serialization state\n"
"\n"
"The state should be in one of 3 forms:\n"
"\n"
"- None\n"
"\n"
"  Ignored\n"
"\n"
"- A dictionary\n"
"\n"
"  In this case, the object's instance dictionary will be cleared and \n"
"  updated with the new state.\n"
"\n"
"- A two-tuple with a string as the first element.  \n"
"\n"
"  In this case, the method named by the string in the first element will be\n"
"  called with the second element.\n"
"\n"
"  This form supports migration of data formats.\n"
"\n"
"- A two-tuple with None or a Dictionary as the first element and\n"
"  with a dictionary as the second element.\n"
"\n"
"  If the first element is not None, then the object's instance dictionary \n"
"  will be cleared and updated with the value.\n"
"\n"
"  The items in the second element will be assigned as attributes.\n"
;

static PyObject *
pickle___setstate__(PyObject *self, PyObject *state)
{
  PyObject *slots=NULL;

  if (PyTuple_Check(state))
    {
      if (! PyArg_ParseTuple(state, "OO", &state, &slots))
        return NULL;
    }

  if (state != Py_None)
    {
      PyObject **dict;

      dict = _PyObject_GetDictPtr(self);
      if (dict)
        {
          if (*dict == NULL)
            {
              *dict = PyDict_New();
              if (*dict == NULL)
                return NULL;
            }
        }

      if (*dict != NULL)
        {
          PyDict_Clear(*dict);
          if (PyDict_Update(*dict, state) < 0)
            return NULL;
        }
      else if (pickle_setattrs_from_dict(self, state) < 0)
        return NULL;
    }

  if (slots != NULL && pickle_setattrs_from_dict(self, slots) < 0)
    return NULL;

  Py_INCREF(Py_None);
  return Py_None;
}

static char pickle___getnewargs__doc[] = 
"Get arguments to be passed to __new__\n"
;

static PyObject *
pickle___getnewargs__(PyObject *self)
{
  return PyTuple_New(0);
}

static char pickle___reduce__doc[] = 
"Reduce an object to contituent parts for serialization\n"
;

static PyObject *
pickle___reduce__(PyObject *self)
{
  PyObject *args=NULL, *bargs=0, *state=NULL;
  int l, i;
  
  bargs = PyObject_CallMethodObjArgs(self, str__getnewargs__, NULL);
  if (bargs == NULL)
    return NULL;

  l = PyTuple_Size(bargs);
  if (l < 0)
    goto end;

  args = PyTuple_New(l+1);
  if (args == NULL)
    goto end;
  
  Py_INCREF(self->ob_type);
  PyTuple_SET_ITEM(args, 0, (PyObject*)(self->ob_type));
  for (i = 0; i < l; i++)
    {
      Py_INCREF(PyTuple_GET_ITEM(bargs, i));
      PyTuple_SET_ITEM(args, i+1, PyTuple_GET_ITEM(bargs, i));
    }
  
  state = PyObject_CallMethodObjArgs(self, str__getstate__, NULL);
  if (state == NULL)
    goto end;

  state = Py_BuildValue("(OON)", __newobj__, args, state);

 end:
  Py_XDECREF(bargs);
  Py_XDECREF(args);

  return state;
}

#define PICKLE_GETSTATE_DEF \
{"__getstate__", (PyCFunction)pickle___getstate__, METH_NOARGS,      \
   pickle___getstate__doc},

#define PICKLE_SETSTATE_DEF \
{"__setstate__", (PyCFunction)pickle___setstate__, METH_O,           \
   pickle___setstate__doc},                                          

#define PICKLE_GETNEWARGS_DEF \
{"__getnewargs__", (PyCFunction)pickle___getnewargs__, METH_NOARGS,  \
   pickle___getnewargs__doc},                                        

#define PICKLE_REDUCE_DEF \
{"__reduce__", (PyCFunction)pickle___reduce__, METH_NOARGS,          \
   pickle___reduce__doc},

#define PICKLE_METHODS PICKLE_GETSTATE_DEF PICKLE_SETSTATE_DEF \
                       PICKLE_GETNEWARGS_DEF PICKLE_REDUCE_DEF
