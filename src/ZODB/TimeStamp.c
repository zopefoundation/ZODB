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

static char TimeStamp_module_documentation[] = 
"Defines 64-bit TimeStamp objects used as ZODB serial numbers.\n"
"\n"
"\n$Id: TimeStamp.c,v 1.9 2001/11/26 20:33:24 andreasjung Exp $\n";

#include <stdlib.h>
#include <time.h>
#ifdef USE_EXTENSION_CLASS
#include "ExtensionClass.h"
#else
#include "Python.h"
#endif


/* ----------------------------------------------------- */

#define UNLESS(E) if(!(E))
#define OBJECT(O) ((PyObject*)(O))

/* Declarations for objects of type TimeStamp */

typedef struct {
  PyObject_HEAD
  unsigned char data[8];
} TimeStamp;

static double
TimeStamp_yad(int y)
{
  double d, s;

  y -= 1900;
  
  d=(y-1)*365;
  if (y > 0) 
    {
        s=1.0;
        y=y-1;
    }
  else
    {
      s=-1.0;
      y = -y;
    }
  return d+s*(y/4-y/100+(y+300)/400);
}

static char month_len[2][12]={
  {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}, 
  {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

static short joff[2][12] = {
  {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
  {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}
};

static double
TimeStamp_abst(int y, int mo, int d, int m, int s)
{
  int l;
  
  l = y%4==0 && (y%100 != 0 || y%400==0);
  return (TimeStamp_yad(y)+ joff[l][mo]+d)*86400 + m*60 + s;
}

static double gmoff=0, sconv=0;

static int
TimeStamp_init_gmoff(void)
{
  struct tm *t;
  time_t z=0;
  
  t=gmtime(&z);
  if (! t)
    {
      PyErr_SetString(PyExc_SystemError, "gmtime failed");
      return -1;
    }
  gmoff=TimeStamp_abst(
	     t->tm_year+1900, t->tm_mon, t->tm_mday-1, 
	     t->tm_hour*60+t->tm_min, t->tm_sec);

  sconv=((double)60)/((double)(1<<16))/((double)(1<<16));
  return 0;
}

static PyObject *
TimeStamp___init__(TimeStamp *self, PyObject *args)
{
  int y, mo, d, h=0, m=0;
  double sec=0;
  char *s;
  unsigned int v;

  if (PyArg_ParseTuple(args, "s#", &s, &m))
    {
      if (m != 8)
	{
	  PyErr_SetString(PyExc_ValueError, "8-character string expected");
	  return NULL;
	}
      memcpy(self->data, s, 8);
    }
  else
    {
      PyErr_Clear();
      if (PyArg_ParseTuple(args, "iii|iid", &y, &mo, &d, &h, &m, &sec))
	{
	  s=self->data;
	  v=((((y-1900)*12+mo-1)*31+d-1)*24+h)*60+m;
	  s[0]=v/16777216;
	  s[1]=(v%16777216)/65536;
	  s[2]=(v%65536)/256;
	  s[3]=v%256;
	  sec /= sconv;
	  v=(unsigned int)sec;
	  s[4]=v/16777216;
	  s[5]=(v%16777216)/65536;
	  s[6]=(v%65536)/256;
	  s[7]=v%256;
	}
      else
	{
	  return NULL;
	}
    }
  Py_INCREF(Py_None);
  return Py_None;
}

static int TimeStamp_y, TimeStamp_m, TimeStamp_d, TimeStamp_mi;

static void 
TimeStamp_parts(TimeStamp *self)
{
  unsigned long v;

  v=self->data[0]*16777216+self->data[1]*65536+self->data[2]*256+self->data[3];
  TimeStamp_y=v/535680+1900;
  TimeStamp_m=(v%535680)/44640+1;
  TimeStamp_d=(v%44640)/1440+1;
  TimeStamp_mi=v%1440;
}

static double
TimeStamp_sec(TimeStamp *self)
{
  unsigned int v;

  v=self->data[4]*16777216+self->data[5]*65536+self->data[6]*256+self->data[7];
  return sconv*v;
}

static PyObject *
TimeStamp_year(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);
  return PyInt_FromLong(TimeStamp_y);
}

static PyObject *
TimeStamp_month(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);
  return PyInt_FromLong(TimeStamp_m);
}

static PyObject *
TimeStamp_day(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);
  return PyInt_FromLong(TimeStamp_d);
}

static PyObject *
TimeStamp_hour(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);
  return PyInt_FromLong(TimeStamp_mi/60);
}

static PyObject *
TimeStamp_minute(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);
  return PyInt_FromLong(TimeStamp_mi%60);
}

static PyObject *
TimeStamp_second(TimeStamp *self, PyObject *args)
{
  return PyFloat_FromDouble(TimeStamp_sec(self));
}

static PyObject *
TimeStamp_timeTime(TimeStamp *self, PyObject *args)
{
  TimeStamp_parts(self);

  return PyFloat_FromDouble(
            TimeStamp_abst(TimeStamp_y, TimeStamp_m-1, TimeStamp_d-1, 
			   TimeStamp_mi, 0)+
	    TimeStamp_sec(self)-gmoff
	    );
}

static PyObject *
TimeStamp_laterThan(TimeStamp *self, PyObject *args)
{
  TimeStamp *o=NULL;
  unsigned char *s;
  PyObject *a;
  int i;
  
  UNLESS(PyArg_ParseTuple(args, "O!", self->ob_type, &o)) return NULL;

  if (memcmp(self->data, o->data, 8) > 0)
    {
      Py_INCREF(self);
      return OBJECT(self);
    }

  self=o;

  UNLESS(a=PyString_FromStringAndSize(self->data, 8)) return NULL;
  s=(unsigned char *)PyString_AsString(a);
  
  for (i=7; i > 3; i--) 
    {
      if (s[i] == 255) 
	s[i]=0;
      else
	{
	  s[i]++;
	  return PyObject_CallFunction(OBJECT(self->ob_type), "O", a);
	}
    }

  TimeStamp_parts(self);
  if (TimeStamp_mi >= 1439) 
    {
      TimeStamp_mi=0;
      i = TimeStamp_y%4==0 && (TimeStamp_y%100 != 0 || TimeStamp_y%400==0);
      if (TimeStamp_d == month_len[i][TimeStamp_m-1]) 
	{
	  TimeStamp_d=1;
	  if (TimeStamp_m == 12)
	    {
	      TimeStamp_m=1;
	      TimeStamp_y++;
	    }
	  else
	    TimeStamp_m++;
	}
      else
	TimeStamp_d++;
    }
  else
    TimeStamp_mi++;
  
  return PyObject_CallFunction(OBJECT(self->ob_type), "iiiii", 
			       TimeStamp_y, TimeStamp_m, TimeStamp_d,
			       TimeStamp_mi/60, TimeStamp_mi%60);
}

static struct PyMethodDef TimeStamp_methods[] = {
  {"year", (PyCFunction)TimeStamp_year, METH_VARARGS, ""},
  {"minute", (PyCFunction)TimeStamp_minute, METH_VARARGS, ""},
  {"month", (PyCFunction)TimeStamp_month, METH_VARARGS, ""},
  {"day", (PyCFunction)TimeStamp_day, METH_VARARGS, ""},
  {"hour", (PyCFunction)TimeStamp_hour, METH_VARARGS, ""},
  {"second", (PyCFunction)TimeStamp_second, METH_VARARGS, ""},
  {"seconds", (PyCFunction)TimeStamp_second, METH_VARARGS, ""},
  {"timeTime", (PyCFunction)TimeStamp_timeTime, METH_VARARGS, ""},
  {"laterThan", (PyCFunction)TimeStamp_laterThan, METH_VARARGS, ""},
#ifdef USE_EXTENSION_CLASS
  {"__init__", (PyCFunction)TimeStamp___init__, METH_VARARGS, 
   ""},
#endif  
  {NULL,		NULL}		/* sentinel */
};

#ifndef USE_EXTENSION_CLASS
static TimeStampobject *
newTimeStamp(PyObject *ignored, PyObject *args)
{
  TimeStamp *self;
	
  UNLESS(self = PyObject_NEW(TimeStamp, &TimeStampType)) return NULL;
  
  ignored=__init__(self, args);
  if (! ignored) return NULL;
    
  Py_DECREF(ignored);
  return self;
}
#endif

static void
TimeStamp_dealloc(TimeStamp *self)
{
#ifdef USE_EXTENSION_CLASS
  Py_DECREF(self->ob_type);
#endif  
  PyMem_DEL(self);
}

static PyObject *
TimeStamp_repr(TimeStamp *self)
{
  return PyString_FromStringAndSize(self->data, 8);
}

static PyObject *
TimeStamp_str(TimeStamp *self)
{
  char buf[128];
  int l;

  TimeStamp_parts(self);
  l=sprintf(buf, "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%f",
	    TimeStamp_y, TimeStamp_m, TimeStamp_d, 
	    TimeStamp_mi/60, TimeStamp_mi%60, TimeStamp_sec(self));

  return PyString_FromStringAndSize(buf, l);
}

static int
TimeStamp_compare(TimeStamp *v, TimeStamp *w)
{
  return memcmp(v->data, w->data, 8);
}

static long
TimeStamp_hash(TimeStamp *self)
{
  return self->data[0]+self->data[1]+self->data[2]+self->data[3]
        +self->data[4]+self->data[5]+self->data[6]+self->data[7];
}

static PyObject *
TimeStamp_getattro(TimeStamp *self, PyObject *name)
{
#ifndef USE_EXTENSION_CLASS
  char *s;

  if (! (s=PyString_AsString(name))) return NULL;
  return Py_FindMethod(TimeStamp_methods, self, s); 
#else
  return Py_FindAttr(OBJECT(self), name);
#endif
}

#ifdef USE_EXTENSION_CLASS
static PyExtensionClass
#else
static PyTypeObject 
#endif
TimeStampType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "TimeStamp",			/*tp_name*/
  sizeof(TimeStamp),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /* methods */
  (destructor)TimeStamp_dealloc,	/*tp_dealloc*/
  (printfunc)0,	/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)TimeStamp_compare,	/*tp_compare*/
  (reprfunc)TimeStamp_repr,		/*tp_repr*/
  0,		/*tp_as_number*/
  0,		/*tp_as_sequence*/
  0,		/*tp_as_mapping*/
  (hashfunc)TimeStamp_hash,		/*tp_hash*/
  (ternaryfunc)0,	/*tp_call*/
  (reprfunc)TimeStamp_str,		/*tp_str*/
  (getattrofunc)TimeStamp_getattro,
  (setattrofunc)0,
  
  /* Space for future expansion */
  0L,0L,
  "Simple time stamps"
#ifdef USE_EXTENSION_CLASS
  , METHOD_CHAIN(TimeStamp_methods),
#endif
};

static struct PyMethodDef Module_Level__methods[] = {
#ifndef USE_EXTENSION_CLASS
  {"TimeStamp", (PyCFunction)newTimeStamp, METH_VARARGS, ""},
#endif
  {NULL, (PyCFunction)NULL, 0, NULL}		/* sentinel */
};

void
initTimeStamp(void)
{
  PyObject *m, *d, *s;
  char *rev="$Revision: 1.9 $";

  if (TimeStamp_init_gmoff() < 0) return;
  if (! ExtensionClassImported) return;

  /* Create the module and add the functions */
  m = Py_InitModule4("TimeStamp", Module_Level__methods,
		     TimeStamp_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);

#ifndef USE_EXTENSION_CLASS
  TimeStampType.ob_type=&PyType_Type;
#else
  PyExtensionClass_Export(d, "TimeStamp", TimeStampType);
#endif

  PyDict_SetItemString(d,"TimeStampType", OBJECT(&TimeStampType));

  s = PyString_FromString("TimeStamp.error");
  PyDict_SetItemString(d, "error", s);
  Py_XDECREF(s);

  s = PyString_FromStringAndSize(rev + 11, strlen(rev + 11) - 2);
  PyDict_SetItemString(d, "__version__", s);
  Py_XDECREF(s);

  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module TimeStamp");
}
