/*****************************************************************************

  Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
  
  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE
  
 ****************************************************************************/
static char winlock_doc_string[] = 
"Lock files on Windows."
"\n"
"$Id: winlock.c,v 1.6 2001/11/28 15:51:20 matt Exp $\n";

#include "Python.h"

static PyObject *Error;

#ifdef MS_WIN32

#include <windows.h>
#include <io.h>

static PyObject *	
winlock(PyObject *ignored, PyObject *args)
{
  int fileno;
  long h, ol, oh, ll, lh;
  
  if (! PyArg_ParseTuple(args, "illll", &fileno, &ol, &oh, &ll, &lh))
    return NULL;

  if ((h=_get_osfhandle(fileno))==-1) {
    PyErr_SetString(Error, "_get_osfhandle failed");
    return NULL;
  }
  if (LockFile((HANDLE)h, ol, oh, ll, lh)) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  PyErr_SetObject(Error, PyInt_FromLong(GetLastError()));
  return NULL;
}

static struct PyMethodDef methods[] = {
  {"LockFile",	(PyCFunction)winlock,	1,
   "LockFile(fileno, offsetLow, offsetHigh, lengthLow, lengthHigh ) -- "
   "Lock the file associated with fileno"},
  {NULL,		NULL}		/* sentinel */
};
#else

static struct PyMethodDef methods[] = {
  {NULL,		NULL}		/* sentinel */
};

#endif

/* Initialization function for the module (*must* be called initcStringIO) */

#ifndef DL_EXPORT	/* declarations for DLL import/export */
#define DL_EXPORT(RTYPE) RTYPE
#endif
DL_EXPORT(void)
initwinlock(void) {
  PyObject *m, *d;
  char *rev="$Revision: 1.6 $";

  if (!(Error=PyString_FromString("winlock.error"))) 
      return;

  /* Create the module and add the functions */
  m = Py_InitModule4("winlock", methods, winlock_doc_string, (PyObject*)NULL,
		     PYTHON_API_VERSION);

  d = PyModule_GetDict(m);
  PyDict_SetItemString(d, "error", Error);
  /* XXX below could blow up in PyDict_SetItem() */
  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
}


