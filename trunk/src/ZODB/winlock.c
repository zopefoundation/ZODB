/*****************************************************************************
 
  Zope Public License (ZPL) Version 0.9.4
  ---------------------------------------
  
  Copyright (c) Digital Creations.  All rights reserved.
  
  Redistribution and use in source and binary forms, with or
  without modification, are permitted provided that the following
  conditions are met:
  
  1. Redistributions in source code must retain the above
     copyright notice, this list of conditions, and the following
     disclaimer.
  
  2. Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions, and the following
     disclaimer in the documentation and/or other materials
     provided with the distribution.
  
  3. Any use, including use of the Zope software to operate a
     website, must either comply with the terms described below
     under "Attribution" or alternatively secure a separate
     license from Digital Creations.
  
  4. All advertising materials, documentation, or technical papers
     mentioning features derived from or use of this software must
     display the following acknowledgement:
  
       "This product includes software developed by Digital
       Creations for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
  5. Names associated with Zope or Digital Creations must not be
     used to endorse or promote products derived from this
     software without prior written permission from Digital
     Creations.
  
  6. Redistributions of any form whatsoever must retain the
     following acknowledgment:
  
       "This product includes software developed by Digital
       Creations for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
  7. Modifications are encouraged but must be packaged separately
     as patches to official Zope releases.  Distributions that do
     not clearly separate the patches from the original work must
     be clearly labeled as unofficial distributions.
  
  Disclaimer
  
    THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND
    ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
    FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT
    SHALL DIGITAL CREATIONS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
    IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
    THE POSSIBILITY OF SUCH DAMAGE.
  
  Attribution
  
    Individuals or organizations using this software as a web site
    must provide attribution by placing the accompanying "button"
    and a link to the accompanying "credits page" on the website's
    main entry point.  In cases where this placement of
    attribution is not feasible, a separate arrangment must be
    concluded with Digital Creations.  Those using the software
    for purposes other than web sites must provide a corresponding
    attribution in locations that include a copyright using a
    manner best suited to the application environment.
  
  This software consists of contributions made by Digital
  Creations and many individuals on behalf of Digital Creations.
  Specific attributions are listed in the accompanying credits
  file.
  
 ****************************************************************************/
static char *what_string = "$Id: winlock.c,v 1.1 1999/02/05 00:35:47 jim Exp $";

#include <windows.h>
#include <io.h>
#include "Python.h"

static PyObject *Error;

#ifdef MS_WIN32
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
initwinlock() {
  PyObject *m, *d;
  char *rev="$Revision: 1.1 $";

  if (! (Error=PyString_FromString("winlock.error"))) return;

  /* Create the module and add the functions */
  m = Py_InitModule4("winlock", methods,
		     "lock files on windows",
		     (PyObject*)NULL,PYTHON_API_VERSION);

  d = PyModule_GetDict(m);
  PyDict_SetItemString(d, "error", Error);
  PyDict_SetItemString(d,"__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  if (PyErr_Occurred()) Py_FatalError("can't initialize module winlock");
}


