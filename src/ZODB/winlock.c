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
static char winlock_doc_string[] =
"Lock files on Windows."
"\n"
"$Id: winlock.c,v 1.9 2003/02/28 20:37:59 tim_one Exp $\n";

#include "Python.h"

static PyObject *Error;

#ifdef MS_WIN32

#include <windows.h>
#include <io.h>

/* LOCK_FUNC is the shared type of Win32 LockFile and UnlockFile. */
typedef WINBASEAPI BOOL WINAPI LOCK_FUNC(HANDLE, DWORD, DWORD, DWORD, DWORD);

static PyObject *
common(LOCK_FUNC func, PyObject *args)
{
	int fileno;
	long h, ofslo, ofshi, lenlo, lenhi;

	if (! PyArg_ParseTuple(args, "illll", &fileno,
			       &ofslo, &ofshi,
			       &lenlo, &lenhi))
		return NULL;

	h = _get_osfhandle(fileno);
	if (h == -1) {
		PyErr_SetString(Error, "_get_osfhandle failed");
		return NULL;
	}
	if (func((HANDLE)h, ofslo, ofshi, lenlo, lenhi)) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	PyErr_SetObject(Error, PyInt_FromLong(GetLastError()));
	return NULL;
}

static PyObject *
winlock(PyObject *ignored, PyObject *args)
{
	return common(LockFile, args);
}

static PyObject *
winunlock(PyObject *ignored, PyObject *args)
{
	return common(UnlockFile, args);
}

static struct PyMethodDef methods[] = {
    {"LockFile",	(PyCFunction)winlock,	METH_VARARGS,
     "LockFile(fileno, offsetLow, offsetHigh, lengthLow, lengthHigh) -- "
     "Lock the file associated with fileno"},

    {"UnlockFile",	(PyCFunction)winunlock,	METH_VARARGS,
     "UnlockFile(fileno, offsetLow, offsetHigh, lengthLow, lengthHigh) -- "
     "Unlock the file associated with fileno"},

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
initwinlock(void)
{
	PyObject *m, *d;

	if (!(Error=PyString_FromString("winlock.error")))
		return;

	/* Create the module and add the functions */
	m = Py_InitModule4("winlock", methods, winlock_doc_string,
			   (PyObject*)NULL, PYTHON_API_VERSION);

	d = PyModule_GetDict(m);
	PyDict_SetItemString(d, "error", Error);
}
