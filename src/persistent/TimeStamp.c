/*****************************************************************************

  Copyright (c) 2001, 2004 Zope Corporation and Contributors. 
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#include "Python.h"
#include <time.h>

PyObject *TimeStamp_FromDate(int, int, int, int, int, double);
PyObject *TimeStamp_FromString(const char *);

static char TimeStampModule_doc[] =
"A 64-bit TimeStamp used as a ZODB serial number.\n"
"\n"
"$Id: TimeStamp.c,v 1.5 2004/05/03 20:17:57 spascoe Exp $\n";


typedef struct {
    PyObject_HEAD
    unsigned char data[8];
} TimeStamp;

/* The first dimension of the arrays below is non-leapyear / leapyear */

static char month_len[2][12]={
  {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
  {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

static short joff[2][12] = {
  {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
  {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}
};

static double gmoff=0;

/* XXX should this be stored in sconv? */
#define SCONV ((double)60) / ((double)(1<<16)) / ((double)(1<<16))

static int
leap(int year)
{
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

static int
days_in_month(int year, int month)
{
    return month_len[leap(year)][month];
}

static double
TimeStamp_yad(int y)
{
    double d, s;

    y -= 1900;

    d = (y - 1) * 365;
    if (y > 0) {
        s = 1.0;
	y -= 1;
    } else {
	s = -1.0;
	y = -y;
    }
    return d + s * (y / 4 - y / 100 + (y + 300) / 400);
}

static double
TimeStamp_abst(int y, int mo, int d, int m, int s)
{
    return (TimeStamp_yad(y) + joff[leap(y)][mo] + d) * 86400 + m * 60 + s;
}

static int
TimeStamp_init_gmoff(void)
{
    struct tm *t;
    time_t z=0;

    t = gmtime(&z);
    if (t == NULL) {
	PyErr_SetString(PyExc_SystemError, "gmtime failed");
	return -1;
    }

    gmoff = TimeStamp_abst(t->tm_year+1900, t->tm_mon, t->tm_mday - 1,
			   t->tm_hour * 60 + t->tm_min, t->tm_sec);

    return 0;
}

static void
TimeStamp_dealloc(TimeStamp *ts)
{
    PyObject_Del(ts);
}

static int
TimeStamp_compare(TimeStamp *v, TimeStamp *w)
{
    int cmp = memcmp(v->data, w->data, 8);
    if (cmp < 0) return -1;
    if (cmp > 0) return 1;
    return 0;
}

static long
TimeStamp_hash(TimeStamp *self)
{
    register unsigned char *p = (unsigned char *)self->data;
    register int len = 8;
    register long x = *p << 7;
    /* XXX unroll loop? */
    while (--len >= 0)
	x = (1000003*x) ^ *p++;
    x ^= 8;
    if (x == -1)
	x = -2;
    return x;
}

typedef struct {
    /* XXX reverse-engineer what's in these things and comment them */
    int y;
    int m;
    int d;
    int mi;
} TimeStampParts;

static void
TimeStamp_unpack(TimeStamp *self, TimeStampParts *p)
{
    unsigned long v;

    v = (self->data[0] * 16777216 + self->data[1] * 65536
	 + self->data[2] * 256 + self->data[3]);
    p->y = v / 535680 + 1900;
    p->m = (v % 535680) / 44640 + 1;
    p->d = (v % 44640) / 1440 + 1;
    p->mi = v % 1440;
}

static double
TimeStamp_sec(TimeStamp *self)
{
    unsigned int v;

    v = (self->data[4] * 16777216 + self->data[5] * 65536
	 + self->data[6] * 256 + self->data[7]);
    return SCONV * v;
}

static PyObject *
TimeStamp_year(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyInt_FromLong(p.y);
}

static PyObject *
TimeStamp_month(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyInt_FromLong(p.m);
}

static PyObject *
TimeStamp_day(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyInt_FromLong(p.d);
}

static PyObject *
TimeStamp_hour(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyInt_FromLong(p.mi / 60);
}

static PyObject *
TimeStamp_minute(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyInt_FromLong(p.mi % 60);
}

static PyObject *
TimeStamp_second(TimeStamp *self)
{
    return PyFloat_FromDouble(TimeStamp_sec(self));
}

static PyObject *
TimeStamp_timeTime(TimeStamp *self)
{
    TimeStampParts p;
    TimeStamp_unpack(self, &p);
    return PyFloat_FromDouble(TimeStamp_abst(p.y, p.m - 1, p.d - 1, p.mi, 0)
			      + TimeStamp_sec(self) - gmoff);
}

static PyObject *
TimeStamp_raw(TimeStamp *self)
{
    return PyString_FromStringAndSize(self->data, 8);
}

static PyObject *
TimeStamp_str(TimeStamp *self)
{
    char buf[128];
    TimeStampParts p;
    int len;

    TimeStamp_unpack(self, &p);
    len =sprintf(buf, "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%09.6f",
	         p.y, p.m, p.d, p.mi / 60, p.mi % 60,
	         TimeStamp_sec(self));

    return PyString_FromStringAndSize(buf, len);
}


static PyObject *
TimeStamp_laterThan(TimeStamp *self, PyObject *obj)
{
    TimeStamp *o = NULL;
    TimeStampParts p;
    unsigned char new[8];
    int i;

    if (obj->ob_type != self->ob_type) {
	PyErr_SetString(PyExc_TypeError, "expected TimeStamp object");
	return NULL;
    }
    o = (TimeStamp *)obj;
    if (memcmp(self->data, o->data, 8) > 0) {
	Py_INCREF(self);
	return (PyObject *)self;
    }

    memcpy(new, o->data, 8);
    for (i = 7; i > 3; i--) {
	if (new[i] == 255)
	    new[i] = 0;
	else {
	    new[i]++;
	    return TimeStamp_FromString(new);
	}
    }

    /* All but the first two bytes are the same.  Need to increment
       the year, month, and day explicitly. */
    TimeStamp_unpack(o, &p);
    if (p.mi >= 1439) {
	p.mi = 0;
	if (p.d == month_len[leap(p.y)][p.m - 1]) {
	    p.d = 1;
	    if (p.m == 12) {
		p.m = 1;
		p.y++;
	    } else
		p.m++;
	} else
	    p.d++;
    } else
	p.mi++;

    return TimeStamp_FromDate(p.y, p.m, p.d, p.mi / 60, p.mi % 60, 0);
}

static struct PyMethodDef TimeStamp_methods[] = {
    {"year", 	(PyCFunction)TimeStamp_year, 	METH_NOARGS},
    {"minute", 	(PyCFunction)TimeStamp_minute, 	METH_NOARGS},
    {"month", 	(PyCFunction)TimeStamp_month, 	METH_NOARGS},
    {"day", 	(PyCFunction)TimeStamp_day,	METH_NOARGS},
    {"hour", 	(PyCFunction)TimeStamp_hour, 	METH_NOARGS},
    {"second", 	(PyCFunction)TimeStamp_second, 	METH_NOARGS},
    {"timeTime",(PyCFunction)TimeStamp_timeTime, 	METH_NOARGS},
    {"laterThan", (PyCFunction)TimeStamp_laterThan, 	METH_O},
    {"raw",	(PyCFunction)TimeStamp_raw,	METH_NOARGS},
    {NULL,	NULL},
};

static PyTypeObject TimeStamp_type = {
    PyObject_HEAD_INIT(NULL)
    0,
    "persistent.TimeStamp",
    sizeof(TimeStamp),
    0,
    (destructor)TimeStamp_dealloc,	/* tp_dealloc */
    0,					/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    (cmpfunc)TimeStamp_compare,		/* tp_compare */
    (reprfunc)TimeStamp_raw,		/* tp_repr */
    0,					/* tp_as_number */
    0,					/* tp_as_sequence */
    0,					/* tp_as_mapping */
    (hashfunc)TimeStamp_hash,		/* tp_hash */
    0,					/* tp_call */
    (reprfunc)TimeStamp_str,		/* tp_str */
    0,					/* tp_getattro */
    0,					/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    0,					/* tp_doc */
    0,					/* tp_traverse */
    0,					/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    0,					/* tp_iter */
    0,					/* tp_iternext */
    TimeStamp_methods,			/* tp_methods */
    0,					/* tp_members */
    0,					/* tp_getset */
    0,					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
};

PyObject *
TimeStamp_FromString(const char *buf)
{
    /* buf must be exactly 8 characters */
    TimeStamp *ts = (TimeStamp *)PyObject_New(TimeStamp, &TimeStamp_type);
    memcpy(ts->data, buf, 8);
    return (PyObject *)ts;
}

#define CHECK_RANGE(VAR, LO, HI) if ((VAR) < (LO) || (VAR) > (HI)) { \
     return PyErr_Format(PyExc_ValueError, \
			 # VAR " must be between %d and %d: %d", \
			 (LO), (HI), (VAR)); \
    }

PyObject *
TimeStamp_FromDate(int year, int month, int day, int hour, int min,
		   double sec)
{
    TimeStamp *ts = NULL;
    int d;
    unsigned int v;

    if (year < 1900)
	return PyErr_Format(PyExc_ValueError,
			    "year must be greater than 1900: %d", year);
    CHECK_RANGE(month, 1, 12);
    d = days_in_month(year, month - 1);
    if (day < 1 || day > d)
	return PyErr_Format(PyExc_ValueError,
			    "day must be between 1 and %d: %d", d, day);
    CHECK_RANGE(hour, 0, 23);
    CHECK_RANGE(min, 0, 59);
    /* Seconds are allowed to be anything, so chill
       If we did want to be pickly, 60 would be a better choice.
    if (sec < 0 || sec > 59)
	return PyErr_Format(PyExc_ValueError,
			    "second must be between 0 and 59: %f", sec);
    */
    ts = (TimeStamp *)PyObject_New(TimeStamp, &TimeStamp_type);
    v = (((year - 1900) * 12 + month - 1) * 31 + day - 1);
    v = (v * 24 + hour) * 60 + min;
    ts->data[0] = v / 16777216;
    ts->data[1] = (v % 16777216) / 65536;
    ts->data[2] = (v % 65536) / 256;
    ts->data[3] = v % 256;
    sec /= SCONV;
    v = (unsigned int)sec;
    ts->data[4] = v / 16777216;
    ts->data[5] = (v % 16777216) / 65536;
    ts->data[6] = (v % 65536) / 256;
    ts->data[7] = v % 256;

    return (PyObject *)ts;
}

PyObject *
TimeStamp_TimeStamp(PyObject *obj, PyObject *args)
{
    char *buf = NULL;
    int len = 0, y, mo, d, h = 0, m = 0;
    double sec = 0;

    if (PyArg_ParseTuple(args, "s#:TimeStamp", &buf, &len)) {
	if (len != 8) {
	    PyErr_SetString(PyExc_ValueError, "8-character string expected");
	    return NULL;
	}
	return TimeStamp_FromString(buf);
    }
    PyErr_Clear();

    if (!PyArg_ParseTuple(args, "iii|iid", &y, &mo, &d, &h, &m, &sec))
	return NULL;
    return TimeStamp_FromDate(y, mo, d, h, m, sec);
}

static PyMethodDef TimeStampModule_functions[] = {
    {"TimeStamp",	TimeStamp_TimeStamp,	METH_VARARGS},
    {NULL,		NULL},
};


void
initTimeStamp(void)
{
    PyObject *m;

    if (TimeStamp_init_gmoff() < 0)
	return;

    m = Py_InitModule4("TimeStamp", TimeStampModule_functions,
		       TimeStampModule_doc, NULL, PYTHON_API_VERSION);
    if (m == NULL)
	return;

    TimeStamp_type.ob_type = &PyType_Type;
    TimeStamp_type.tp_getattro = PyObject_GenericGetAttr;
}
