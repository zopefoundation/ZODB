/*############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################*/

/*
 *   This file is also used as a really extensive macro in
 *   ../app/container/_zope_app_container_contained.c.  If you need to
 *   change this file, you need to "svn copy" it to ../app/container/.
 *
 *   This approach is taken to allow the sources for the two packages
 *   to be compilable when the relative locations of these aren't
 *   related in the same way as they are in a checkout.
 *
 *   This will be revisited in the future, but works for now.
 */

#include "Python.h"
#include "modsupport.h"

#define PROXY_MODULE
#include "proxy.h"

static PyTypeObject ProxyType;

#define Proxy_Check(wrapper)   (PyObject_TypeCheck((wrapper), &ProxyType))

static PyObject *
empty_tuple = NULL;


/*
 *   Slot methods.
 */

static PyObject *
wrap_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *result = NULL;
    PyObject *object;

    if (PyArg_UnpackTuple(args, "__new__", 1, 1, &object)) {
        if (kwds != NULL && PyDict_Size(kwds) != 0) {
            PyErr_SetString(PyExc_TypeError,
                            "proxy.__new__ does not accept keyword args");
            return NULL;
        }
        result = PyType_GenericNew(type, args, kwds);
        if (result != NULL) {
            ProxyObject *wrapper = (ProxyObject *) result;
            Py_INCREF(object);
            wrapper->proxy_object = object;
        }
    }
    return result;
}

static int
wrap_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    int result = -1;
    PyObject *object;

    if (PyArg_UnpackTuple(args, "__init__", 1, 1, &object)) {
        ProxyObject *wrapper = (ProxyObject *)self;
        if (kwds != NULL && PyDict_Size(kwds) != 0) {
            PyErr_SetString(PyExc_TypeError,
                            "proxy.__init__ does not accept keyword args");
            return -1;
        }
        /* If the object in this proxy is not the one we
         * received in args, replace it with the new one.
         */
        if (wrapper->proxy_object != object) {
            PyObject *temp = wrapper->proxy_object;
            Py_INCREF(object);
            wrapper->proxy_object = object;
            Py_DECREF(temp);
        }
        result = 0;
    }
    return result;
}

static int
wrap_traverse(PyObject *self, visitproc visit, void *arg)
{
    PyObject *ob = Proxy_GET_OBJECT(self);
    if (ob != NULL)
        return visit(ob, arg);
    else
        return 0;
}

static int
wrap_clear(PyObject *self)
{
    ProxyObject *proxy = (ProxyObject *)self;
    PyObject *temp = proxy->proxy_object;

    if (temp != NULL) {
        proxy->proxy_object = NULL;
        Py_DECREF(temp);
    }
    return 0;
}

static PyObject *
wrap_richcompare(PyObject* self, PyObject* other, int op)
{
    if (Proxy_Check(self)) {
        self = Proxy_GET_OBJECT(self);
    }
    else {
        other = Proxy_GET_OBJECT(other);
    }
    return PyObject_RichCompare(self, other, op);
}

static PyObject *
wrap_iter(PyObject *self)
{
    return PyObject_GetIter(Proxy_GET_OBJECT(self));
}

static PyObject *
wrap_iternext(PyObject *self)
{
    return PyIter_Next(Proxy_GET_OBJECT(self));
}

static void
wrap_dealloc(PyObject *self)
{
    (void) wrap_clear(self);
    self->ob_type->tp_free(self);
}

/* A variant of _PyType_Lookup that doesn't look in ProxyType.
 *
 * If argument search_wrappertype is nonzero, we can look in WrapperType.
 */
PyObject *
WrapperType_Lookup(PyTypeObject *type, PyObject *name)
{
    int i, n;
    PyObject *mro, *res, *base, *dict;

    /* Look in tp_dict of types in MRO */
    mro = type->tp_mro;

    /* If mro is NULL, the type is either not yet initialized
       by PyType_Ready(), or already cleared by type_clear().
       Either way the safest thing to do is to return NULL. */
    if (mro == NULL)
        return NULL;

    assert(PyTuple_Check(mro));

    n = PyTuple_GET_SIZE(mro) 
      - 1; /* We don't want to look at the last item, which is object. */

    for (i = 0; i < n; i++) {
        base = PyTuple_GET_ITEM(mro, i);

        if (((PyTypeObject *)base) != &ProxyType) {
            if (PyClass_Check(base))
                dict = ((PyClassObject *)base)->cl_dict;
            else {
                assert(PyType_Check(base));
                dict = ((PyTypeObject *)base)->tp_dict;
            }
            assert(dict && PyDict_Check(dict));
            res = PyDict_GetItem(dict, name);
            if (res != NULL)
                return res;
        }
    }
    return NULL;
}


static PyObject *
wrap_getattro(PyObject *self, PyObject *name)
{
    PyObject *wrapped;
    PyObject *descriptor;
    PyObject *res = NULL;
    char *name_as_string;
    int maybe_special_name;

#ifdef Py_USING_UNICODE
    /* The Unicode to string conversion is done here because the
       existing tp_getattro slots expect a string object as name
       and we wouldn't want to break those. */
    if (PyUnicode_Check(name)) {
        name = PyUnicode_AsEncodedString(name, NULL, NULL);
        if (name == NULL)
            return NULL;
    }
    else
#endif
    if (!PyString_Check(name)){
        PyErr_SetString(PyExc_TypeError, "attribute name must be string");
        return NULL;
    }
    else
        Py_INCREF(name);

    name_as_string = PyString_AS_STRING(name);
    wrapped = Proxy_GET_OBJECT(self);
    if (wrapped == NULL) {
        PyErr_Format(PyExc_RuntimeError,
            "object is NULL; requested to get attribute '%s'",
            name_as_string);
        goto finally;
    }

    maybe_special_name = name_as_string[0] == '_' && name_as_string[1] == '_';

    if (!(maybe_special_name && strcmp(name_as_string, "__class__") == 0)) {

        descriptor = WrapperType_Lookup(self->ob_type, name);

        if (descriptor != NULL) {
            if (PyType_HasFeature(descriptor->ob_type, Py_TPFLAGS_HAVE_CLASS)
                && descriptor->ob_type->tp_descr_get != NULL) {
                res = descriptor->ob_type->tp_descr_get(
                        descriptor,
                        self,
                        (PyObject *)self->ob_type);
            } else {
                Py_INCREF(descriptor);
                res = descriptor;
            }
            goto finally;
        }
    }
    res = PyObject_GetAttr(wrapped, name);

finally:
    Py_DECREF(name);
    return res;
}

static int
wrap_setattro(PyObject *self, PyObject *name, PyObject *value)
{
    PyObject *wrapped;
    PyObject *descriptor;
    int res = -1;

#ifdef Py_USING_UNICODE
    /* The Unicode to string conversion is done here because the
       existing tp_setattro slots expect a string object as name
       and we wouldn't want to break those. */
    if (PyUnicode_Check(name)) {
        name = PyUnicode_AsEncodedString(name, NULL, NULL);
        if (name == NULL)
            return -1;
    }
    else
#endif
    if (!PyString_Check(name)){
        PyErr_SetString(PyExc_TypeError, "attribute name must be string");
        return -1;
    }
    else
        Py_INCREF(name);

    descriptor = WrapperType_Lookup(self->ob_type, name);
    if (descriptor != NULL) {
        if (PyType_HasFeature(descriptor->ob_type, Py_TPFLAGS_HAVE_CLASS) &&
            descriptor->ob_type->tp_descr_set != NULL) {
            res = descriptor->ob_type->tp_descr_set(descriptor, self, value);
        } else {
            PyErr_Format(PyExc_TypeError,
                "Tried to set attribute '%s' on wrapper, but it is not"
                " a data descriptor", PyString_AS_STRING(name));
        }
        goto finally;
    }

    wrapped = Proxy_GET_OBJECT(self);
    if (wrapped == NULL) {
        PyErr_Format(PyExc_RuntimeError,
            "object is NULL; requested to set attribute '%s'",
            PyString_AS_STRING(name));
        goto finally;
    }
    res = PyObject_SetAttr(wrapped, name, value);

finally:
    Py_DECREF(name);
    return res;
}

static int
wrap_print(PyObject *wrapper, FILE *fp, int flags)
{
    return PyObject_Print(Proxy_GET_OBJECT(wrapper), fp, flags);
}

static PyObject *
wrap_str(PyObject *wrapper) {
    return PyObject_Str(Proxy_GET_OBJECT(wrapper));
}

static PyObject *
wrap_repr(PyObject *wrapper)
{
    return PyObject_Repr(Proxy_GET_OBJECT(wrapper));
}


static int
wrap_compare(PyObject *wrapper, PyObject *v)
{
    return PyObject_Compare(Proxy_GET_OBJECT(wrapper), v);
}

static long
wrap_hash(PyObject *self)
{
    return PyObject_Hash(Proxy_GET_OBJECT(self));
}

static PyObject *
wrap_call(PyObject *self, PyObject *args, PyObject *kw)
{
    if (kw)
        return PyEval_CallObjectWithKeywords(Proxy_GET_OBJECT(self),
					     args, kw);
    else
        return PyObject_CallObject(Proxy_GET_OBJECT(self), args);
}

/*
 *   Number methods
 */

/*
 * Number methods.
 */

static PyObject *
call_int(PyObject *self)
{
    PyNumberMethods *nb = self->ob_type->tp_as_number;
    if (nb == NULL || nb->nb_int == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "object can't be converted to int");
        return NULL;
    }
    return nb->nb_int(self);
}

static PyObject *
call_long(PyObject *self)
{
    PyNumberMethods *nb = self->ob_type->tp_as_number;
    if (nb == NULL || nb->nb_long == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "object can't be converted to long");
        return NULL;
    }
    return nb->nb_long(self);
}

static PyObject *
call_float(PyObject *self)
{
    PyNumberMethods *nb = self->ob_type->tp_as_number;
    if (nb == NULL || nb->nb_float== NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "object can't be converted to float");
        return NULL;
    }
    return nb->nb_float(self);
}

static PyObject *
call_oct(PyObject *self)
{
    PyNumberMethods *nb = self->ob_type->tp_as_number;
    if (nb == NULL || nb->nb_oct== NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "object can't be converted to oct");
        return NULL;
    }
    return nb->nb_oct(self);
}

static PyObject *
call_hex(PyObject *self)
{
    PyNumberMethods *nb = self->ob_type->tp_as_number;
    if (nb == NULL || nb->nb_hex == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "object can't be converted to hex");
        return NULL;
    }
    return nb->nb_hex(self);
}

static PyObject *
call_ipow(PyObject *self, PyObject *other)
{
    /* PyNumber_InPlacePower has three args.  How silly. :-) */
    return PyNumber_InPlacePower(self, other, Py_None);
}

typedef PyObject *(*function1)(PyObject *);

static PyObject *
check1(ProxyObject *self, char *opname, function1 operation)
{
    PyObject *result = NULL;

    result = operation(Proxy_GET_OBJECT(self));
#if 0
    if (result != NULL)
        /* XXX create proxy for result? */
        ;
#endif
    return result;
}

static PyObject *
check2(PyObject *self, PyObject *other,
       char *opname, char *ropname, binaryfunc operation)
{
    PyObject *result = NULL;
    PyObject *object;

    if (Proxy_Check(self)) {
        object = Proxy_GET_OBJECT(self);
        result = operation(object, other);
    }
    else if (Proxy_Check(other)) {
        object = Proxy_GET_OBJECT(other);
        result = operation(self, object);
    }
    else {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }
#if 0
    if (result != NULL)
        /* XXX create proxy for result? */
        ;
#endif
    return result;
}

static PyObject *
check2i(ProxyObject *self, PyObject *other,
	char *opname, binaryfunc operation)
{
	PyObject *result = NULL;
	PyObject *object = Proxy_GET_OBJECT(self);

        result = operation(object, other);
        if (result == object) {
            /* If the operation was really carried out inplace,
               don't create a new proxy, but use the old one. */
            Py_INCREF(self);
            Py_DECREF(object);
            result = (PyObject *)self;
        }
#if 0
        else if (result != NULL)
            /* XXX create proxy for result? */
            ;
#endif
	return result;
}

#define UNOP(NAME, CALL) \
	static PyObject *wrap_##NAME(PyObject *self) \
	{ return check1((ProxyObject *)self, "__"#NAME"__", CALL); }

#define BINOP(NAME, CALL) \
	static PyObject *wrap_##NAME(PyObject *self, PyObject *other) \
	{ return check2(self, other, "__"#NAME"__", "__r"#NAME"__", CALL); }

#define INPLACE(NAME, CALL) \
	static PyObject *wrap_i##NAME(PyObject *self, PyObject *other) \
	{ return check2i((ProxyObject *)self, other, "__i"#NAME"__", CALL); }

BINOP(add, PyNumber_Add)
BINOP(sub, PyNumber_Subtract)
BINOP(mul, PyNumber_Multiply)
BINOP(div, PyNumber_Divide)
BINOP(mod, PyNumber_Remainder)
BINOP(divmod, PyNumber_Divmod)

static PyObject *
wrap_pow(PyObject *self, PyObject *other, PyObject *modulus)
{
    PyObject *result = NULL;
    PyObject *object;

    if (Proxy_Check(self)) {
        object = Proxy_GET_OBJECT(self);
        result = PyNumber_Power(object, other, modulus);
    }
    else if (Proxy_Check(other)) {
        object = Proxy_GET_OBJECT(other);
        result = PyNumber_Power(self, object, modulus);
    }
    else if (modulus != NULL && Proxy_Check(modulus)) {
        object = Proxy_GET_OBJECT(modulus);
        result = PyNumber_Power(self, other, modulus);
    }
    else {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }
    return result;
}

BINOP(lshift, PyNumber_Lshift)
BINOP(rshift, PyNumber_Rshift)
BINOP(and, PyNumber_And)
BINOP(xor, PyNumber_Xor)
BINOP(or, PyNumber_Or)

static int
wrap_coerce(PyObject **p_self, PyObject **p_other)
{
    PyObject *self = *p_self;
    PyObject *other = *p_other;
    PyObject *object;
    PyObject *left;
    PyObject *right;
    int r;

    assert(Proxy_Check(self));
    object = Proxy_GET_OBJECT(self);

    left = object;
    right = other;
    r = PyNumber_CoerceEx(&left, &right);
    if (r != 0)
        return r;
    /* Now left and right have been INCREF'ed.  Any new value that
       comes out is proxied; any unchanged value is left unchanged. */
    if (left == object) {
        /* Keep the old proxy */
        Py_INCREF(self);
        Py_DECREF(left);
        left = self;
    }
#if 0
    else {
        /* XXX create proxy for left? */
    }
    if (right != other) {
        /* XXX create proxy for right? */
    }
#endif
    *p_self = left;
    *p_other = right;
    return 0;
}

UNOP(neg, PyNumber_Negative)
UNOP(pos, PyNumber_Positive)
UNOP(abs, PyNumber_Absolute)
UNOP(invert, PyNumber_Invert)

UNOP(int, call_int)
UNOP(long, call_long)
UNOP(float, call_float)
UNOP(oct, call_oct)
UNOP(hex, call_hex)

INPLACE(add, PyNumber_InPlaceAdd)
INPLACE(sub, PyNumber_InPlaceSubtract)
INPLACE(mul, PyNumber_InPlaceMultiply)
INPLACE(div, PyNumber_InPlaceDivide)
INPLACE(mod, PyNumber_InPlaceRemainder)
INPLACE(pow, call_ipow)
INPLACE(lshift, PyNumber_InPlaceLshift)
INPLACE(rshift, PyNumber_InPlaceRshift)
INPLACE(and, PyNumber_InPlaceAnd)
INPLACE(xor, PyNumber_InPlaceXor)
INPLACE(or, PyNumber_InPlaceOr)

BINOP(floordiv, PyNumber_FloorDivide)
BINOP(truediv, PyNumber_TrueDivide)
INPLACE(floordiv, PyNumber_InPlaceFloorDivide)
INPLACE(truediv, PyNumber_InPlaceTrueDivide)

static int
wrap_nonzero(PyObject *self)
{
    return PyObject_IsTrue(Proxy_GET_OBJECT(self));
}

/*
 *   Sequence methods
 */

static int
wrap_length(PyObject *self)
{
    return PyObject_Length(Proxy_GET_OBJECT(self));
}

static PyObject *
wrap_slice(PyObject *self, int start, int end)
{
    return PySequence_GetSlice(Proxy_GET_OBJECT(self), start, end);
}

static int
wrap_ass_slice(PyObject *self, int i, int j, PyObject *value)
{
    return PySequence_SetSlice(Proxy_GET_OBJECT(self), i, j, value);
}

static int
wrap_contains(PyObject *self, PyObject *value)
{
    return PySequence_Contains(Proxy_GET_OBJECT(self), value);
}

/*
 *   Mapping methods
 */

static PyObject *
wrap_getitem(PyObject *wrapper, PyObject *v) {
    return PyObject_GetItem(Proxy_GET_OBJECT(wrapper), v);
}

static int
wrap_setitem(PyObject *self, PyObject *key, PyObject *value)
{
    if (value == NULL)
	return PyObject_DelItem(Proxy_GET_OBJECT(self), key);
    else
	return PyObject_SetItem(Proxy_GET_OBJECT(self), key, value);
}

/*
 *   Normal methods
 */

static char
reduce__doc__[] =
"__reduce__()\n"
"Raise an exception; this prevents proxies from being picklable by\n"
"default, even if the underlying object is picklable.";

static PyObject *
wrap_reduce(PyObject *self)
{
    PyObject *pickle_error = NULL;
    PyObject *pickle = PyImport_ImportModule("pickle");

    if (pickle == NULL)
        PyErr_Clear();
    else {
        pickle_error = PyObject_GetAttrString(pickle, "PicklingError");
        if (pickle_error == NULL)
            PyErr_Clear();
    }
    if (pickle_error == NULL) {
        pickle_error = PyExc_RuntimeError;
        Py_INCREF(pickle_error);
    }
    PyErr_SetString(pickle_error,
                    "proxy instances cannot be pickled");
    Py_DECREF(pickle_error);
    return NULL;
}

static PyNumberMethods
wrap_as_number = {
    wrap_add,				/* nb_add */
    wrap_sub,				/* nb_subtract */
    wrap_mul,				/* nb_multiply */
    wrap_div,				/* nb_divide */
    wrap_mod,				/* nb_remainder */
    wrap_divmod,			/* nb_divmod */
    wrap_pow,				/* nb_power */
    wrap_neg,				/* nb_negative */
    wrap_pos,				/* nb_positive */
    wrap_abs,				/* nb_absolute */
    wrap_nonzero,			/* nb_nonzero */
    wrap_invert,			/* nb_invert */
    wrap_lshift,			/* nb_lshift */
    wrap_rshift,			/* nb_rshift */
    wrap_and,				/* nb_and */
    wrap_xor,				/* nb_xor */
    wrap_or,				/* nb_or */
    wrap_coerce,			/* nb_coerce */
    wrap_int,				/* nb_int */
    wrap_long,				/* nb_long */
    wrap_float,				/* nb_float */
    wrap_oct,				/* nb_oct */
    wrap_hex,				/* nb_hex */

    /* Added in release 2.0 */
    /* These require the Py_TPFLAGS_HAVE_INPLACEOPS flag */
    wrap_iadd,				/* nb_inplace_add */
    wrap_isub,				/* nb_inplace_subtract */
    wrap_imul,				/* nb_inplace_multiply */
    wrap_idiv,				/* nb_inplace_divide */
    wrap_imod,				/* nb_inplace_remainder */
    (ternaryfunc)wrap_ipow,		/* nb_inplace_power */
    wrap_ilshift,			/* nb_inplace_lshift */
    wrap_irshift,			/* nb_inplace_rshift */
    wrap_iand,				/* nb_inplace_and */
    wrap_ixor,				/* nb_inplace_xor */
    wrap_ior,				/* nb_inplace_or */

    /* Added in release 2.2 */
    /* These require the Py_TPFLAGS_HAVE_CLASS flag */
    wrap_floordiv,			/* nb_floor_divide */
    wrap_truediv,			/* nb_true_divide */
    wrap_ifloordiv,			/* nb_inplace_floor_divide */
    wrap_itruediv,			/* nb_inplace_true_divide */
};

static PySequenceMethods
wrap_as_sequence = {
    wrap_length,			/* sq_length */
    0,					/* sq_concat */
    0,					/* sq_repeat */
    0,					/* sq_item */
    wrap_slice,				/* sq_slice */
    0,					/* sq_ass_item */
    wrap_ass_slice,			/* sq_ass_slice */
    wrap_contains,			/* sq_contains */
};

static PyMappingMethods
wrap_as_mapping = {
    wrap_length,			/* mp_length */
    wrap_getitem,			/* mp_subscript */
    wrap_setitem,			/* mp_ass_subscript */
};

static PyMethodDef
wrap_methods[] = {
    {"__reduce__", (PyCFunction)wrap_reduce, METH_NOARGS, reduce__doc__},
    {NULL, NULL},
};

/*
 * Note that the numeric methods are not supported.  This is primarily
 * because of the way coercion-less operations are performed with
 * new-style numbers; since we can't tell which side of the operation
 * is 'self', we can't ensure we'd unwrap the right thing to perform
 * the actual operation.  We also can't afford to just unwrap both
 * sides the way weakrefs do, since we don't know what semantics will
 * be associated with the wrapper itself.
 */

statichere PyTypeObject
ProxyType = {
    PyObject_HEAD_INIT(NULL)  /* PyObject_HEAD_INIT(&PyType_Type) */  
    0,
    "zope.proxy.ProxyBase",
    sizeof(ProxyObject),
    0,
    wrap_dealloc,			/* tp_dealloc */
    wrap_print,				/* tp_print */
    0,					/* tp_getattr */
    0,					/* tp_setattr */
    wrap_compare,			/* tp_compare */
    wrap_repr,				/* tp_repr */
    &wrap_as_number,			/* tp_as_number */
    &wrap_as_sequence,			/* tp_as_sequence */
    &wrap_as_mapping,			/* tp_as_mapping */
    wrap_hash,				/* tp_hash */
    wrap_call,				/* tp_call */
    wrap_str,				/* tp_str */
    wrap_getattro,			/* tp_getattro */
    wrap_setattro,			/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC
        | Py_TPFLAGS_CHECKTYPES | Py_TPFLAGS_BASETYPE, /* tp_flags */
    0,					/* tp_doc */
    wrap_traverse,			/* tp_traverse */
    wrap_clear,				/* tp_clear */
    wrap_richcompare,			/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    wrap_iter,				/* tp_iter */
    wrap_iternext,			/* tp_iternext */
    wrap_methods,			/* tp_methods */
    0,					/* tp_members */
    0,					/* tp_getset */
    0,					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0,					/* tp_dictoffset */
    wrap_init,				/* tp_init */
    0,                   		/* tp_alloc */
    wrap_new,				/* tp_new */
    0, /*_PyObject_GC_Del,*/		/* tp_free */
};

static PyObject *
create_proxy(PyObject *object)
{
    PyObject *result = NULL;
    PyObject *args;

    args = PyTuple_New(1);
    if (args != NULL) {
        Py_INCREF(object);
        PyTuple_SET_ITEM(args, 0, object);
        result = PyObject_CallObject((PyObject *)&ProxyType, args);
        Py_DECREF(args);
    }
    return result;
}

static int
api_check(PyObject *obj)
{
    return obj ? Proxy_Check(obj) : 0;
}

static PyObject *
api_create(PyObject *object)
{
    if (object == NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot create proxy around NULL");
        return NULL;
    }
    return create_proxy(object);
}

static PyObject *
api_getobject(PyObject *proxy)
{
    if (proxy == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
			"cannot pass NULL to ProxyAPI.getobject()");
        return NULL;
    }
    if (Proxy_Check(proxy))
        return Proxy_GET_OBJECT(proxy);
    else {
        PyErr_Format(PyExc_TypeError, "expected proxy object, got %s",
		     proxy->ob_type->tp_name);
        return NULL;
    }
}

static ProxyInterface
wrapper_capi = {
    &ProxyType,
    api_check,
    api_create,
    api_getobject,
};

static PyObject *api_object = NULL;


static char
getobject__doc__[] =
"getProxiedObject(proxy) --> object\n"
"\n"
"Get the underlying object for proxy, or the object itself, if it is\n"
"not a proxy.";

static PyObject *
wrapper_getobject(PyObject *unused, PyObject *obj)
{
  if (Proxy_Check(obj)) 
    obj = Proxy_GET_OBJECT(obj);
  
  if (obj == NULL)
    obj = Py_None;

  Py_INCREF(obj);
  return obj;
}

static char
isProxy__doc__[] =
"Check whether the given object is a proxy\n"
"\n"
"If proxytype is not None, checkes whether the object is\n"
"proxied by the given proxytype.\n"
;

static PyObject *
wrapper_isProxy(PyObject *unused, PyObject *args)
{
  PyObject *obj, *result;
  PyTypeObject *proxytype=&ProxyType;

  if (! PyArg_ParseTuple(args, "O|O!:isProxy", 
                         &obj, &PyType_Type, &proxytype)
      )
    return NULL;

  while (obj && Proxy_Check(obj))
  {
    if (PyObject_TypeCheck(obj, proxytype))
      {
        result = Py_True;
	Py_INCREF(result);
        return result;
      }
    obj = Proxy_GET_OBJECT(obj);
  }
  result = Py_False;
  Py_INCREF(result);
  return result;
}

static char
removeAllProxies__doc__[] =
"removeAllProxies(proxy) --> object\n"
"\n"
"Get the proxied object with no proxies\n"
"\n"
"If obj is not a proxied object, return obj.\n"
"\n"
"The returned object has no proxies.\n"
;

static PyObject *
wrapper_removeAllProxies(PyObject *unused, PyObject *obj)
{
  while (obj && Proxy_Check(obj)) 
    obj = Proxy_GET_OBJECT(obj);
  
  if (obj == NULL)
    obj = Py_None;

  Py_INCREF(obj);
  return obj;
}

static char
sameProxiedObjects__doc__[] = 
"Check whether two objects are the same or proxies of the same object";

static PyObject *
wrapper_sameProxiedObjects(PyObject *unused, PyObject *args)
{
  PyObject *ob1, *ob2;

  if (! PyArg_ParseTuple(args, "OO:sameProxiedObjects", &ob1, &ob2))
    return NULL;

  while (ob1 && Proxy_Check(ob1)) 
    ob1 = Proxy_GET_OBJECT(ob1);

  while (ob2 && Proxy_Check(ob2)) 
    ob2 = Proxy_GET_OBJECT(ob2);

  if (ob1 == ob2)
    ob1 = Py_True;
  else
    ob1 = Py_False;

  Py_INCREF(ob1);
  return ob1;
}


static char
queryProxy__doc__[] =
"Look for a proxy of the given type around the object\n"
"\n"
"If no such proxy can be found, return the default.\n"
;

static PyObject *
wrapper_queryProxy(PyObject *unused, PyObject *args)
{
  PyObject *obj, *result=Py_None;
  PyTypeObject *proxytype=&ProxyType;

  if (! PyArg_ParseTuple(args, "O|O!O:queryProxy", 
                         &obj, &PyType_Type, &proxytype, &result)
      )
    return NULL;

  while (obj && Proxy_Check(obj))
  {
    if (PyObject_TypeCheck(obj, proxytype))
      {
        Py_INCREF(obj);
        return obj;
      }
    obj = Proxy_GET_OBJECT(obj);
  }

  Py_INCREF(result);
  return result;
}

static char
queryInnerProxy__doc__[] =
"Look for the inner-most proxy of the given type around the object\n"
"\n"
"If no such proxy can be found, return the default.\n"
"\n"
"If there is such a proxy, return the inner-most one.\n"
;

static PyObject *
wrapper_queryInnerProxy(PyObject *unused, PyObject *args)
{
  PyObject *obj, *result=Py_None;
  PyTypeObject *proxytype=&ProxyType;

  if (! PyArg_ParseTuple(args, "O|O!O:queryInnerProxy", 
                         &obj, &PyType_Type, &proxytype, &result)
      )
    return NULL;

  while (obj && Proxy_Check(obj))
  {
    if (PyObject_TypeCheck(obj, proxytype))
      result = obj;
    obj = Proxy_GET_OBJECT(obj);
  }

  Py_INCREF(result);
  return result;
}

static char
module___doc__[] =
"Association between an object, a context object, and a dictionary.\n\
\n\
The context object and dictionary give additional context information\n\
associated with a reference to the basic object.  The wrapper objects\n\
act as proxies for the original object.";


static PyMethodDef
module_functions[] = {
    {"getProxiedObject", wrapper_getobject, METH_O, getobject__doc__},
    {"isProxy", wrapper_isProxy, METH_VARARGS, isProxy__doc__},
    {"sameProxiedObjects", wrapper_sameProxiedObjects, METH_VARARGS, 
     sameProxiedObjects__doc__},
    {"queryProxy", wrapper_queryProxy, METH_VARARGS, queryProxy__doc__},
    {"queryInnerProxy", wrapper_queryInnerProxy, METH_VARARGS, 
     queryInnerProxy__doc__},
    {"removeAllProxies", wrapper_removeAllProxies, METH_O, 
     removeAllProxies__doc__},
    {NULL}
};

void
init_zope_proxy_proxy(void)
{
    PyObject *m = Py_InitModule3("_zope_proxy_proxy", 
                                 module_functions, module___doc__);

    if (m == NULL)
        return;

    if (empty_tuple == NULL)
        empty_tuple = PyTuple_New(0);

    ProxyType.tp_free = _PyObject_GC_Del;

    if (PyType_Ready(&ProxyType) < 0)
        return;

    Py_INCREF(&ProxyType);
    PyModule_AddObject(m, "ProxyBase", (PyObject *)&ProxyType);

    if (api_object == NULL) {
        api_object = PyCObject_FromVoidPtr(&wrapper_capi, NULL);
        if (api_object == NULL)
            return;
    }
    Py_INCREF(api_object);
    PyModule_AddObject(m, "_CAPI", api_object);
}
