/* Backport type definitions from Python 2.5's object.h */
#ifndef BTREE_PY24COMPATH_H
#define BTREE_PY24COMPAT_H
#if PY_VERSION_HEX < 0x02050000
typedef Py_ssize_t (*lenfunc)(PyObject *);
typedef PyObject *(*ssizeargfunc)(PyObject *, Py_ssize_t);
typedef PyObject *(*ssizessizeargfunc)(PyObject *, Py_ssize_t, Py_ssize_t);
typedef int(*ssizeobjargproc)(PyObject *, Py_ssize_t, PyObject *);
typedef int(*ssizessizeobjargproc)(PyObject *, Py_ssize_t, Py_ssize_t, PyObject *);
#endif /* PY_VERSION_HEX */
#endif /* BTREE_PY24COMPAT_H */
