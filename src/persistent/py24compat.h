/* Backport type definitions from Python 2.5's object.h */
#ifndef PERSISTENT_PY24COMPATH_H
#define PERSISTENT_PY24COMPAT_H
#if PY_VERSION_HEX < 0x02050000
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif /* PY_VERSION_HEX */
#endif /* PERSISTENT_PY24COMPAT_H */
