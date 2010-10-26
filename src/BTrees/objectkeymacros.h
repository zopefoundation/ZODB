#define KEYMACROS_H "$Id$\n"
#define KEY_TYPE PyObject *
#define KEY_TYPE_IS_PYOBJECT

#include "Python.h"

static PyObject *object_;

static int
check_argument_cmp(PyObject *arg)
{
  if (arg->ob_type->tp_richcompare == NULL
      &&
      arg->ob_type->tp_compare ==
      ((PyTypeObject *)object_)->ob_type->tp_compare
      )
    {
      PyErr_SetString(PyExc_TypeError, "Object has default comparison");
      return 0;
    }
  return 1;
}

#define TEST_KEY_SET_OR(V, KEY, TARGET) if ( ( (V) = PyObject_Compare((KEY),(TARGET)) ), PyErr_Occurred() )
#define INCREF_KEY(k) Py_INCREF(k)
#define DECREF_KEY(KEY) Py_DECREF(KEY)
#define COPY_KEY(KEY, E) KEY=(E)
#define COPY_KEY_TO_OBJECT(O, K) O=(K); Py_INCREF(O)
#define COPY_KEY_FROM_ARG(TARGET, ARG, S) \
    TARGET=(ARG); \
    (S) = check_argument_cmp(ARG); 
