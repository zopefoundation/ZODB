#define KEYMACROS_H "$Id$\n"
#define KEY_TYPE PyObject *
#define KEY_TYPE_IS_PYOBJECT

#include "Python.h"

static PyObject *object_;

static int
check_argument_cmp(PyObject *arg)
{
  /* printf("check cmp %p %p %p %p\n",  */
  /*        arg->ob_type->tp_richcompare, */
  /*        ((PyTypeObject *)object_)->ob_type->tp_richcompare, */
  /*        arg->ob_type->tp_compare, */
  /*        ((PyTypeObject *)object_)->ob_type->tp_compare); */

  if (arg->ob_type->tp_richcompare == NULL
      &&
#if PY_MAJOR_VERSION==2 && PY_MINOR_VERSION < 6
       arg->ob_type->tp_compare == NULL
#else
       arg->ob_type->tp_compare ==
       ((PyTypeObject *)object_)->ob_type->tp_compare
#endif
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
