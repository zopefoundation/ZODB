#define KEYMACROS_H "$Id: objectkeymacros.h,v 1.3 2002/05/31 09:41:07 htrd Exp $\n"
#define KEY_TYPE PyObject *
#define TEST_KEY_SET_OR(V, KEY, TARGET) if ( ( (V) = PyObject_Compare((KEY),(TARGET)) ), PyErr_Occurred() )
#define INCREF_KEY(k) Py_INCREF(k)
#define DECREF_KEY(KEY) Py_DECREF(KEY)
#define COPY_KEY(KEY, E) KEY=(E)
#define COPY_KEY_TO_OBJECT(O, K) O=(K); Py_INCREF(O)
#define COPY_KEY_FROM_ARG(TARGET, ARG, S) TARGET=(ARG)
