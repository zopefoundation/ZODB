
#define VALUEMACROS_H "$Id: objectvaluemacros.h,v 1.3 2001/03/20 13:52:00 jim Exp $\n"

#define VALUE_TYPE PyObject *
#define TEST_VALUE(VALUE, TARGET) PyObject_Compare((VALUE),(TARGET))
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define INCREF_VALUE(k) Py_INCREF(k)
#define DECREF_VALUE(k) Py_DECREF(k)
#define COPY_VALUE(k,e) k=(e)
#define COPY_VALUE_TO_OBJECT(O, K) O=(K); Py_INCREF(O)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, S) TARGET=(ARG)
#define NORMALIZE_VALUE(V, MIN) Py_INCREF(V)
