
#define VALUEMACROS_H "$Id: objectvaluemacros.h,v 1.4 2002/06/27 00:32:54 tim_one Exp $\n"

#define VALUE_TYPE PyObject *
#define VALUE_TYPE_IS_PYOBJECT
#define TEST_VALUE(VALUE, TARGET) PyObject_Compare((VALUE),(TARGET))
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define INCREF_VALUE(k) Py_INCREF(k)
#define DECREF_VALUE(k) Py_DECREF(k)
#define COPY_VALUE(k,e) k=(e)
#define COPY_VALUE_TO_OBJECT(O, K) O=(K); Py_INCREF(O)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, S) TARGET=(ARG)
#define NORMALIZE_VALUE(V, MIN) Py_INCREF(V)
