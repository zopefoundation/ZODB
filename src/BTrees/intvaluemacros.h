
#define VALUEMACROS_H "$Id$\n"

#define NEED_LONG_LONG_SUPPORT

#define VALUE_TYPE PY_LONG_LONG
#undef VALUE_TYPE_IS_PYOBJECT
#define TEST_VALUE(K, T) (((K) < (T)) ? -1 : (((K) > (T)) ? 1: 0)) 
#define VALUE_SAME(VALUE, TARGET) ( (VALUE) == (TARGET) )
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define VALUE_PARSE "L"
#define DECREF_VALUE(k)
#define INCREF_VALUE(k)
#define COPY_VALUE(V, E) (V=(E))
#define COPY_VALUE_TO_OBJECT(O, K) O=longlong_as_object(K)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, STATUS) \
    if (PyInt_Check(ARG)) TARGET=PyInt_AS_LONG(ARG); else \
        if (longlong_check(ARG)) TARGET=PyLong_AsLongLong(ARG); else \
            if (PyLong_Check(ARG)) { \
                PyErr_SetString(PyExc_ValueError, "long integer out of range"); \
                (STATUS)=0; (TARGET)=0; } \
            else { \
            PyErr_SetString(PyExc_TypeError, "expected integer key");   \
            (STATUS)=0; (TARGET)=0; }

#define NORMALIZE_VALUE(V, MIN) ((MIN) > 0) ? ((V)/=(MIN)) : 0

#define MERGE_DEFAULT 1
#define MERGE(O1, w1, O2, w2) ((O1)*(w1)+(O2)*(w2))
#define MERGE_WEIGHT(O, w) ((O)*(w))
