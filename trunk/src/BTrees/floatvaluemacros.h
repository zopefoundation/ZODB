
#define VALUEMACROS_H "$Id$\n"

#define VALUE_TYPE float
#undef VALUE_TYPE_IS_PYOBJECT
#define TEST_VALUE(K, T) (((K) < (T)) ? -1 : (((K) > (T)) ? 1: 0))
#define VALUE_SAME(VALUE, TARGET) ( (VALUE) == (TARGET) )
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define VALUE_PARSE "f"
#define DECREF_VALUE(k)
#define INCREF_VALUE(k)
#define COPY_VALUE(V, E) (V=(E))
#define COPY_VALUE_TO_OBJECT(O, K) O=PyFloat_FromDouble(K)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, STATUS) \
  if (PyFloat_Check(ARG)) TARGET = (float)PyFloat_AsDouble(ARG); \
  else if (PyInt_Check(ARG)) TARGET = (float)PyInt_AsLong(ARG); \
  else { \
      PyErr_SetString(PyExc_TypeError, "expected float or int value"); \
      (STATUS)=0; (TARGET)=0; }

#define NORMALIZE_VALUE(V, MIN) ((MIN) > 0) ? ((V)/=(MIN)) : 0

#define MERGE_DEFAULT 1.0f
#define MERGE(O1, w1, O2, w2) ((O1)*(w1)+(O2)*(w2))
#define MERGE_WEIGHT(O, w) ((O)*(w))
