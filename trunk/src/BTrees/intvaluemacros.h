#define VALUE_TYPE int
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define VALUE_PARSE "i"
#define DECREF_VALUE(k)
#define INCREF_VALUE(k)
#define COPY_VALUE(V, E) (V=(E))
#define COPY_VALUE_TO_OBJECT(O, K) O=PyInt_FromLong(K) 
#define COPY_VALUE_FROM_ARG(TARGET, ARG, STATUS) \
  if (PyInt_Check(ARG)) TARGET=PyInt_AsLong(ARG); else { \
      PyErr_SetString(PyExc_TypeError, "expected integer value"); \
      *(STATUS)=0; } 
  
#define MERGE_DEFAULT 1
#define MERGE(O1, w1, O2, w2) ((O1)*(w1)+(O2)*(w2))
#define MERGE_WEIGHT(O, w) ((O)*(w))
