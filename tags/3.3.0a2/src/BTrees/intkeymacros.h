
#define KEYMACROS_H "$Id: intkeymacros.h,v 1.10 2002/06/27 00:32:54 tim_one Exp $\n"

#define KEY_TYPE int
#undef KEY_TYPE_IS_PYOBJECT
#define KEY_CHECK PyInt_Check
#define TEST_KEY_SET_OR(V, K, T) if ( ( (V) = (((K) < (T)) ? -1 : (((K) > (T)) ? 1: 0)) ) , 0 )
#define DECREF_KEY(KEY)
#define INCREF_KEY(k)
#define COPY_KEY(KEY, E) (KEY=(E))
#define COPY_KEY_TO_OBJECT(O, K) O=PyInt_FromLong(K)
#define COPY_KEY_FROM_ARG(TARGET, ARG, STATUS) \
  if (PyInt_Check(ARG)) TARGET=PyInt_AS_LONG(ARG); else { \
      PyErr_SetString(PyExc_TypeError, "expected integer key"); \
      (STATUS)=0; (TARGET)=0; }
#define MULTI_INT_UNION 1
