
#define KEYMACROS_H "$Id: intkeymacros.h,v 1.4 2001/03/20 13:52:00 jim Exp $\n"

#define KEY_TYPE int
#define KEY_CHECK PyInt_Check
#define TEST_KEY(K, T) (((K) < (T)) ? -1 : (((K) > (T)) ? 1: 0)) 
#define DECREF_KEY(KEY)
#define INCREF_KEY(k)
#define COPY_KEY(KEY, E) (KEY=(E))
#define COPY_KEY_TO_OBJECT(O, K) O=PyInt_FromLong(K)
#define COPY_KEY_FROM_ARG(TARGET, ARG, STATUS) \
  if (PyInt_Check(ARG)) TARGET=PyInt_AsLong(ARG); else { \
      PyErr_SetString(PyExc_TypeError, "expected integer key"); \
      *(STATUS)=0; } 
