
/***********************************************************
     Copyright 

       Copyright 1997 Digital Creations, L.L.C., 910 Princess Anne
       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
       rights reserved.

******************************************************************/


static char BTree_module_documentation[] = 
""
"\n$Id: BTree.c,v 1.11 1997/12/12 23:43:05 jim Exp $"
;

#define PERSISTENT

#ifdef PERSISTENT
#include "cPersistence.h"
#else
#include "ExtensionClass.h"
#define PER_USE_OR_RETURN(self, NULL)
#define PER_ALLOW_DEACTIVATION(self)
#define PER_PREVENT_DEACTIVATION(self)
#endif


static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define UNLESS(E) if(!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define RETURN_NONE Py_INCREF(Py_None); return Py_None
#define LIST(O) ((PyListObject*)(O))
#define OBJECT(O) ((PyObject*)(O))

#define MIN_BUCKET_ALLOC 8
#define MAX_SIZE(N) 32

#ifdef INTKEY
#define KEY_TYPE INTKEY
#define KEY_PARSE "i"
#define TEST_KEY(k) ((k)-ikey)
#define DECREF_KEY(k)
#define ASSIGN_KEY(k,e) (k=e)
#else
#define KEY_TYPE PyObject *
#define KEY_PARSE "O"
#define TEST_KEY(k) PyObject_Compare(k,key)
#define DECREF_KEY(k) Py_DECREF(k)
#define ASSIGN_KEY(k,e) ASSIGN(k,e)
#endif

#ifdef INTVAL
#define VALUE_TYPE INTVAL
#define VALUE_PARSE "i"
#define DECREF_VALUE(k)
#define ASSIGN_VALUE(k,e) (k=e)
#else
#define VALUE_TYPE PyObject *
#define VALUE_PARSE "O"
#define DECREF_VALUE(k) Py_DECREF(k)
#define ASSIGN_VALUE(k,e) ASSIGN(k,e)
#endif

typedef struct ItemStruct {
  KEY_TYPE key;
  VALUE_TYPE value;
} Item;

typedef struct BTreeItemStruct {
  KEY_TYPE key;
  PyObject *value;
  int count;
} BTreeItem;

typedef struct {
  cPersistent_HEAD
  int size, len;
  Item *data;
} Bucket;

staticforward PyExtensionClass BucketType;

#define BUCKET(O) ((Bucket*)(O))
#define Bucket_Check(O) ((O)->ob_type==(PyTypeObject*)&BucketType)

typedef struct {
  cPersistent_HEAD
  int size, len;	
  BTreeItem *data;
  int count;
} BTree;

staticforward PyExtensionClass BucketType;

#define BTREE(O) ((BTree*)(O))
#define BTree_Check(O) ((O)->ob_type==(PyTypeObject*)&BTreeType)

/************************************************************************
  BTreeItems
  */

typedef struct {
  PyObject_HEAD
  BTree *data;
  int first, len;
  char kind;
} BTreeItems;

staticforward PyTypeObject BTreeItemsType;

static PyObject *
newBTreeItems(BTree *data, char kind, int first, int last)
{
  BTreeItems *self;
	
  UNLESS(self = PyObject_NEW(BTreeItems, &BTreeItemsType)) return NULL;
  Py_INCREF(data);
  self->data=data;
  self->kind=kind;
  self->first=first;
  self->len=last-first;
  return OBJECT(self);
}

static void
BTreeItems_dealloc(BTreeItems *self)
{
  Py_DECREF(self->data);
  PyMem_DEL(self);
}

static int
BTreeItems_length( BTreeItems *self)
{
  return self->len;
}

static PyObject * 
BTreeItems_concat( BTreeItems *self, PyObject *bb)
{
  PyErr_SetString(PyExc_TypeError,
		  "BTreeItems objects do not support concatenation");
  return NULL;
}

static PyObject *
BTreeItems_repeat(BTreeItems *self, int n)
{
  PyErr_SetString(PyExc_TypeError,
		  "BTreeItems objects do not support repetition");
  return NULL;
}

static PyObject *
BTreeItems_item_BTree(char kind, int i, BTree *btree)
{
  int l;
  BTreeItem *d;
  PyObject *r;

  PER_USE_OR_RETURN(btree, NULL);

  for(d=btree->data, l=btree->len;
      --l >= 0 && i >= d->count;
      i -= d->count, d++);

  PER_ALLOW_DEACTIVATION(btree);

  if(Bucket_Check(d->value))
    {
      PER_USE_OR_RETURN(d->value, NULL);
      switch(kind)
	{
	case 'k': 
#ifdef INTKEY
	  r=PyInt_FromLong((BUCKET(d->value)->data[i].key));
#else
	  r=(BUCKET(d->value)->data[i].key);
	  Py_INCREF(r);
#endif
	  break;
	case 'v': 
#ifdef INTVAL
	  r=PyInt_FromLong((BUCKET(d->value)->data[i].value));
#else
	  r=(BUCKET(d->value)->data[i].value);
	  Py_INCREF(r);
#endif
	  break;
	default:
	  r=Py_BuildValue(KEY_PARSE VALUE_PARSE,
			  BUCKET(d->value)->data[i].key,
			  BUCKET(d->value)->data[i].value);
	}
      PER_ALLOW_DEACTIVATION(BUCKET(d->value));
      return r;
    }
  return BTreeItems_item_BTree(kind, i, BTREE(d->value));
}

static PyObject *
BTreeItems_item(BTreeItems *self, int i)
{
  int j, l;

  j=i;
  l=self->len;
  if(j < 0) j += l;
  if(j < 0 || j >= l)
    {
      PyObject *v;
      v=PyInt_FromLong(i);
      UNLESS(v)
	{
	  v=Py_None;
	  Py_INCREF(v);
	}
      PyErr_SetObject(PyExc_IndexError, v);
      Py_DECREF(v);
      return NULL;
    }
  i=j+self->first;

  return BTreeItems_item_BTree(self->kind, i, self->data);
}

static PyObject *
BTreeItems_slice(BTreeItems *self, int ilow, int ihigh)
{
  if(ihigh > self->len) ihigh=self->len;
  ilow  += self->first;
  ihigh += self->first;
  return newBTreeItems(self->data, self->kind, ilow, ihigh);
}

static int
BTreeItems_ass_item(BTreeItems *self, int i, PyObject *v)
{
  PyErr_SetString(PyExc_TypeError,
		  "BTreeItems objects do not support item assignment");
  return -1;
}

static int
BTreeItems_ass_slice(PyListObject *self, int ilow, int ihigh, PyObject *v)
{
  PyErr_SetString(PyExc_TypeError,
		  "BTreeItems objects do not support slice assignment");
  return -1;
}

static PySequenceMethods BTreeItems_as_sequence = {
	(inquiry)BTreeItems_length,		/*sq_length*/
	(binaryfunc)BTreeItems_concat,		/*sq_concat*/
	(intargfunc)BTreeItems_repeat,		/*sq_repeat*/
	(intargfunc)BTreeItems_item,		/*sq_item*/
	(intintargfunc)BTreeItems_slice,		/*sq_slice*/
	(intobjargproc)BTreeItems_ass_item,	/*sq_ass_item*/
	(intintobjargproc)BTreeItems_ass_slice,	/*sq_ass_slice*/
};

/* -------------------------------------------------------------- */

static PyTypeObject BTreeItemsType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "BTreeItems",			/*tp_name*/
  sizeof(BTreeItems),	/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /* methods */
  (destructor)BTreeItems_dealloc,	/*tp_dealloc*/
  (printfunc)0,	/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,	/*tp_compare*/
  (reprfunc)0,		/*tp_repr*/
  0,		/*tp_as_number*/
  &BTreeItems_as_sequence,		/*tp_as_sequence*/
  0,		/*tp_as_mapping*/
  (hashfunc)0,		/*tp_hash*/
  (ternaryfunc)0,	/*tp_call*/
  (reprfunc)0,		/*tp_str*/
  0,			/*tp_getattro*/
  0,			/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Sequence type used to iterate over BTree items." /* Documentation string */
};

/************************************************************************/


static void *
PyMalloc(size_t sz)
{
  void *r;

  if(r=malloc(sz)) return r;

  PyErr_NoMemory();
  return NULL;
}

static void *
PyRealloc(void *p, size_t sz)
{
  void *r;

  if(r=realloc(p,sz)) return r;

  PyErr_NoMemory();
  return NULL;
}

static PyObject *
Twople(PyObject *i1, PyObject *i2)
{
  PyObject *t;
  
  if(t=PyTuple_New(2))
    {
      Py_INCREF(i1);
      PyTuple_SET_ITEM(t,0,i1);
      Py_INCREF(i2);
      PyTuple_SET_ITEM(t,1,i2);
    }

  return t;
}

static int
BTree_ini(BTree *self)
{
  PyObject *b;

  UNLESS(b=PyObject_CallObject(OBJECT(&BucketType), NULL)) return -1;
#ifndef INTKEY
  Py_INCREF(Py_None);
  self->data->key=Py_None;
#endif
  self->data->value=b;
  self->data->count=0;
  self->len=1;
  self->count=0;
  return 0;
}

static int
BTree_init(BTree *self)
{
  UNLESS(self->data=PyMalloc(sizeof(BTreeItem)*2)) return -1;
  self->size=2;
  return BTree_ini(self);
}

static int
bucket_index(Bucket *self, PyObject *key, int less)
{
  /*
    If less, return the index of the largest key that is less than or
    equall to key.  Otherwise return the index of the smallest key
    that is greater than or equal to key.
   */
  int min, max, i, l, cmp;
#ifdef INTKEY
  int ikey;

  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return -9;
    }
  ikey=PyInt_AsLong(key);
#endif

  PER_USE_OR_RETURN(self, -1);

  for(min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key);
      if(cmp < 0) min=i;
      else if(cmp == 0)
	{
	  PER_ALLOW_DEACTIVATION(self);
	  return i;
	}
      else max=i;
    }

  PER_ALLOW_DEACTIVATION(self);

  if(less) return max-1;
  if(max==min) return min;
  return min+1;
}

static PyObject *
_bucket_get(Bucket *self, PyObject *key, int has_key)
{
  int min, max, i, l, cmp;
  PyObject *r;
#ifdef INTKEY
  int ikey;

  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return NULL;
    }
  ikey=PyInt_AsLong(key);
#endif

  PER_USE_OR_RETURN(self, NULL);

  for(min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key);
      if(cmp < 0) min=i;
      else if(cmp == 0)
	{
	  if(has_key) r=PyInt_FromLong(1);
	  else
	    {
#ifdef INTVAL
	      r=PyInt_FromLong(self->data[i].value);
#else
	      r=self->data[i].value;
	      Py_INCREF(r);
#endif
	    }
	  PER_ALLOW_DEACTIVATION(self);
	  return r;
	}
      else max=i;
    }

  PER_ALLOW_DEACTIVATION(self);
  if(has_key) return PyInt_FromLong(0);
  PyErr_SetObject(PyExc_KeyError, key);
  return NULL;
}

static PyObject *
bucket_get(Bucket *self, PyObject *key)
{
  return _bucket_get(self, key, 0);
}

static PyObject *
bucket_map(Bucket *self, PyObject *args)
{
  PyObject *keys, *key, *r;
  int l, i, a;

  UNLESS(PyArg_ParseTuple(args,"O", &keys)) return NULL;
  if((l=PyObject_Length(keys)) < 0) return NULL;
  UNLESS(r=PyList_New(0)) return NULL;

  for(i=0; i < l; i++)
    {
      UNLESS(key=PySequence_GetItem(keys,i)) goto err;
      ASSIGN(key, _bucket_get(self, key, 0));
      if(key)
	{
	  a=PyList_Append(r,key);
	  Py_DECREF(key);
	  if(a<0) goto err;
	}
      else PyErr_Clear();
    }

  return r;

err:
  Py_DECREF(r);
  return NULL;
}

static int
BTree_index(BTree *self, PyObject *key, int less)
{
  /*
    If less, return the index of the largest key that is less than or
    equall to key.  Otherwise return the index of the smallest key
    that is greater than or equal to key.
   */
  int min, max, i, cmp;
#ifdef INTKEY
  int ikey;

  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return -9;
    }
  ikey=PyInt_AsLong(key);
#endif

  PER_USE_OR_RETURN(self, -1);
  
  UNLESS(self->data) if(BTree_init(self) < 0) goto err;
  
  for(min=0, max=self->len, i=max/2; max-min > 1; i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key);
      if(cmp < 0) min=i;
      else if(cmp == 0)
	{
	  min=i;
	  break;
	}
      else max=i;
    }

  if(Bucket_Check(self->data[min].value))
    i=bucket_index(BUCKET(self->data[min].value), key, less);
  else
    i= BTree_index( BTREE(self->data[min].value), key, less);

  if(i==-9) goto err;

  while(--min >= 0) i+=self->data[min].count;

  PER_ALLOW_DEACTIVATION(self);
  return i;

err:

  PER_ALLOW_DEACTIVATION(self);
  return -9;
}    

static PyObject *
_BTree_get(BTree *self, PyObject *key, int has_key)
{
  int min, max, i, cmp;
  PyObject *r;
#ifdef INTKEY
  int ikey;

  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return NULL;
    }
  ikey=PyInt_AsLong(key);
#endif

  PER_USE_OR_RETURN(self, NULL);

  UNLESS(self->data) if(BTree_init(self) < 0) goto err;
  
  for(min=0, max=self->len, i=max/2; max-min > 1; i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key);
      if(cmp < 0) min=i;
      else if(cmp == 0)
	{
	  min=i;
	  break;
	}
      else max=i;
    }

  if(Bucket_Check(self->data[min].value))
    r=_bucket_get(BUCKET(self->data[min].value), key, has_key);
  else
    r=_BTree_get( BTREE(self->data[min].value), key, has_key);

  PER_ALLOW_DEACTIVATION(self);
  return r;

err:

  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}    

static PyObject *
BTree_get(BTree *self, PyObject *key)
{
  return _BTree_get(self, key, 0);
}

static PyObject *
BTree_map(BTree *self, PyObject *args)
{
  PyObject *keys, *key, *r;
  int l, i, a;

  UNLESS(PyArg_ParseTuple(args,"O", &keys)) return NULL;
  if((l=PyObject_Length(keys)) < 0) return NULL;
  UNLESS(r=PyList_New(0)) return NULL;

  for(i=0; i < l; i++)
    {
      UNLESS(key=PySequence_GetItem(keys,i)) goto err;
      ASSIGN(key, _BTree_get(self, key, 0));
      if(key)
	{
	  a=PyList_Append(r,key);
	  Py_DECREF(key);
	  if(a<0) goto err;
	}
      else PyErr_Clear();
    }

  return r;

err:
  Py_DECREF(r);
  return NULL;
}


static int
_bucket_set(Bucket *self, PyObject *key, PyObject *v)
{
  int min, max, i, l, cmp;
  Item *d;
#ifdef INTKEY
  int ikey;
#endif
#ifdef INTVAL
  int iv;
#endif

#ifdef INTKEY
  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __setitem__ expected integer value");
      return -1;
    }
  ikey=PyInt_AsLong(key);
#endif

#ifdef INTVAL
  UNLESS(!v || PyInt_Check(v))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return -1;
    }
  iv=PyInt_AsLong(v);
#endif

  PER_USE_OR_RETURN(self, -1);

  for(min=0, max=l=self->len, i=max/2; i != l; l=i, i=(min+max)/2)
    {
      if((cmp=TEST_KEY(self->data[i].key)) < 0) min=i;
      else if(cmp==0)
	{
	  if(v)
	    {
	      Py_INCREF(v);
#ifdef INTVAL
	      self->data[i].value=iv;
#else
	      ASSIGN(self->data[i].value, v);
#endif
	      if(PER_CHANGED(self) < 0) goto err;
	      PER_ALLOW_DEACTIVATION(self);
	      return 0;
	    }
	  else
	    {
	      self->len--;
	      d=self->data+i;
	      DECREF_KEY(d->key);
	      DECREF_VALUE(d->value);
	      if(i < self->len)	memmove(d,d+1,sizeof(Item)*(self->len-i));
	      else if(! self->len)
		{
		  self->size=0;
		  free(self->data);
		  self->data=NULL;
		}
	      if(PER_CHANGED(self) < 0) goto err;
	      PER_ALLOW_DEACTIVATION(self);
	      return 1;
	    }
	}
      else max=i;
    }

  if(!v)
    {
      PyErr_SetObject(PyExc_KeyError, key);
      goto err;
    }

  if(self->len==self->size)
    {
      if(self->data)
	{
	  UNLESS(d=PyRealloc(self->data, sizeof(Item)*self->size*2)) goto err;
	  self->data=d;
	  self->size*=2;
	}
      else
	{
	  UNLESS(self->data=PyMalloc(sizeof(Item)*MIN_BUCKET_ALLOC)) goto err;
	  self->size=MIN_BUCKET_ALLOC;
	}
    }
  if(max != i) i++;
  d=self->data+i;
  if(self->len > i) memmove(d+1,d,sizeof(Item)*(self->len-i));
#ifdef INTKEY
  d->key=ikey;
#else
  d->key=key;
  Py_INCREF(key);
#endif
#ifdef INTVAL
  d->value=iv;
#else
  d->value=v;
  Py_INCREF(v);
#endif
  self->len++;

  if(PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  return 1;

err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

static int
bucket_setitem(Bucket *self, PyObject *key, PyObject *v)
{
  if(_bucket_set(self,key,v) < 0) return -1;
  return 0;
}

static int
bucket_split(Bucket *self, int index, Bucket *next)
{
  if(index < 0 || index >= self->len) index=self->len/2;

  UNLESS(next->data=PyMalloc(sizeof(Item)*(self->len-index))) return -1;
  next->len=self->len-index;
  next->size=next->len;
  memcpy(next->data, self->data+index, sizeof(Item)*next->size);

  self->len=index;

  return 0;
}

static int
BTree_count(BTree *self)
{
  int i, c=0;
  BTreeItem *d;

  for(i=self->len, d=self->data; --i >= 0; d++)
    c += d->count;

  return c;
}

static int
BTree_split(BTree *self, int index, BTree *next)
{
  if(index < 0 || index >= self->len) index=self->len/2;
  
  UNLESS(next->data=PyMalloc(sizeof(BTreeItem)*(self->len-index)))
    return -1;
  next->len=self->len-index;
  next->size=next->len;
  memcpy(next->data, self->data+index, sizeof(BTreeItem)*next->size);
  if((next->count=BTree_count(next)) < 0) return -1;
  
  self->len = index;
  self->count -= next->count;
  
  return 0;
}

static int
BTree_clone(BTree *self)
{
  /* We've grown really big without anybody splitting us.
     We should split ourselves.
   */
  BTree *n1=0, *n2=0;
  BTreeItem *d=0;
  int count;
  
  /* Create two BTrees to hold ourselves after split */
  UNLESS(n1=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL))) return -1;
  UNLESS(n2=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL))) goto err;

  /* Create a new data buffer to hold two BTrees */
  UNLESS(d=PyMalloc(sizeof(BTreeItem)*2)) goto err;

  count=self->count;

  /* Split ourself */
  if(BTree_split(self,-1,n2) < 0) goto err;
  
  /* Move our data to new BTree */
  n1->size=self->size;
  n1->len=self->len;
  n1->count=self->count;
  n1->data=self->data;

  /* Initialize our data to hold split data */
  self->data=d;
  Py_INCREF(Py_None);
#ifndef INTKEY
  self->data->key=Py_None;
#endif
  self->len=2;
  self->size=2;
  self->data->value=OBJECT(n1);
  self->data->count=n1->count;
#ifndef INTKEY
  Py_INCREF(n2->data->key);
#endif
  self->data[1].key=n2->data->key;
  self->data[1].value=OBJECT(n2);
  self->data[1].count=n2->count;
  self->count=count;

  return 0;

err:
  Py_XDECREF(n1);
  Py_XDECREF(n2);
  free(d);
  return -1;
}

static int 
BTree_grow(BTree *self, int index)
{
  int i;
  PyObject *v, *e=0;
  BTreeItem *d;

  if(self->len == self->size)
    {
      UNLESS(d=PyRealloc(self->data, sizeof(BTreeItem)*self->size*2))
	return -1;
      self->data=d;
      self->size *= 2;
    }

  d=self->data+index;
  v=d->value;
  UNLESS(e=PyObject_CallObject(OBJECT(v->ob_type), NULL)) return -1;

  PER_USE_OR_RETURN(v, -1);

  if(Bucket_Check(v))
    {
      i=bucket_split(BUCKET(v), -1, BUCKET(e));
      d->count=BUCKET(v)->len;
    }
  else
    {
      i=BTree_split(  BTREE(v), -1,   BTREE(e));
      d->count=BTREE(v)->count;      
    }

  PER_ALLOW_DEACTIVATION(BUCKET(v));

  if(i < 0)
    {
      Py_DECREF(e);
      return -1;
    }

  index++;
  d++;
  if(self->len > index)
    memmove(d+1, d, sizeof(BTreeItem)*(self->len-index));

  if(Bucket_Check(v))
    {
      d->key=BUCKET(e)->data->key;
      d->count=BUCKET(e)->len;
    }
  else
    {
      d->key=BTREE(e)->data->key;
      d->count=BTREE(e)->count;      
    }
#ifndef INTKEY
  Py_INCREF(d->key);
#endif
  d->value=e;
  
  self->len++;

  if(self->len >= MAX_SIZE(self) * 2) return BTree_clone(self);
  
  return 0;
}

static int
_BTree_set(BTree *self, PyObject *key, PyObject *value)
{
  int i, min, max, cmp, grew;
  BTreeItem *d;
#ifdef INTKEY
  int ikey;
#endif
#ifdef INTVAL
  int iv;
#endif

#ifdef INTKEY
  UNLESS(PyInt_Check(key))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __setitem__ expected integer value");
      return -1;
    }
  ikey=PyInt_AsLong(key);
#endif

#ifdef INTVAL
  UNLESS(!value || PyInt_Check(value))
    {
      PyErr_SetString(PyExc_TypeError,
		      "Bucket __getitem__ expected integer key");
      return -1;
    }
  iv=PyInt_AsLong(value);
#endif

  PER_USE_OR_RETURN(self, -1);

  UNLESS(self->data) if(BTree_init(self) < 0) goto err;

  for(min=0, max=self->len, i=max/2; max-min > 1; i=(max+min)/2)
    {
      d=self->data+i;
      cmp=TEST_KEY(d->key);
      if(cmp < 0) min=i;
      else if(cmp==0)
	{
	  min=i;
	  break;
	}
      else max=i;
    }

  d=self->data+min;
  if(Bucket_Check(d->value))
    grew=_bucket_set(BUCKET(d->value), key, value);
  else
    grew= _BTree_set( BTREE(d->value), key, value);
  if(grew < 0) goto err;

  if(grew)
    {
      if(value)			/* got bigger */
	{
	  d->count++;
	  self->count++;
	  if(BUCKET(d->value)->len > MAX_SIZE(self) &&
	     BTree_grow(self,min) < 0) goto err;
	}
      else			/* got smaller */
	{
	  d->count--;
	  self->count--;
	  if(! d->count)
	    {
	      self->len--;
	      Py_DECREF(d->value);
	      DECREF_KEY(d->key);
	      if(min < self->len)
		memmove(d, d+1, (self->len-min)*sizeof(BTreeItem));
	    }
	}
      if(PER_CHANGED(self) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return grew;

err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

static int
BTree_setitem(BTree *self, PyObject *key, PyObject *v)
{
  if(_BTree_set(self,key,v) < 0) return -1;
  return 0;
}

  
static PyObject *
bucket_keys(Bucket *self, PyObject *args)
{
  PyObject *r=0, *key;
  int i;
  
  PER_USE_OR_RETURN(self, NULL);

  UNLESS(r=PyList_New(self->len)) goto err;

  for(i=self->len; --i >= 0; )
    {
#ifdef INTKEY
      UNLESS(key=PyInt_FromLong(self->data[i].key)) goto err;
#else
      key=self->data[i].key;
      Py_INCREF(key);
#endif
      if(PyList_SetItem(r, i, key) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);
  return NULL;
}
  
static PyObject *
bucket_values(Bucket *self, PyObject *args)
{
  PyObject *r=0, *v;
  int i;
  
  PER_USE_OR_RETURN(self, NULL);
  
  UNLESS(r=PyList_New(self->len)) goto err;

  for(i=self->len; --i >= 0; )
    {
#ifdef INTVAL
      UNLESS(v=PyInt_FromLong(self->data[i].value)) goto err;
#else
      v=self->data[i].value;
      Py_INCREF(v);
#endif
      if(PyList_SetItem(r, i, v) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);
  return NULL;
}
  
static PyObject *
bucket_items(Bucket *self, PyObject *args)
{
  PyObject *r, *item;
  int i;
  
  PER_USE_OR_RETURN(self, NULL);
  
  UNLESS(r=PyList_New(self->len)) goto err;

  for(i=self->len; --i >= 0; )
    {
      UNLESS(item=Py_BuildValue(KEY_PARSE VALUE_PARSE,
				self->data[i].key,self->data[i].value))
	goto err;
      if(PyList_SetItem(r, i, item) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);
  return NULL;
}

#ifdef PERSISTENT
static PyObject *
bucket__p___reinit__(Bucket *self, PyObject *args)
{
  int i;
  /* Note that this implementation is broken, in that it doesn't
     account for subclass needs. */
  for(i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      DECREF_VALUE(self->data[i].value);
    }
  self->len=0;
  Py_INCREF(Py_None);
  return Py_None;
}
#endif
  
static PyObject *
bucket_clear(Bucket *self, PyObject *args)
{
  int i;

  PER_USE_OR_RETURN(self, NULL);

  for(i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      DECREF_VALUE(self->data[i].value);
    }
  self->len=0;
  if(PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  RETURN_NONE;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}
  
static int
_BTree_clear(BTree *self)
{
  int i;

  UNLESS(self->data) return 0;

  for(i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      Py_DECREF(self->data[i].value);
    }

  i=BTree_ini(self);

  return i;
}

#ifdef PERSISTENT
static PyObject *
BTree__p___reinit__(BTree *self, PyObject *args)
{
  /* Note that this implementation is broken, in that it doesn't
     account for subclass needs. */
  if(_BTree_clear(self) < 0) return NULL;
  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static PyObject *
BTree_clear(BTree *self, PyObject *args)
{
  PER_USE_OR_RETURN(self, NULL);
  if(_BTree_clear(self) < 0) goto err;

  if(PER_CHANGED(self) < 0) goto err;

  PER_ALLOW_DEACTIVATION(self);

  RETURN_NONE;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
bucket_getstate(Bucket *self, PyObject *args)
{
  PyObject *r, *keys=0, *values=0;
  int i, l;
#ifdef INTKEY
  int v;
  char *c;
#else
#ifdef INTVAL
  int v;
  char *c;
#endif
#endif

  PER_USE_OR_RETURN(self, NULL);

  l=self->len;

#ifdef INTKEY
  UNLESS(keys=PyString_FromStringAndSize(NULL,l*sizeof(int))) goto err;
  UNLESS(c=PyString_AsString(keys)) goto err;
  for(i=0; i < l; i++)
    {
      v=self->data[i].key;
      *c++ = (int)( v        & 0xff);
      *c++ = (int)((v >> 8)  & 0xff);
      *c++ = (int)((v >> 16) & 0xff);
      *c++ = (int)((v >> 24) & 0xff);
    }
#else
  UNLESS(keys=PyTuple_New(self->len)) goto err;
  for(i=0; i<l; i++)
    {
      r=self->data[i].key;
      Py_INCREF(r);
      PyTuple_SET_ITEM(keys,i,r);
    }
#endif

#ifdef INTVAL
  UNLESS(values=PyString_FromStringAndSize(NULL,l*sizeof(int))) goto err;
  UNLESS(c=PyString_AsString(values)) goto err;
  for(i=0; i < l; i++)
    {
      v=self->data[i].value;
      *c++ = (int)( v        & 0xff);
      *c++ = (int)((v >> 8)  & 0xff);
      *c++ = (int)((v >> 16) & 0xff);
      *c++ = (int)((v >> 24) & 0xff);
    }
#else
  UNLESS(values=PyTuple_New(self->len)) goto err;
  for(i=0; i<l; i++)
    {
      r=self->data[i].value;
      Py_INCREF(r);
      PyTuple_SET_ITEM(values,i,r);
    }
#endif

  PER_ALLOW_DEACTIVATION(self);
  r=Py_BuildValue("OO",keys,values);
  return r;

err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(keys);
  Py_XDECREF(values);
  return NULL;
}


static PyObject *
bucket_setstate(Bucket *self, PyObject *args)
{
  PyObject *r, *keys=0, *values=0;
  int i, l, v;
  Item *d;
#ifdef INTKEY
  char *ck;
#endif
#ifdef INTVAL
  char *cv;
#endif

  PER_PREVENT_DEACTIVATION(self); 

  UNLESS(PyArg_ParseTuple(args,"O",&r)) goto err;
  UNLESS(PyArg_ParseTuple(r,"OO",&keys,&values)) goto err;

  if((l=PyObject_Length(keys)) < 0) goto err;
#ifdef INTKEY
  l/=4;
  UNLESS(ck=PyString_AsString(keys)) goto err;
#endif

  if((v=PyObject_Length(values)) < 0) goto err;
#ifdef INTVAL
  v/=4;
  UNLESS(cv=PyString_AsString(values)) goto err;
#endif

  if(l!=v)
    {
      PyErr_SetString(PyExc_ValueError,
		      "number of keys differs from number of values");
      goto err;
    }
  
  if(l > self->size)
    if(self->data)
      {
	UNLESS(d=PyRealloc(self->data, sizeof(Item)*l)) goto err;
	self->data=d;
	self->size=l;
      }
    else
      {
	UNLESS(d=PyMalloc(sizeof(Item)*l)) goto err;
	self->data=d;
	self->size=l;
      }
  else d=self->data;

#ifdef INTKEY
  for(i=l; --i >= 0; d++)
    {
      v  = ((int)(unsigned char)*ck++)      ;
      v |= ((int)(unsigned char)*ck++) <<  8;
      v |= ((int)(unsigned char)*ck++) << 16;
      v |= ((int)(unsigned char)*ck++) << 24;
      d->key=v;
    }
#else

  for(i=0; i<l; i++, d++)
    {
      UNLESS(r=PySequence_GetItem(keys,i)) goto err;
      if(i < self->len) Py_DECREF(d->key);
      d->key=r;
    }
#endif

  d=self->data;

#ifdef INTVAL
  for(i=l; --i >= 0; d++)
    {
      v  = ((int)(unsigned char)*cv++)      ;
      v |= ((int)(unsigned char)*cv++) <<  8;
      v |= ((int)(unsigned char)*cv++) << 16;
      v |= ((int)(unsigned char)*cv++) << 24;
      d->value=v;
    }
#else

  for(i=0; i<l; i++, d++)
    {
      UNLESS(r=PySequence_GetItem(values,i)) goto err;
      if(i < self->len) Py_DECREF(d->value);
      d->value=r;
    }
#endif

  self->len=l;

  PER_ALLOW_DEACTIVATION(self);
  Py_INCREF(Py_None);
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
bucket_has_key(Bucket *self, PyObject *args)
{
  PyObject *key;

  UNLESS(PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _bucket_get(self, key, 1);
}

static struct PyMethodDef Bucket_methods[] = {
  {"__getstate__", (PyCFunction)bucket_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction)bucket_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"keys",	(PyCFunction)bucket_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
  {"has_key",	(PyCFunction)bucket_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"values",	(PyCFunction)bucket_values,	METH_VARARGS,
     "values() -- Return the values"},
  {"items",	(PyCFunction)bucket_items,	METH_VARARGS,
     "items() -- Return the items"},
  {"clear",	(PyCFunction)bucket_clear,	METH_VARARGS,
     "clear() -- Remove all of the items from the bucket"},
  {"map",	(PyCFunction)bucket_map,	METH_VARARGS,
     "map(keys) -- map a sorted sequence of keys into values\n\n"
     "Invalid keys are skipped"},
#ifdef PERSISTENT
  {"_p___reinit__",	(PyCFunction)bucket__p___reinit__,	METH_VARARGS,
   "_p___reinit__() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static PyObject *
BTree_getstate(BTree *self, PyObject *args)
{
  PyObject *r=0, *item;
  int i;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS(r=PyTuple_New(self->len)) goto err;
  for(i=self->len; --i >= 0; )
    {
      UNLESS(item=Py_BuildValue(KEY_PARSE "Oi",
				self->data[i].key, self->data[i].value,
				self->data[i].count))
	goto err;
      PyTuple_SET_ITEM(r,i,item);
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

err:
  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);
  return NULL;
}

static PyObject *
BTree_setstate(BTree *self, PyObject *args)
{
  PyObject *state, *v=0;
  BTreeItem *d;
  int l, i;

  UNLESS(PyArg_ParseTuple(args,"O",&state)) return NULL;
  if((l=PyTuple_Size(state))<0) return NULL;
  
  PER_PREVENT_DEACTIVATION(self); 

  if(l>self->size)
    {
      if(self->data)
	{
	  UNLESS(d=PyRealloc(self->data, sizeof(BTreeItem)*l)) goto err;
	  self->data=d;
	  self->size=l;
	}
      else
	{
	  UNLESS(self->data=PyMalloc(sizeof(BTreeItem)*l)) goto err;
	  self->size=l;
	}
    }
  for(i=self->len, d=self->data; --i >= 0; d++)
    {
      DECREF_KEY(d->key);
      Py_DECREF(d->value);
    }
  for(self->len=0, self->count=0, i=0, d=self->data; i < l;
      i++, d++, self->len++)
    {
      UNLESS(PyArg_ParseTuple(PyTuple_GET_ITEM(state,i),
			      KEY_PARSE "Oi",
			      &(d->key), &(d->value), &(d->count)))
	goto err;
#ifndef INTKEY
      Py_INCREF(d->key);
#endif
      Py_INCREF(d->value);
      self->count+=d->count;
    }

  PER_ALLOW_DEACTIVATION(self);
  Py_INCREF(Py_None);
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
BTree_elements(BTree *self, PyObject *args, char type)
{
  PyObject *f=0, *l=0;
  int fi, li;

  UNLESS(PyArg_ParseTuple(args,"|OO",&f, &l)) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  if(f && f != Py_None)
    {
      fi=BTree_index(self, f, 0);
      if(fi==-9) goto err;
    }
  else fi=0;

  if(l)
    {
      li=BTree_index(self, l, 1);
      if(li==-9) goto err;
      li++;
    }
  else li=self->count;
  
  PER_ALLOW_DEACTIVATION(self);
  return newBTreeItems(self,type,fi,li);

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
BTree_keys(BTree *self, PyObject *args)
{
  return BTree_elements(self,args,'k');
}

static PyObject *
BTree_values(BTree *self, PyObject *args)
{
  return BTree_elements(self,args,'v');
}

static PyObject *
BTree_items(BTree *self, PyObject *args)
{
  return BTree_elements(self,args,'i');
}

static PyObject *
BTree_has_key(BTree *self, PyObject *args)
{
  PyObject *key;

  UNLESS(PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _BTree_get(self, key, 1);
}

static struct PyMethodDef BTree_methods[] = {
  {"__getstate__",	(PyCFunction)BTree_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction)BTree_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"has_key",	(PyCFunction)BTree_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"keys",	(PyCFunction)BTree_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
  {"values",	(PyCFunction)BTree_values,	METH_VARARGS,
     "values() -- Return the values"},
  {"items",	(PyCFunction)BTree_items,	METH_VARARGS,
     "items() -- Return the items"},
  {"clear",	(PyCFunction)BTree_clear,	METH_VARARGS,
     "clear() -- Remove all of the items from the BTree"},  
  {"map",	(PyCFunction)BTree_map,	METH_VARARGS,
     "map(keys) -- map a sorted sequence of keys into values\n\n"
     "Invalid keys are skipped"},
#ifdef PERSISTENT
  {"_p___reinit__",	(PyCFunction)BTree__p___reinit__,	METH_VARARGS,
   "_p___reinit__() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

static void
Bucket_dealloc(Bucket *self)
{
  int i;

  for(i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      DECREF_VALUE(self->data[i].value);
    }
  free(self->data);

  PyMem_DEL(self);
}

static void
BTree_dealloc(BTree *self)
{
  int i;

  for(i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      Py_DECREF(self->data[i].value);
    }
  free(self->data);
  PyMem_DEL(self);
}

/* Code to access Bucket objects as mappings */

static int
Bucket_length( Bucket *self)
{
  int r;
  PER_USE_OR_RETURN(self, -1);
  r=self->len;
  PER_ALLOW_DEACTIVATION(self);
  return r;
}

static PyMappingMethods Bucket_as_mapping = {
  (inquiry)Bucket_length,		/*mp_length*/
  (binaryfunc)bucket_get,		/*mp_subscript*/
  (objobjargproc)bucket_setitem,	/*mp_ass_subscript*/
};

static int
BTree_length( BTree *self)
{
  int r;
  PER_USE_OR_RETURN(self, -1);
  r=self->count;
  PER_ALLOW_DEACTIVATION(self);
  return r;
}

static PyMappingMethods BTree_as_mapping = {
  (inquiry)BTree_length,		/*mp_length*/
  (binaryfunc)BTree_get,		/*mp_subscript*/
  (objobjargproc)BTree_setitem,	/*mp_ass_subscript*/
};

static PyObject *
bucket_repr(Bucket *self)
{
  static PyObject *format;
  PyObject *r, *t;

  UNLESS(format) UNLESS(format=PyString_FromString("Bucket(%s)")) return NULL;
  UNLESS(t=PyTuple_New(1)) return NULL;
  UNLESS(r=bucket_items(self,NULL)) goto err;
  PyTuple_SET_ITEM(t,0,r);
  r=t;
  ASSIGN(r,PyString_Format(format,r));
  return r;
err:
  Py_DECREF(t);
  return NULL;
}

static PyExtensionClass BucketType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "Bucket",			/*tp_name*/
  sizeof(Bucket),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /*********** methods ***********************/
  (destructor)Bucket_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)bucket_repr,	/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &Bucket_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,		/*tp_getattro*/
  0,				/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Mapping type implemented as sorted list of items", 
  METHOD_CHAIN(Bucket_methods),
  EXTENSIONCLASS_BASICNEW_FLAG,
};

static PyExtensionClass BTreeType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  "BTree",			/*tp_name*/
  sizeof(BTree),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /************* methods ********************/
  (destructor)BTree_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)0,			/*tp_repr*/
  0,				/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &BTree_as_mapping,		/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,
  0,				/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Mapping type implemented as sorted list of items", 
  METHOD_CHAIN(BTree_methods),
  EXTENSIONCLASS_BASICNEW_FLAG,
};

static struct PyMethodDef module_methods[] = {
  {NULL,		NULL}		/* sentinel */
};

void
#ifdef INTKEY
#ifdef INTVAL
initIIBTree()
#define MODNAME "IIBTree"
#else
initIOBTree()
#define MODNAME "IOBTree"
#endif
#else
#ifdef INTVAL
initOIBTree()
#define MODNAME "OIBTree"
#else
initBTree()
#define MODNAME "BTree"
#endif
#endif
{
  PyObject *m, *d;
  char *rev="$Revision: 1.11 $";

  UNLESS(PyExtensionClassCAPI=PyCObject_Import("ExtensionClass","CAPI"))
      return;

#ifdef PERSISTENT
  if(cPersistenceCAPI=PyCObject_Import("cPersistence","CAPI"))
    {
	static PyMethodChain mb, mn;

	mb.methods=BucketType.methods.methods;
	BucketType.methods.methods=cPersistenceCAPI->methods->methods;
	BucketType.methods.link=&mb;
	BucketType.tp_getattro=cPersistenceCAPI->getattro;
	BucketType.tp_setattro=cPersistenceCAPI->setattro;


	mn.methods=BTreeType.methods.methods;
	BTreeType.methods.methods=cPersistenceCAPI->methods->methods;
	BTreeType.methods.link=&mn;
	BTreeType.tp_getattro=cPersistenceCAPI->getattro;
	BTreeType.tp_setattro=cPersistenceCAPI->setattro;

    }
  else return;
#else
  BucketType.tp_getattro=PyExtensionClassCAPI->getattro;
  BTreeType.tp_getattro=PyExtensionClassCAPI->getattro;
#endif


  /* Create the module and add the functions */
  m = Py_InitModule4(MODNAME, module_methods,
		     BTree_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);

  PyExtensionClass_Export(d,"Bucket",BucketType);
  PyExtensionClass_Export(d,"BTree",BTreeType);

  PyDict_SetItemString(d, "__version__",
		       PyString_FromStringAndSize(rev+11,strlen(rev+11)-2));
  

#include "dcprotect.h"
	
  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module BTree");
}

/*
  PER_USE_OR_RETURN(self, NULL);
  PER_ALLOW_DEACTIVATION(self);
 */

/*****************************************************************************
Revision Log:

  $Log: BTree.c,v $
  Revision 1.11  1997/12/12 23:43:05  jim
  Added basicnew support.

  Revision 1.10  1997/11/13 20:45:51  jim
  Fixed some bad return values.

  Revision 1.9  1997/11/13 20:38:35  jim
  added dcprotect

  Revision 1.8  1997/11/03 15:17:53  jim
  Fixed stupid bug in has_key methods.

  Revision 1.7  1997/10/30 20:58:43  jim
  Upped bucket sizes.

  Revision 1.6  1997/10/10 18:21:45  jim
  Fixed bug in range queries.

  Revision 1.5  1997/10/01 02:47:06  jim
  Fixed bug in setstate that allocates too much memory.

  Revision 1.4  1997/09/17 17:20:32  jim
  Fixed bug in deleting members from BTree.

  Revision 1.3  1997/09/12 18:35:45  jim
  Fixed bug leading to random core dumps.

  Revision 1.2  1997/09/10 17:24:47  jim
  *** empty log message ***

  Revision 1.1  1997/09/08 18:42:21  jim
  initial BTree

  $Revision 1.1  1997/02/24 23:25:42  jim
  $initial
  $

*****************************************************************************/
