/*****************************************************************************
  
  Zope Public License (ZPL) Version 1.0
  -------------------------------------
  
  Copyright (c) Digital Creations.  All rights reserved.
  
  This license has been certified as Open Source(tm).
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
  
  1. Redistributions in source code must retain the above copyright
     notice, this list of conditions, and the following disclaimer.
  
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions, and the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.
  
  3. Digital Creations requests that attribution be given to Zope
     in any manner possible. Zope includes a "Powered by Zope"
     button that is installed by default. While it is not a license
     violation to remove this button, it is requested that the
     attribution remain. A significant investment has been put
     into Zope, and this effort will continue if the Zope community
     continues to grow. This is one way to assure that growth.
  
  4. All advertising materials and documentation mentioning
     features derived from or use of this software must display
     the following acknowledgement:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     In the event that the product being advertised includes an
     intact Zope distribution (with copyright and license included)
     then this clause is waived.
  
  5. Names associated with Zope or Digital Creations must not be used to
     endorse or promote products derived from this software without
     prior written permission from Digital Creations.
  
  6. Modified redistributions of any form whatsoever must retain
     the following acknowledgment:
  
       "This product includes software developed by Digital Creations
       for use in the Z Object Publishing Environment
       (http://www.zope.org/)."
  
     Intact (re-)distributions of any official Zope release do not
     require an external acknowledgement.
  
  7. Modifications are encouraged but must be packaged separately as
     patches to official Zope releases.  Distributions that do not
     clearly separate the patches from the original work must be clearly
     labeled as unofficial distributions.  Modifications which do not
     carry the name Zope may be packaged in any form, as long as they
     conform to all of the clauses above.
  
  
  Disclaimer
  
    THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
    EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
    USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
    OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
    SUCH DAMAGE.
  
  
  This software consists of contributions made by Digital Creations and
  many individuals on behalf of Digital Creations.  Specific
  attributions are listed in the accompanying credits file.
  
 ****************************************************************************/

static char BTree_module_documentation[] = 
""
"\n$Id: BTreeTemplate.c,v 1.3 2001/02/04 18:00:31 jim Exp $"
;

#include "cPersistence.h"

/***************************************************************
   The following are macros that ought to be in cPersistence.h */
#ifndef PER_USE 

#define PER_USE(O) \
(((O)->state != cPersistent_GHOST_STATE \
  || (cPersistenceCAPI->setstate((PyObject*)(O)) >= 0)) \
 ? (((O)->state==cPersistent_UPTODATE_STATE) \
    ? ((O)->state=cPersistent_STICKY_STATE) : 1) : 0)

#define Ghost_Test(O) ((O)->state == cPersistent_GHOST_STATE)
#endif
/***************************************************************/


static void PyVar_Assign(PyObject **v, PyObject *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGN(V,E) PyVar_Assign(&(V),(E))
#define ASSIGNC(V,E) (Py_INCREF((E)), PyVar_Assign(&(V),(E)))
#define UNLESS(E) if (!(E))
#define UNLESS_ASSIGN(V,E) ASSIGN(V,E); UNLESS(V)
#define RETURN_NONE Py_INCREF(Py_None); return Py_None
#define LIST(O) ((PyListObject*)(O))
#define OBJECT(O) ((PyObject*)(O))

#define MIN_BUCKET_ALLOC 16
#define MAX_BTREE_SIZE(B) 256
#define MAX_BUCKET_SIZE(B) DEFAULT_MAX_BUCKET_SIZE
#define MAX_SIZE(B) (Bucket_Check(B) ? MAX_BUCKET_SIZE(B) : MAX_BTREE_SIZE(B))


typedef struct ItemStruct {
  KEY_TYPE key;
#ifndef NOVAL
  VALUE_TYPE value;
#endif
} Item;

typedef struct BTreeItemStruct {
  KEY_TYPE key;
  PyObject *value;
} BTreeItem;

typedef struct bucket_s {
  cPersistent_HEAD
  int size, len;
  struct bucket_s *next;
  Item *data;
} Bucket;

staticforward PyExtensionClass BucketType;

#define BUCKET(O) ((Bucket*)(O))
#define Bucket_Check(O) ((O)->ob_type==(PyTypeObject*)&BucketType)

static void PyVar_AssignB(Bucket **v, Bucket *e) { Py_XDECREF(*v); *v=e;}
#define ASSIGNB(V,E) PyVar_AssignB(&(V),(E))
#define ASSIGNBC(V,E) (Py_INCREF((E)), PyVar_AssignB(&(V),(E)))

typedef struct {
  cPersistent_HEAD
  int size, len;
  Bucket *firstbucket;
  BTreeItem *data;
} BTree;

staticforward PyExtensionClass BTreeType;

#define BTREE(O) ((BTree*)(O))
#define BTree_Check(O) ((O)->ob_type==(PyTypeObject*)&BTreeType)

typedef struct {
  PyObject_HEAD
  Bucket *firstbucket;			/* First bucket known		*/
  Bucket *currentbucket;		/* Current bucket position	*/
  Bucket *lastbucket;			/* Last bucket position		*/
  int currentoffset;			/* Start count of current bucket*/
  int pseudoindex;			/* Its an indicator		*/
  int first, last;
#ifndef NOVAL
  char kind;
#endif
} BTreeItems;

staticforward PyTypeObject BTreeItemsType;

static PyObject *
IndexError(int i)
{                              
  PyObject *v;

  v=PyInt_FromLong(i);
  UNLESS (v) {
    v=Py_None;
    Py_INCREF(v);
  }
  PyErr_SetObject(PyExc_IndexError, v);
  Py_DECREF(v);
  return NULL;
}

static Bucket *
PreviousBucket(Bucket *current, Bucket *first, int i)
{
  if (! first) return NULL;
  if (first==current)
    {
      IndexError(i);
      return NULL;
    }

  Py_INCREF(first);
  while (1)
    {
      PER_USE_OR_RETURN(first,NULL);
      if (first->next==current) 
        {
          PER_ALLOW_DEACTIVATION(first);
          return first;
        }
      else if (first->next)
        {
          Bucket *next = first->next;
          Py_INCREF(next);
          PER_ALLOW_DEACTIVATION(first);
          Py_DECREF(first);
          first=next;
        }
      else
        {
          PER_ALLOW_DEACTIVATION(first);
          Py_DECREF(first);
          IndexError(i);
          return NULL;
        }
    }
}


/*
** newBTreeItems
**
** Creates a new slice mapping into a BTree, of type 'k','v' or other
** which has a low and high bound
**
** Arguments:	data	The base BTree
**		kind	'k','v' or other for key, value or object
**		lowbucket First bucket in range or NULL
**		lowoffset offset of first element in bucket or -1
**		highbucket Last bucket in range or NULL
**		highoffset offset of last element in bucket or -1
**
** Returns: 	newly created BTreeItems object
*/
static PyObject *
newBTreeItems(
#ifndef NOVAL
              char kind, 
#endif
              Bucket *lowbucket, int lowoffset,
              Bucket *highbucket, int highoffset)
{
  BTreeItems *self;
	
  UNLESS (self = PyObject_NEW(BTreeItems, &BTreeItemsType)) return NULL;
#ifndef NOVAL
  self->kind=kind;
#endif
  self->first=lowoffset;
  self->last=highoffset;
  Py_INCREF(lowbucket);
  self->firstbucket = lowbucket;
  Py_INCREF(highbucket);
  self->lastbucket = highbucket;
  Py_INCREF(lowbucket);
  self->currentbucket = lowbucket;
  self->currentoffset = lowoffset;
  self->pseudoindex = 0;

  return OBJECT(self);
}

static void
BTreeItems_dealloc(BTreeItems *self)
{
  Py_DECREF(self->firstbucket);
  Py_DECREF(self->lastbucket);
  Py_DECREF(self->currentbucket);
  PyMem_DEL(self);
}

static int 
BTreeItems_length_or_nonzero(BTreeItems *self, int nonzero)
{
  int r;
  Bucket *b, *next;

  b=self->firstbucket;
  r=self->last + 1 - self->first;

  if (nonzero && r > 0) 
    /* Short-circuit if all we care about is nonempty */
    return 1;

  if (b == self->lastbucket) return r;

  Py_INCREF(b);
  PER_USE_OR_RETURN(b, -1);
  while ((next=b->next)) 
    {
      r += b->len;
      if (nonzero && r > 0) 
        /* Short-circuit if all we care about is nonempty */
        break;

      if (next == self->lastbucket) 
        break; /* we already counted the last bucket */

      Py_INCREF(next);
      PER_ALLOW_DEACTIVATION(b);
      Py_DECREF(b);
      b=next;
      PER_USE_OR_RETURN(b, -1);
    }
  PER_ALLOW_DEACTIVATION(b);
  Py_DECREF(b);

  return r >= 0 ? r : 0;
}

static int
BTreeItems_length( BTreeItems *self)
{                              
  return BTreeItems_length_or_nonzero(self, 0);
}

static int 
firstBucketOffset(Bucket **bucket, int *offset)
{
  Bucket *b;

  *offset = (*bucket)->len - 1;
  while ((*bucket)->len < 1)
    {
      b=(*bucket)->next;
      if (b==NULL) return 0;
      Py_INCREF(b);
      PER_ALLOW_DEACTIVATION((*bucket));
      ASSIGNB((*bucket), b);
      UNLESS (PER_USE(*bucket)) return -1;
      *offset = 0;
    }
}

static int 
lastBucketOffset(Bucket **bucket, int *offset, Bucket *firstbucket, int i)
{
  Bucket *b;

  *offset = (*bucket)->len - 1;
  while ((*bucket)->len < 1)
    {
      b=PreviousBucket((*bucket), firstbucket, i);
      if (b==NULL) return 0;
      PER_ALLOW_DEACTIVATION((*bucket));
      ASSIGNB((*bucket), b);
      UNLESS (PER_USE(*bucket)) return -1;
      *offset = (*bucket)->len - 1;
    }
}

/*
** BTreeItems_seek
**
** Find the ith position in the BTreeItems.  Pseudoindex is used to
** determine motion relative to the current bucket.
**
** Arguments:  	self		The BTree
**		i		the index to seek to, positive for a forward
**				index (0..n) or negative (-m..-1) (m=n+1)
**
**
** Returns 0 if successful, -1 on failure to seek
*/
static int
BTreeItems_seek(BTreeItems *self, int i) 
{
  int delta, pseudoindex, currentoffset;
  Bucket *b, *currentbucket;

  pseudoindex=self->pseudoindex;
  currentbucket=self->currentbucket;
  Py_INCREF(currentbucket);
  currentoffset=self->currentoffset;

  /* Make sure that the index and psuedoindex have the same sign */
  if (pseudoindex < 0 && i >=0) 
    {                                
      /* Position to the start of the sequence. */
      ASSIGNB(currentbucket, self->firstbucket);
      Py_INCREF(currentbucket);
      currentoffset = self->first;

      UNLESS (PER_USE(currentbucket)) goto err;

      /* We need to be careful that we have a valid offset! */
      if (currentoffset >= currentbucket->len)
        {
          switch (firstBucketOffset(&currentbucket, &currentoffset))
            {
            case 0: goto no_match;
            case -1: goto err;
            }
        }
      pseudoindex = 0;
    } 
  else if (self->pseudoindex >= 0 && i < 0) 
    {
      /* Position to the end of the sequence. */
      ASSIGNBC(currentbucket, self->lastbucket);
      currentoffset = self->last;
      UNLESS (PER_USE(currentbucket)) goto err;
      
      /* We need to be careful that we have a valid offset! */
      if (currentoffset >= currentbucket->len)
        {
          switch (lastBucketOffset(&currentbucket, &currentoffset,
                                   self->firstbucket, i))
            {
            case 0: goto no_match;
            case -1: goto err;
            }
        }
      pseudoindex = -1;
    }
  else
    {
      UNLESS (PER_USE(currentbucket)) goto err;
      
      /* We need to be careful that we have a valid offset! */
      if (currentoffset >= currentbucket->len) goto no_match;
    }
                                
  /* Whew, we got here so we have a valid offset! */

  while ((delta = i - pseudoindex) != 0) 
    {
      if (delta < 0) 
        {
          /* First, would we drop below zero? */
          if (pseudoindex >= 0 && pseudoindex + delta < 0) goto no_match;
      
          /* Next, do we have to backup a bucket? */
          if (currentoffset + delta < 0) 
            {
              if (currentbucket == self->firstbucket) goto no_match;
              
              b=PreviousBucket(currentbucket, self->firstbucket, i);
              if (b==NULL) goto no_match;
              
              PER_ALLOW_DEACTIVATION(currentbucket);
              ASSIGNB(currentbucket, b);
              UNLESS (PER_USE(currentbucket)) goto err;

              delta += currentoffset;
              pseudoindex -= currentoffset + 1;

              if ((currentoffset = currentbucket->len - 1) < 0)
                /* We backed into an empty bucket. Fix the psuedo index */
                if (++pseudoindex == 0) goto no_match;
            }
          else 
            {	/* Local adjustment */
              pseudoindex += delta;
              currentoffset += delta;
            }

        if (currentbucket == self->firstbucket &&
            currentoffset < self->first) goto no_match;

        } 
      else if (delta > 0)
        {
          
          /* Simple backwards range check */
          if (pseudoindex < 0 && pseudoindex + delta >= 0) 
            goto no_match;
          
          /* Next, do we go forward a bucket? */
          if (currentoffset + delta >= currentbucket->len) 
            {
              while (1)
                {
                  if ((b=currentbucket->next) == NULL) goto no_match;
                  delta -= currentbucket->len - currentoffset;
                  pseudoindex += (currentbucket->len - currentoffset);
                  Py_INCREF(b);
                  PER_ALLOW_DEACTIVATION(currentbucket);
                  ASSIGNB(currentbucket, b);
                  UNLESS (PER_USE(currentbucket)) goto err;
                  currentoffset = 0;
                  if (currentbucket->len) break;
                } 
            }
          else
            {	/* Local adjustment */
              pseudoindex += delta;
              currentoffset += delta;
            }
          if (currentbucket == self->lastbucket &&
              currentoffset > self->last) goto no_match;
          
        }
    }
  
  PER_ALLOW_DEACTIVATION(currentbucket);

  if (currentbucket==self->currentbucket) Py_DECREF(currentbucket);
  else ASSIGNB(self->currentbucket, currentbucket);

  self->pseudoindex=pseudoindex;
  self->currentoffset=currentoffset;
  
  return 0;

 no_match:

  IndexError(i);
  
  PER_ALLOW_DEACTIVATION(currentbucket);

 err:
  Py_XDECREF(currentbucket);
  return -1;
}


/*
** BTreeItems_item
**
** Arguments:	self	a BTreeItems structure
**		i	Which item to inspect
**
** Returns:	the BTreeItems_item_BTree of self->kind, i
**		(ie pulls the ith item out)
*/
static PyObject *
BTreeItems_item(BTreeItems *self, int i)
{
  PyObject *r, *k=0, *v=0;
  
  if (BTreeItems_seek(self, i) < 0) return NULL;

  PER_USE_OR_RETURN(self->currentbucket, NULL);

#ifdef NOVAL

  COPY_KEY_TO_OBJECT(r, self->currentbucket->data[self->currentoffset].key);

#else

  switch(self->kind) {
  case 'k': 
    COPY_KEY_TO_OBJECT(r, self->currentbucket->data[self->currentoffset].key);
    break;
  case 'v': 
    COPY_VALUE_TO_OBJECT(r, 
                         self->currentbucket->data[self->currentoffset].value);
    break;
  default:
    COPY_KEY_TO_OBJECT(k, self->currentbucket->data[self->currentoffset].key);
    UNLESS (k) return NULL;
      
    COPY_VALUE_TO_OBJECT(v,
                         self->currentbucket->data[self->currentoffset].value);
    UNLESS (v) return NULL;

    UNLESS (r=PyTuple_New(2)) goto err;

    PyTuple_SET_ITEM(r, 0, k);
    PyTuple_SET_ITEM(r, 1, v);
  }

#endif

  PER_ALLOW_DEACTIVATION(self->currentbucket);
  return r;

 err:
  Py_DECREF(k);
  Py_XDECREF(v);
  PER_ALLOW_DEACTIVATION(self->currentbucket);
  return NULL;
}

/*
** BTreeItems_slice
**
** Creates a new BTreeItems structure representing the slice
** between the low and high range
**
** Arguments:	self	The old BTreeItems structure
**		ilow	The start index
**		ihigh	The end index
**
** Returns:	BTreeItems item 
*/
static PyObject *
BTreeItems_slice(BTreeItems *self, int ilow, int ihigh)
{
	Bucket *lowbucket;
	Bucket *highbucket;
	int lowoffset;
	int highoffset;


	if (BTreeItems_seek(self, ilow) < 0) return NULL;

	lowbucket = self->currentbucket;
	lowoffset = self->currentoffset;

	if (BTreeItems_seek(self, ihigh) < 0) return NULL;

	highbucket = self->currentbucket;
	highoffset = self->currentoffset;

	return newBTreeItems(
#ifndef NOVAL
                             self->kind, 
#endif
                             lowbucket, lowoffset,
                             highbucket, highoffset);
}

static PySequenceMethods BTreeItems_as_sequence = {
	(inquiry) BTreeItems_length,
	(binaryfunc)0,
	(intargfunc)0,
	(intargfunc) BTreeItems_item,
	(intintargfunc) BTreeItems_slice,
};

/* Number Method items (just for nb_nonzero!) */

static int
BTreeItems_nonzero(BTreeItems *self)
{
  return BTreeItems_length_or_nonzero(self, 1);
}

static PyNumberMethods BTreeItems_as_number_for_nonzero = {
  0,0,0,0,0,0,0,0,0,0,
   (inquiry)BTreeItems_nonzero};

static PyTypeObject BTreeItemsType = {
  PyObject_HEAD_INIT(NULL)
  0,					/*ob_size*/
  PREFIX "BTreeItems",	        /*tp_name*/
  sizeof(BTreeItems),		        /*tp_basicsize*/
  0,					/*tp_itemsize*/
  /* methods */
  (destructor) BTreeItems_dealloc,	/*tp_dealloc*/
  (printfunc)0,				/*tp_print*/
  (getattrfunc)0,			/*obsolete tp_getattr*/
  (setattrfunc)0,			/*obsolete tp_setattr*/
  (cmpfunc)0,				/*tp_compare*/
  (reprfunc)0,				/*tp_repr*/
  &BTreeItems_as_number_for_nonzero,	/*tp_as_number*/
  &BTreeItems_as_sequence,		/*tp_as_sequence*/
  0,					/*tp_as_mapping*/
  (hashfunc)0,				/*tp_hash*/
  (ternaryfunc)0,			/*tp_call*/
  (reprfunc)0,				/*tp_str*/
  0,					/*tp_getattro*/
  0,					/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Sequence type used to iterate over BTree items." /* Documentation string */
};

/************************************************************************/


static void *
PyMalloc(size_t sz)
{
  void *r;

  if (r=malloc(sz)) return r;

  PyErr_NoMemory();
  return NULL;
}

static void *
PyRealloc(void *p, size_t sz)
{
  void *r;

  if (r=realloc(p,sz)) return r;

  PyErr_NoMemory();
  return NULL;
}

static PyObject *
Twople(PyObject *i1, PyObject *i2)
{
  PyObject *t;
  
  if (t=PyTuple_New(2))
    {
      Py_INCREF(i1);
      PyTuple_SET_ITEM(t,0,i1);
      Py_INCREF(i2);
      PyTuple_SET_ITEM(t,1,i2);
    }

  return t;
}

/*
** _bucket_get
**
** Get the bucket item with the matching key
**
** Arguments:	self	The bucket
**		key	The key to match against
**		has_key	Just return object "1" if key found, object "0" if not
**
** Returns:	object	matching object or 0/1 object
*/
static PyObject *
_bucket_get(Bucket *self, PyObject *keyarg
#ifndef NOVAL
            , int has_key
#endif
            )
{
  int min, max, i, l, cmp, copied=1;
  PyObject *r;
  KEY_TYPE key;
  
  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  for (min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key, key);
      if (cmp < 0) min=i;
      else if (cmp == 0)
	{
#ifdef NOVAL
          r=PyInt_FromLong(1);
#else
	  if (has_key) r=PyInt_FromLong(1);
	  else
	    {
              COPY_VALUE_TO_OBJECT(r, self->data[i].value);
	    }
#endif
	  PER_ALLOW_DEACTIVATION(self);
	  return r;
	}
      else max=i;
    }

  PER_ALLOW_DEACTIVATION(self);
#ifdef NOVAL
  return PyInt_FromLong(0);
#else
  if (has_key) return PyInt_FromLong(0);
  PyErr_SetObject(PyExc_KeyError, keyarg);
  return NULL;
#endif
}

/*
** bucket_get
**
** wrapper for _bucket_get
**
** Arguments:	self	The bucket
**		key	the key to match
**
** Returns:	matching object or NULL
*/
static PyObject *
bucket_get(Bucket *self, PyObject *key)
{
  return _bucket_get(self, key
#ifndef NOVAL
                     , 0
#endif
                     );
}

/*
** _BTree_get
**
*/
static PyObject *
_BTree_get(BTree *self, PyObject *keyarg
#ifndef NOVAL
               , int has_key
#endif
               )
{
  int min, max, i, cmp, copied=1;
  PyObject *r;
  KEY_TYPE key;
  
  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return NULL;

  PER_USE_OR_RETURN(self, NULL);

  if (self->len)
    {
      for (min=0, max=self->len, i=max/2; max-min > 1; i=(min+max)/2)
        {
          cmp=TEST_KEY(self->data[i].key, key);
          if (cmp < 0) min=i;
          else if (cmp == 0)
            {
              min=i;
              break;
            }
          else max=i;
        }
      
      if (Bucket_Check(self->data[min].value)) 
        r=_bucket_get(BUCKET(self->data[min].value), keyarg
#ifndef NOVAL
                      , has_key
#endif
                      );
      else
        r=_BTree_get( BTREE(self->data[min].value), keyarg
#ifndef NOVAL
                      , has_key
#endif
                      );
    }
  else
    {  /* No data */
#ifndef NOVAL
      UNLESS (has_key) 
        {
          PyErr_SetObject(PyExc_KeyError, keyarg);
          r=NULL;
        }
      else
#endif
        r=PyInt_FromLong(0);
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;
}    

static PyObject *
BTree_get(BTree *self, PyObject *key)
{
  return _BTree_get(self, key
#ifndef NOVAL
                        , 0
#endif
                        );
}

/*
** _bucket_set
**
** Assign a value into a bucket
**
** Arguments:	self	The bucket
**		key	The key of the object to insert
**		v	The value of the object to insert
**              unique  Inserting a unique key
**
** Returns:	-1 	on error
**		 0	on success with a replacement
**		 1	on success with a new value (growth)
*/
static int
_bucket_set(Bucket *self, PyObject *keyarg, PyObject *v, int unique)
{
  int min, max, i, l, cmp, copied=1;
  Item *d;
  KEY_TYPE key;
  DECLARE_VALUE(value);
  
  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS(copied) return -1;
  COPY_VALUE_FROM_ARG(value, v, &copied);
  UNLESS(copied) return -1;

  PER_USE_OR_RETURN(self, -1);

  for (min=0, max=l=self->len, i=max/2; i != l; l=i, i=(min+max)/2)
    {
      if ((cmp=TEST_KEY(self->data[i].key, key)) < 0) min=i;
      else if (cmp==0)
	{
	  if (v)			/* Assign value to key */
	    {
              if (! unique)
                {
                  DECREF_VALUE(self->data[i].value);
                  COPY_VALUE(self->data[i].value, value);
                  INCREF_VALUE(self->data[i].value);
                  if (PER_CHANGED(self) < 0) goto err;
                }
	      PER_ALLOW_DEACTIVATION(self);
	      return 0;
	    }
	  else			/* There's no value so remove the item */
	    {
	      self->len--;
	      d=self->data+i;
	      DECREF_KEY(d->key);
	      DECREF_VALUE(d->value);
	      if (i < self->len)	
                memmove(d,d+1,sizeof(Item)*(self->len-i));
	      else if (! self->len)
		{
		  self->size=0;
		  free(self->data);
		  self->data=NULL;
		}
	      if (PER_CHANGED(self) < 0) goto err;
	      PER_ALLOW_DEACTIVATION(self);
	      return 1;
	    }
	}
      else max=i;
    }

  if (!v)
    {
      PyErr_SetObject(PyExc_KeyError, keyarg);
      goto err;
    }

  if (self->len==self->size)
    {
      if (self->data)
	{
	  UNLESS (d=PyRealloc(self->data, sizeof(Item)*self->size*2)) goto err;
	  self->data=d;
	  self->size*=2;
	}
      else
	{
	  UNLESS (self->data=PyMalloc(sizeof(Item)*MIN_BUCKET_ALLOC)) goto err;
	  self->size=MIN_BUCKET_ALLOC;
	}
    }
  if (max != i) i++;
  d=self->data+i;
  if (self->len > i) memmove(d+1,d,sizeof(Item)*(self->len-i));

  COPY_KEY(d->key, key);
  INCREF_KEY(d->key);

  COPY_VALUE(d->value, value);
  INCREF_VALUE(d->value);

  self->len++;

  if (PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  return 1;

err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

/*
** bucket_setitem
**
** wrapper for _bucket_setitem (eliminates +1 return code)
**
** Arguments:	self	The bucket
**		key	The key to insert under
**		v	The value to insert
**
** Returns	 0 	on success
**		-1	on failure
*/
static int
bucket_setitem(Bucket *self, PyObject *key, PyObject *v)
{
  if (_bucket_set(self, key, v, 0) < 0) return -1;
  return 0;
}

/*
** bucket_split
**
** Splits one bucket into two
**
** Arguments:	self	The bucket
**		index	the index of the key to split at (O.O.B use midpoint)
**		next	the new bucket to split into
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
bucket_split(Bucket *self, int index, Bucket *next)
{
  if (index < 0 || index >= self->len) index=self->len/2;

  UNLESS (next->data=PyMalloc(sizeof(Item)*(self->len-index))) return -1;
  next->next = self->next;
  Py_INCREF(next);
  self->next = next;
  next->len=self->len-index;
  next->size=next->len;
  memcpy(next->data, self->data+index, sizeof(Item)*next->size);

  self->len=index;

  return 0;
}

/*
** BTree_split
**
** Splits a BTree at a given index
**
** Arguments:	self	The original BTree
**		index	The index to split at (if out of bounds use midpoint)
**		next	The BTree to split into
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
BTree_split(BTree *self, int index, BTree *next)
{
  if (index < 0 || index >= self->len) index=self->len/2;
  
  UNLESS (next->data=PyMalloc(sizeof(BTreeItem)*(self->len-index)))
    return -1;
  next->len=self->len-index;
  next->size=next->len;
  memcpy(next->data, self->data+index, sizeof(BTreeItem)*next->size);
  
  self->len = index;

  if (Bucket_Check(next->data->value)) 
    {
      next->firstbucket = BUCKET(next->data->value);
      Py_INCREF(next->firstbucket);
    }
  else
    {
      PER_USE_OR_RETURN(BTREE(next->data->value), -1);
      next->firstbucket = BTREE(next->data->value)->firstbucket;
      Py_INCREF(self->firstbucket);
      PER_ALLOW_DEACTIVATION(BTREE(next->data->value));
    }
  
  return 0;
}

/*
** BTree_clone
**
** Split a BTree node into two children, leaving the original node the
** parent.
**
** Arguments:	self	The BTree
**
** Returns:	 0	on success
**		-1	on failure
*/
static int
BTree_clone(BTree *self)
{
  /* We've grown really big without anybody splitting us.
     We should split ourselves.
   */
  BTree *n1=0, *n2=0;
  BTreeItem *d=0;
  
  /* Create two BTrees to hold ourselves after split */
  UNLESS (n1=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL))) 
    return -1;
  UNLESS (n2=BTREE(PyObject_CallObject(OBJECT(self->ob_type), NULL))) 
    goto err;

  /* Create a new data buffer to hold two BTrees */
  UNLESS (d=PyMalloc(sizeof(BTreeItem)*2)) goto err;

  /* Split ourself */
  if (BTree_split(self,-1,n2) < 0) goto err;
  
  /* Move our data to new BTree */
  n1->size=self->size;
  n1->len=self->len;
  n1->data=self->data;
  n1->firstbucket = self->firstbucket;
  Py_INCREF(n1->firstbucket);
  
  /* Initialize our data to hold split data */
  self->data=d;
  self->len=2;
  self->size=2;
  self->data->value=OBJECT(n1);
  COPY_KEY(self->data[1].key, n2->data->key);
  INCREF_KEY(self->data[1].key);
  self->data[1].value=OBJECT(n2);

  return 0;

err:
  Py_XDECREF(n1);
  Py_XDECREF(n2);
  free(d);
  return -1;
}

/*
** BTree_grow
**
** Grow a BTree 
**
** Arguments:	self	The BTree
**		index	the index item to insert at
**
** Returns:	 0	on success
**		-1	on failure
*/
static int 
BTree_grow(BTree *self, int index)
{
  int i;
  PyObject *v, *e=0;
  BTreeItem *d;

  if (self->len == self->size)
    {
      if (self->size)
        {
          UNLESS (d=PyRealloc(self->data, sizeof(BTreeItem)*self->size*2))
            return -1;
          self->data=d;
          self->size *= 2;
        }
      else
        {
          UNLESS (d=PyMalloc(sizeof(BTreeItem)*2))
            return -1;
          self->data=d;
          self->size = 2;
        }
    }
     
  d=self->data+index;
  if (self->len)
    {
      v=d->value;
      /* Create a new object of the same type as the target value */
      UNLESS (e=PyObject_CallObject(OBJECT(v->ob_type), NULL)) return -1;

      PER_USE_OR_RETURN(BUCKET(v), -1);


      /* Now split between the original (v) and the new (e) at the midpoint*/
      if (Bucket_Check(v))
        {
          i=bucket_split(BUCKET(v), -1, BUCKET(e));
        }
      else
        {
          i=BTree_split(  BTREE(v), -1,   BTREE(e));
        }

      PER_ALLOW_DEACTIVATION(BUCKET(v));

      if (i < 0)
        {
          Py_DECREF(e);
          return -1;
        }

      index++;
      d++;
      if (self->len > index)	/* Shift up the old values one array slot */
        memmove(d+1, d, sizeof(BTreeItem)*(self->len-index));

      if (Bucket_Check(v))
        {
          COPY_KEY(d->key, BUCKET(e)->data->key);
        }
      else
        {
          COPY_KEY(d->key, BTREE(e)->data->key);
        }
      INCREF_KEY(d->key);
      d->value=e;

      self->len++;

      if (self->len >= MAX_BTREE_SIZE(self) * 2) return BTree_clone(self);
    }
  else
    {
      /* Create a new object of the same type as the target value */
      UNLESS (d->value=PyObject_CallObject(OBJECT(&BucketType), NULL))
        return -1;
      self->len=1;
      Py_INCREF(d->value);
      self->firstbucket = BUCKET(d->value);
    }     
  
  return 0;
}

static int
Bucket_nextBucket(Bucket *self, Bucket **r)
{
  PER_USE_OR_RETURN(self, -1);
  *r=self->next;
  Py_XINCREF(*r);
  PER_ALLOW_DEACTIVATION(self);
  return 0;
}

static int 
Bucket_deleteNextBucket(Bucket *self)
{
  PER_USE_OR_RETURN(self, -1);
  if (self->next)
    {
      Bucket *n;
      if (Bucket_nextBucket(self->next, &n) < 0) goto err;
      ASSIGNB(self->next, n);
      PER_CHANGED(self);
    }
  PER_ALLOW_DEACTIVATION(self);
  return 0;
 err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

static Bucket *
BTree_lastBucket(BTree *self) 
{
  PyObject *o;

  UNLESS (self->data && self->len) 
    {
      IndexError(-1); /*XXX*/
      return NULL;
    }

  o=self->data[self->len - 1].value;
  Py_INCREF(o);

  if (Bucket_Check(o)) return BUCKET(o);

  self=BTREE(o);

  PER_USE_OR_RETURN(self, NULL);
  ASSIGN(o, OBJECT(BTree_lastBucket(self)));
  PER_ALLOW_DEACTIVATION(self);
  
  return BUCKET(o);
}

static int
BTree_deleteNextBucket(BTree *self)
{
  Bucket *b;

  PER_USE_OR_RETURN(self, -1);

  UNLESS (b=BTree_lastBucket(self)) goto err;
  if (Bucket_deleteNextBucket(b) < 0) goto err;
  
  return 0;

 err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

/*
** _BTree_set
**
** inserts a key/value pair into the tree
**
** Arguments:	self	The BTree
**		key	The key of the item to insert
**		value	The object to insert
**              unique  We are inserting a unique key
**
** Returns:	-1	on failure
**		 0	on successful replacement
**		 1 	on successful insert with growth
*/
static int
_BTree_set(BTree *self, PyObject *keyarg, PyObject *value, int unique)
{
  int i, min, max, cmp, grew, copied=1;
  BTreeItem *d;
  KEY_TYPE key;

  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return -1;

  PER_USE_OR_RETURN(self, -1);

  UNLESS (self->len)
    {
      if (value) 
        {
          if (BTree_grow(self, 0) < 0) return -1;
        }
      else 
        {
          PyErr_SetObject(PyExc_KeyError, keyarg);
          return -1;
        }
    }

  /* Binary search to find insertion point */
  for (min=0, max=self->len, i=max/2; max-min > 1; i=(max+min)/2)
    {
      d=self->data+i;
      cmp=TEST_KEY(d->key, key);
      if (cmp < 0) min=i;
      else if (cmp==0)
	{
	  min=i;
	  break;
	}
      else max=i;
    }

  d=self->data+min;
  if (Bucket_Check(d->value))
    grew=_bucket_set(BUCKET(d->value), keyarg, value, unique);
  else
    grew= _BTree_set( BTREE(d->value), keyarg, value, unique);
  if (grew < 0) goto err;

  if (grew)
    {
      if (value)			/* got bigger */
	{
          if ((BUCKET(d->value)->len > MAX_SIZE(d->value))
              && BTree_grow(self,min) < 0) 
            goto err;
	}
      else			/* got smaller */
	{
          if (BUCKET(d->value)->len == 0)
            {
              if (min)
                {
                  /* Not the first subtree, we can delete it because
                     we have the previous subtree handy. 
                  */
                  if (Bucket_Check(d->value))
                    {
                      if (Bucket_deleteNextBucket(BUCKET(d[-1].value)) < 0)
                        goto err;
                    }
                  else
                    {
                      if (0 && BTree_deleteNextBucket(BTREE(d[-1].value)) < 0)
                        goto err;
                    }
                  self->len--;
                  Py_DECREF(d->value);
                  DECREF_KEY(d->key);
                  if (min < self->len)
                    memmove(d, d+1, (self->len-min)*sizeof(BTreeItem));
                }

              if (self->len==1 && BUCKET(self->data->value)->len == 0)
                {
                  /* Our last subtree is empty, woo hoo, we can delete it! */
                  Py_DECREF(self->data->value);

                  /* Ah hah! I bet you are wondering why we don't
                     decref the first key.  We don't decref it because
                     we don't initialize it in the first place. So
                     there! 
                  
                  DECREF_KEY(self->data->key);
                  */
                  self->len=0;
                  Py_DECREF(self->firstbucket);
                  self->firstbucket=NULL;
                }
            }
        }
      if (PER_CHANGED(self) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return grew;

err:
  PER_ALLOW_DEACTIVATION(self);
  return -1;
}

/*
** BTree_setitem
**
** wrapper for _BTree_set
**
** Arguments:	self	The BTree
**		key	The key to insert
**		v	The value to insert
**
** Returns	-1	on failure
**		 0	on success
*/
static int
BTree_setitem(BTree *self, PyObject *key, PyObject *v)
{
  if (_BTree_set(self, key, v, 0) < 0) return -1;
  return 0;
}

/*
 Bucket_findRangeEnd -- Find the index of a range endpoint 
 (possibly) contained in a bucket.

 Arguments:	self		The bucket
		key		the key to match against
		low             end flag
                offset          The output offset
	

 If low, return bucket and index of the smallest item >= key,
 otherwise return bucket and index of the largest item <= key.

 Return: 0 -- Not found, 1 -- found, -1 -- error.
*/
static int
Bucket_findRangeEnd(Bucket *self, PyObject *keyarg, int low, int *offset)
{
  int min, max, i, l, cmp, copied=1;
  Bucket *chase;
  Bucket *release = NULL;
  KEY_TYPE key;

  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return -1;

  PER_USE_OR_RETURN(self, -1);

  for (min=0, max=self->len, i=max/2, l=max; i != l; l=i, i=(min+max)/2) 
    {
      cmp=TEST_KEY(self->data[i].key, key);
      if (cmp < 0)
	min=i;
      else if (cmp == 0)
        {
          PER_ALLOW_DEACTIVATION(self);
          *offset=i;
          return 1;
        } 
      else
        max=i;
  }

  /* OK, no matches, pick max or min, depending on whether
     we want an upper or low end.
  */
  if (low) 
    {
      if (max == self->len) i=0;
      else 
        {
          i=1;
          *offset=max;
        }
    }
  else
    {
      if (max == 0) i=0;
      else 
        {
          i=1;
          *offset=min;
        }
    }

  PER_ALLOW_DEACTIVATION(self);

  return i;
}

static PyObject *
Bucket_maxminKey(Bucket *self, PyObject *args, int min)
{
  PyObject *key=0;
  int rc, offset;
  
  if (args && ! PyArg_ParseTuple(args, "|O", &key)) return NULL;
    
  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->data && self->len) goto empty;
  
  /* Find the low range */  
  if (key) 
    {
      if ((rc = Bucket_findRangeEnd(self, key, min, &offset)) <= 0)
        {
          if (rc < 0) return NULL;
          goto empty;
        }
    }
  else if (min) offset = 0;
  else offset = self->len -1;

  COPY_KEY_TO_OBJECT(key, self->data[offset].key);
  PER_ALLOW_DEACTIVATION(self);

  return key;
  
 empty:
  PyErr_SetString(PyExc_ValueError, "empty bucket");

 err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

static PyObject *
Bucket_minKey(Bucket *self, PyObject *args)
{
  return Bucket_maxminKey(self, args, 1);
}

static PyObject *
Bucket_maxKey(Bucket *self, PyObject *args)
{
  return Bucket_maxminKey(self, args, 0);
}

static int 
Bucket_rangeSearch(Bucket *self, PyObject *args, int *low, int *high)
{
  PyObject *f=0, *l=0;
  int rc;
  
  if (args && ! PyArg_ParseTuple(args,"|OO",&f, &l)) return -1;
    
  UNLESS (self->data && self->len) goto empty;
  
  /* Find the low range */  
  if (f && f != Py_None) 
    {
      UNLESS (rc = Bucket_findRangeEnd(self, f, 1, low))
        {
          if (rc < 0) return -1;
          goto empty;
        }
    } 
  else *low = 0;
  
  /* Find the high range */
  if (l && l != Py_None) 
    {
      UNLESS (rc = Bucket_findRangeEnd(self, l, 0, high))
        {
          if (rc < 0) return -1;
          goto empty;
        } 
    }
  else *high=self->len - 1;

  return 0;

 empty:
  *low=0;
  *high=-1;
  return 0;
}

/*
** bucket_keys
**
** Generate a list of all keys in the bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns:	list of bucket keys
*/  
static PyObject *
bucket_keys(Bucket *self, PyObject *args)
{
  PyObject *r=0, *key;
  int i, low, high;
  
  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      COPY_KEY_TO_OBJECT(key, self->data[i].key);
      if (PyList_SetItem(r, i, key) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(r);
  return NULL;
}

#ifndef NOVAL
/*
** bucket_values
**
** Generate a list of all values in the bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns	list of values
*/
static PyObject *
bucket_values(Bucket *self, PyObject *args)
{
  PyObject *r=0, *v;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      COPY_VALUE_TO_OBJECT(v, self->data[i].value);
      UNLESS (v) goto err;
      if (PyList_SetItem(r, i, v) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(r);
  return NULL;
}

/*
** bucket_items
**
** Returns a list of all items in a bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**		
** Returns:	list of all items in the bucket
*/
static PyObject *
bucket_items(Bucket *self, PyObject *args)
{
  PyObject *r=0, *o=0, *item=0;
  int i, low, high;

  PER_USE_OR_RETURN(self, NULL);

  if (Bucket_rangeSearch(self, args, &low, &high) < 0) goto err;

  UNLESS (r=PyList_New(high-low+1)) goto err;

  for (i=low; i <= high; i++)
    {
      UNLESS (item = PyTuple_New(2)) goto err;

      COPY_KEY_TO_OBJECT(o, self->data[i].key);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 0, o);

      COPY_VALUE_TO_OBJECT(o, self->data[i].value);
      UNLESS (o) goto err;
      PyTuple_SET_ITEM(item, 1, o);
      
      if (PyList_SetItem(r, i, item) < 0) goto err;

      item = 0;
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(r);
  Py_XDECREF(item);
  return NULL;
}
#endif

/*
** bucket__p_deactivate
**
** Reinitialization function for persistence machinery; turns this
** bucket into a ghost (releases contained data)
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns:	None
*/
static PyObject *
bucket__p_deactivate(Bucket *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE)
    {
      int i;
      PyObject *dict;

      for (i=self->len; --i >= 0; )
	{
	  DECREF_KEY(self->data[i].key);
	  DECREF_VALUE(self->data[i].value);
	}
      Py_DECREF(self->next);
      if (HasInstDict(self) && (dict=INSTANCE_DICT(self))) PyDict_Clear(dict);
      self->len=0;
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}

/*
** bucket_clear
**
** Zeros out a bucket
**
** Arguments:	self	The bucket
**		args	(unused)
**
** Returns:	None 	on success
**		NULL	on failure
**
*/  
static PyObject *
bucket_clear(Bucket *self, PyObject *args)
{
  int i;

  PER_USE_OR_RETURN(self, NULL);

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      DECREF_VALUE(self->data[i].value);
    }
  self->len=0;
  if (PER_CHANGED(self) < 0) goto err;
  PER_ALLOW_DEACTIVATION(self);
  RETURN_NONE;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

/*
** _BTree_clear
**
** Clears out all of the values in the BTree
**
** Arguments:	self	The BTree
**
** Returns:	 0	on success
**		-1	on failure
*/  
static int
_BTree_clear(BTree *self)
{
  int i;

  for (i=self->len; --i >= 0; )
    {
      if (i) DECREF_KEY(self->data[i].key);
      Py_DECREF(self->data[i].value);
    }

  Py_XDECREF(self->firstbucket);
  self->firstbucket=NULL;
  self->len=0;

  return 0;
}

/*
** BTree__p_deactivate
**
** Persistance machinery support to turn the object into a ghost
**
** Arguments:	self	The BTree
**		args	(unused)
**
** Returns:	None 	on success
**		NULL	on failure
*/
static PyObject *
BTree__p_deactivate(BTree *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE)
    {
      PyObject *dict;

      if (_BTree_clear(self) < 0) return NULL;
      if (HasInstDict(self) && (dict=INSTANCE_DICT(self))) PyDict_Clear(dict);
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}

/*
** BTree_clear
**
** Wrapper for _BTree_clear
**
** Arguments:	self	the BTree
**		args	(unused)
**
** Returns:	None	on success
**		NULL	on failure
*/
static PyObject *
BTree_clear(BTree *self, PyObject *args)
{
  PER_USE_OR_RETURN(self, NULL);
  if (_BTree_clear(self) < 0) goto err;

  if (PER_CHANGED(self) < 0) goto err;

  PER_ALLOW_DEACTIVATION(self);

  RETURN_NONE;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

/*
** bucket_getstate
**
** bulk get all objects in bucket
**
** Arguments:	self	The Bucket
**		args	(unused)
**
** Returns:	pair of tuples of keys, values
*/
static PyObject *
bucket_getstate(Bucket *self, PyObject *args)
{
  PyObject *r=0, *o=0, *items=0;
  int i, l;

  PER_USE_OR_RETURN(self, NULL);

  l=self->len;

  if (items=PyTuple_New(self->len))
    for (i=0; i<l; i++)
      {
#ifdef NOVAL
        COPY_KEY_TO_OBJECT(r, self->data[i].key);
        UNLESS (r) goto err;
#else
        UNLESS (r = PyTuple_New(2)) goto err;

        COPY_KEY_TO_OBJECT(o, self->data[i].key);
        UNLESS (o) goto err;
        PyTuple_SET_ITEM(r, 0, o);

        COPY_VALUE_TO_OBJECT(o, self->data[i].value);
        UNLESS (o) goto err;
        PyTuple_SET_ITEM(r, 1, o);
#endif
        PyTuple_SET_ITEM(items, i, r);
        r=0;
      }

  if (self->next) 
    r=Py_BuildValue("OO", items, self->next);
  else
    r=Py_BuildValue("(O)", items);

  PER_ALLOW_DEACTIVATION(self);

  return r;

err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(items);
  Py_XDECREF(r);
  return NULL;
}

/*
** bucket_setstate
**
** bulk set of all items in bucket
**
** Arguments:	self	The Bucket
**		args	The object pointng to the two lists of tuples
**
** Returns:	None	on success
**		NULL	on error
*/
static PyObject *
bucket_setstate(Bucket *self, PyObject *args)
{
  PyObject *k, *v, *r, *items;
  Bucket *next=0;
  int i, l, copied=1;
  Item *d;

  PER_PREVENT_DEACTIVATION(self); 

  UNLESS (PyArg_ParseTuple(args, "O", &args)) goto err;

  UNLESS (PyArg_ParseTuple(args, "O|O!", &items, &BucketType, &next)) goto err;

  if ((l=PyTuple_Size(items)) < 0) goto err;

  for (i=self->len, d=self->data; --i >= 0; d++)
    {
      DECREF_KEY(d->key);
      DECREF_VALUE(d->value);
    }
  self->len=0;

  if (self->next)
    {
      Py_DECREF(self->next);
      self->next=0;
    }
  
  if (l > self->size)
    {
      UNLESS (d=PyRealloc(self->data, sizeof(Item)*l)) goto err;
      self->data=d;
      self->size=l;
    }
  
  for (i=0, d=self->data; i<l; i++, d++)
    {
      r=PyTuple_GET_ITEM(items, i);
#ifdef NOVAL
      COPY_KEY_FROM_ARG(d->key, r, &copied);
      UNLESS (copied) return NULL;
#else
      UNLESS(k=PyTuple_GetItem(r, 0)) goto perr;
      UNLESS(v=PyTuple_GetItem(r, 1)) goto perr;
      COPY_KEY_FROM_ARG(d->key, k, &copied);
      UNLESS (copied) return NULL;
      COPY_VALUE_FROM_ARG(d->value, v, &copied);
      UNLESS (copied) return NULL;
#endif
    }

  self->len=l;

  PER_ALLOW_DEACTIVATION(self);
  Py_INCREF(Py_None);
  return Py_None;

 perr:
  self->len=i;
 err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

/*
** bucket_has_key
**
*/
static PyObject *
bucket_has_key(Bucket *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _bucket_get(self, key
#ifndef NOVAL 
                     ,1
#endif                    
                     );
}

#ifndef NOVAL
/*
** bucket_getm
**
*/
static PyObject *
bucket_getm(Bucket *self, PyObject *args)
{
  PyObject *key, *d=Py_None, *r;

  UNLESS (PyArg_ParseTuple(args, "O|O", &key, &d)) return NULL;
  if ((r=_bucket_get(self, key, 0))) return r;
  PyErr_Clear();
  Py_INCREF(d);
  return d;
}
#endif

static struct PyMethodDef Bucket_methods[] = {
  {"__getstate__", (PyCFunction) bucket_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) bucket_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"keys",	(PyCFunction) bucket_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
  {"has_key",	(PyCFunction) bucket_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"clear",	(PyCFunction) bucket_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the bucket"},
  {"maxKey", (PyCFunction) Bucket_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Fine the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) Bucket_minKey,	METH_VARARGS,
   "minKey([key]) -- Fine the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
#ifndef NOVAL
  {"values",	(PyCFunction) bucket_values,	METH_VARARGS,
     "values() -- Return the values"},
  {"items",	(PyCFunction) bucket_items,	METH_VARARGS,
     "items() -- Return the items"},
  {"get",	(PyCFunction) bucket_getm,	METH_VARARGS,
   "get(key[,default]) -- Look up a value\n\n"
   "Return the default (or None) if the key is not found."
  },
#endif
  {"_p_deactivate", (PyCFunction) bucket__p_deactivate, METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
  {NULL,		NULL}		/* sentinel */
};

/*
** BTree_getstate
**
** Get a tuple of all objects in a BTree
**
*/
static PyObject *
BTree_getstate(BTree *self, PyObject *args)
{
  PyObject *r=0, *o, *item;
  PyObject *result;
  int i;

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (r=PyTuple_New(self->len)) goto err;
  for (i=self->len; --i >= 0; )
    {
      UNLESS (item=PyTuple_New(2)) goto err;
      if (i)
        {
          COPY_KEY_TO_OBJECT(o, self->data[i].key);
        }
      else
        {
          o=Py_None;
          Py_INCREF(o);
        }
      PyTuple_SET_ITEM(item, 0, o);
      o=self->data[i].value;
      Py_INCREF(o);
      PyTuple_SET_ITEM(item, 1, o);
      PyTuple_SET_ITEM(r,i,item);
    }

  result = Py_BuildValue("OO", r, self->firstbucket);

  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);

  return result;

err:
  PER_ALLOW_DEACTIVATION(self);
  Py_DECREF(r);
  return NULL;
}

/*
** BTree_setstate
**
** Bulk set all objects in a BTree from a tuple
*/
static PyObject *
BTree_setstate(BTree *self, PyObject *args)
{
  PyObject *state, *k, *v=0, *items;
  BTreeItem *d;
  Bucket *firstbucket;
  int l, i, r, copied=1;

  if (!PyArg_ParseTuple(args,"O",&state)) return NULL;

  if (!PyArg_ParseTuple(state,"O|O!",&items, &BucketType, &firstbucket))
    return NULL;

  if ((l=PyTuple_Size(items)) < 0) return NULL;
 
  PER_PREVENT_DEACTIVATION(self); 
  ASSIGNB(self->firstbucket, firstbucket);

  for (i=self->len, d=self->data; --i >= 0; d++)
    {
      if (d != self->data)
        DECREF_KEY(d->key);
      Py_DECREF(d->value);
    }
  self->len=0;

  if (l > self->size)
    {
      UNLESS (d=PyRealloc(self->data, sizeof(BTreeItem)*l)) goto err;
      self->data=d;
      self->size=l;
    }

  for (i=0, d=self->data; i < l; i++, d++)
    {
      UNLESS (PyArg_ParseTuple(PyTuple_GET_ITEM(state,i), "OO", 
                               &k, &(d->value)))
	goto err;
      if (i) 
        {
          COPY_KEY_FROM_ARG(d->key, k, &copied);
          UNLESS (&copied) return NULL;
          INCREF_KEY(d->key);
        }
      Py_INCREF(d->value);
    }
  self->len=l;

  PER_ALLOW_DEACTIVATION(self);

  Py_INCREF(Py_None);
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}


/*
 BTree_findRangeEnd -- Find one end, expressed as a bucket and
 position, for a range search. Used by BTree_rangeSearch below.

 If low, return bucket and index of the smallest item >= key,
 otherwise return bucket and index of the largest item <= key.

 Return: 0 -- Not found, 1 -- found, -1 -- error.
*/
static int
BTree_findRangeEnd(BTree *self, PyObject *keyarg, int low, 
                   Bucket **bucket, int *offset) {
  int min, max, i=0, cmp, copied=1;
  KEY_TYPE key;

  COPY_KEY_FROM_ARG(key, keyarg, &copied);
  UNLESS (copied) return -1;

  /* We don't need to: PER_USE_OR_RETURN(self, -1);
     because the caller does. */
  
  UNLESS (self->data && self->len) return 0;
  
  for (min=0, max=self->len, i=max/2; max-min > 1; i=(min+max)/2)
    {
      cmp=TEST_KEY(self->data[i].key, key);
      if (cmp < 0) min=i;
      else if (cmp == 0)
	{
	  min=i;
	  break;
	}
      else max=i;
    }

  if (Bucket_Check(self->data[min].value)) 
    {
      *bucket = BUCKET(self->data[min].value);
      if ((i=Bucket_findRangeEnd(*bucket, keyarg, low, offset)))
        Py_INCREF(*bucket);
    }
  else
    {
      self=BTREE(self->data[min].value);
      PER_USE_OR_RETURN(self, -1);
      i = BTree_findRangeEnd(self, keyarg, low, bucket, offset);
      PER_ALLOW_DEACTIVATION(self);
    }

  return i;
}    

static PyObject *
BTree_maxminKey(BTree *self, PyObject *args, int min)
{
  PyObject *key=0;
  Bucket *bucket = NULL;
  int offset, rc;
  
  UNLESS (PyArg_ParseTuple(args, "|O", &key)) return NULL;
  
  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->data && self->len) goto empty;
  
  /* Find the  range */
  
  if (key) 
    {
      if ((rc = BTree_findRangeEnd(self, key, min, &bucket, &offset)) <= 0)
        {
          if (rc < 0) goto err;
          goto empty;
        } 
      PER_ALLOW_DEACTIVATION(self);
      PER_USE_OR_RETURN(bucket, NULL);
    }
  else if (min)
    {
      bucket = self->firstbucket;
      Py_INCREF(bucket);
      PER_ALLOW_DEACTIVATION(self);
      PER_USE_OR_RETURN(bucket, NULL);
      offset = 0;
      if (offset >= bucket->len)
        {
          switch (firstBucketOffset(&bucket, &offset))
            {
            case 0:  goto empty;
            case -1: goto err;
            }
        }
    }
  else
    {
      bucket = BTree_lastBucket(self);
      PER_ALLOW_DEACTIVATION(self);
      PER_USE_OR_RETURN(bucket, NULL);
      if (bucket->len)
        offset = bucket->len - 1; 
      else
        {
          switch (lastBucketOffset(&bucket, &offset, self->firstbucket, -1))
            {
            case 0:  goto empty;
            case -1: goto err;
            }
        }
    }
  
  COPY_KEY_TO_OBJECT(key, bucket->data[offset].key);
  PER_ALLOW_DEACTIVATION(bucket);
  Py_DECREF(bucket);

  return key;
  
 empty:
  PyErr_SetString(PyExc_ValueError, "empty tree");

 err:
  PER_ALLOW_DEACTIVATION(self);
  if (bucket)  
    {
      PER_ALLOW_DEACTIVATION(bucket);
      Py_DECREF(bucket);
    }
  return NULL;
}

static PyObject *
BTree_minKey(BTree *self, PyObject *args)
{
  return BTree_maxminKey(self, args, 1);
}

static PyObject *
BTree_maxKey(BTree *self, PyObject *args)
{
  return BTree_maxminKey(self, args, 0);
}

/*
** BTree_rangeSearch
**
** Generates a BTreeItems object based on the two indexes passed in,
** being the range between them.
**
*/
static PyObject *
BTree_rangeSearch(BTree *self, PyObject *args
#ifndef NOVAL
                      , char type
#endif
                      )
{
  PyObject *f=0, *l=0;
  int rc;
  Bucket *lowbucket = NULL;
  Bucket *highbucket = NULL;
  int lowoffset;
  int highoffset;
  
  UNLESS (PyArg_ParseTuple(args,"|OO",&f, &l)) return NULL;
  
  PER_USE_OR_RETURN(self, NULL);

  UNLESS (self->data && self->len) goto empty;

  /* Find the low range */
  
  if (f && f != Py_None) 
    {
      if ((rc = BTree_findRangeEnd(self, f, 1, &lowbucket, &lowoffset)) <= 0)
        {
          if (rc < 0) goto err;
          goto empty;
        }
    } 
  else 
    {
      lowbucket = self->firstbucket;
      Py_INCREF(lowbucket);
      lowoffset = 0;
    }
  
  /* Find the high range */
  
  if (l && l != Py_None) 
    {
      if ((rc = BTree_findRangeEnd(self, l, 0, &highbucket, &highoffset)) <= 0)
        {
          Py_DECREF(lowbucket);
          if (rc < 0) goto err;
          goto empty;
        } 
    }
  else 
    {
      highbucket = BTree_lastBucket(self);
      highoffset = highbucket->len - 1; 
    }
  
  PER_ALLOW_DEACTIVATION(self);
  
  f=newBTreeItems(
#ifndef NOVAL
                  type, 
#endif
                  lowbucket, lowoffset, highbucket, highoffset);
  Py_DECREF(lowbucket);
  Py_DECREF(highbucket);
  return f;
  
 err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;

 empty:
  PER_ALLOW_DEACTIVATION(self);
  return PyTuple_New(0);
}

/*
** BTree_keys
*/
static PyObject *
BTree_keys(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args
#ifndef NOVAL
                               ,'k'
#endif
                               );
}

#ifndef NOVAL
/*
** BTree_values
*/
static PyObject *
BTree_values(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args,'v');
}

/*
** BTree_items
*/
static PyObject *
BTree_items(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args,'i');
}

/*
** BTree_getm
*/
static PyObject *
BTree_getm(BTree *self, PyObject *args)
{
  PyObject *key, *d=Py_None, *r;

  UNLESS (PyArg_ParseTuple(args, "O|O", &key, &d)) return NULL;
  if ((r=_BTree_get(self, key, 0))) return r;
  PyErr_Clear();
  Py_INCREF(d);
  return d;
}

#endif

/*
** BTree_has_key
*/
static PyObject *
BTree_has_key(BTree *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args,"O",&key)) return NULL;
  return _BTree_get(self, key
#ifndef NOVAL
                        , 1
#endif
                        );
}

static PyObject *
BTree_addUnique(BTree *self, PyObject *args)
{
  int grew;
  PyObject *key, *v;

#ifdef NOVAL
  UNLESS (PyArg_ParseTuple(args, "O", &key)) return NULL;
  v=Py_None;
#else
  UNLESS (PyArg_ParseTuple(args, "OO", &key, &v)) return NULL;
#endif

  if ((grew=_BTree_set(self, key, v, 1)) < 0) return NULL;
  return PyInt_FromLong(grew);
}


static struct PyMethodDef BTree_methods[] = {
  {"__getstate__", (PyCFunction) BTree_getstate,	METH_VARARGS,
   "__getstate__() -- Return the picklable state of the object"},
  {"__setstate__", (PyCFunction) BTree_setstate,	METH_VARARGS,
   "__setstate__() -- Set the state of the object"},
  {"has_key",	(PyCFunction) BTree_has_key,	METH_VARARGS,
     "has_key(key) -- Test whether the bucket contains the given key"},
  {"keys",	(PyCFunction) BTree_keys,	METH_VARARGS,
     "keys() -- Return the keys"},
#ifndef NOVAL
  {"values",	(PyCFunction) BTree_values,	METH_VARARGS,
     "values() -- Return the values"},
  {"items",	(PyCFunction) BTree_items,	METH_VARARGS,
     "items() -- Return the items"},
  {"get",	(PyCFunction) BTree_getm,	METH_VARARGS,
   "get(key[,default]) -- Look up a value\n\n"
   "Return the default (or None) if the key is not found."
  },
  {"maxKey", (PyCFunction) BTree_maxKey,	METH_VARARGS,
   "maxKey([key]) -- Fine the maximum key\n\n"
   "If an argument is given, find the maximum <= the argument"},
  {"minKey", (PyCFunction) BTree_minKey,	METH_VARARGS,
   "minKey([key]) -- Fine the minimum key\n\n"
   "If an argument is given, find the minimum >= the argument"},
#endif
  {"clear",	(PyCFunction) BTree_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the BTree"},  
  {"addUnique", (PyCFunction)BTree_addUnique, METH_VARARGS,
   "addUnique(key"
#ifndef NOVAL
   ", value"
#endif
   ") -- Add an item if the key is not already used.\n\n"
   "Return 1 if the item was added, or 0 otherwise"
  },
  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
  {NULL,		NULL}		/* sentinel */
};

static void
Bucket_dealloc(Bucket *self)
{
  int i;

  for (i=self->len; --i >= 0; )
    {
      DECREF_KEY(self->data[i].key);
      DECREF_VALUE(self->data[i].value);
    }
  free(self->data);
  PER_DEL(self);

  Py_DECREF(self->ob_type);
  PyMem_DEL(self);
}

static void
BTree_dealloc(BTree *self)
{
  int i;

  for (i=self->len; --i >= 0; )
    {
      if (i) DECREF_KEY(self->data[i].key);
      Py_DECREF(self->data[i].value);
    }
  if (self->data) free(self->data);

  Py_XDECREF(self->firstbucket);

  PER_DEL(self);

  Py_DECREF(self->ob_type);
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
BTree_length_or_nonzero(BTree *self, int nonzero)
{
  int c=0;
  Bucket *b, *n;
  
  PER_USE_OR_RETURN(self, -1); 
  b = self->firstbucket;
  Py_XINCREF(b);
  PER_ALLOW_DEACTIVATION(self);

  while (b != NULL) 
    {
      PER_USE_OR_RETURN(b, -1); 
      c += b->len;
      if (nonzero && c)
        {
          /* Short-circuit if all we care about is nonempty */
          PER_ALLOW_DEACTIVATION(b);
          Py_DECREF(b);
          return 1;
        }
      n = b->next;
      Py_XINCREF(n);
      PER_ALLOW_DEACTIVATION(b);
      ASSIGNB(b, n);
    }

  return c;
}

static int
BTree_length( BTree *self)
{
  return BTree_length_or_nonzero(self, 0);
}

static PyMappingMethods BTree_as_mapping = {
  (inquiry)BTree_length,		/*mp_length*/
  (binaryfunc)BTree_get,		/*mp_subscript*/
  (objobjargproc)BTree_setitem,	        /*mp_ass_subscript*/
};

static int
BTree_nonzero( BTree *self)
{
  return BTree_length_or_nonzero(self, 1);
}

static PyNumberMethods BTree_as_number_for_nonzero = {
  0,0,0,0,0,0,0,0,0,0,
  (inquiry)BTree_nonzero};

static PyObject *
bucket_repr(Bucket *self)
{
  static PyObject *format;
  PyObject *r, *t;

  UNLESS (format) UNLESS (format=PyString_FromString(PREFIX "Bucket(%s)")) 
    return NULL;
  UNLESS (t=PyTuple_New(1)) return NULL;
#ifdef NOVAL
  UNLESS (r=bucket_keys(self,NULL)) goto err;
#else
  UNLESS (r=bucket_items(self,NULL)) goto err;
#endif
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
  PREFIX "Bucket",			/*tp_name*/
  sizeof(Bucket),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /*********** methods ***********************/
  (destructor) Bucket_dealloc,	/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc) bucket_repr,	/*tp_repr*/
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
  EXTENSIONCLASS_BASICNEW_FLAG | PERSISTENT_TYPE_FLAG,
};

static PyExtensionClass BTreeType = {
  PyObject_HEAD_INIT(NULL)
  0,				/*ob_size*/
  PREFIX "BTree",			/*tp_name*/
  sizeof(BTree),		/*tp_basicsize*/
  0,				/*tp_itemsize*/
  /************* methods ********************/
  (destructor) BTree_dealloc,/*tp_dealloc*/
  (printfunc)0,			/*tp_print*/
  (getattrfunc)0,		/*obsolete tp_getattr*/
  (setattrfunc)0,		/*obsolete tp_setattr*/
  (cmpfunc)0,			/*tp_compare*/
  (reprfunc)0,			/*tp_repr*/
  &BTree_as_number_for_nonzero,	/*tp_as_number*/
  0,				/*tp_as_sequence*/
  &BTree_as_mapping,	/*tp_as_mapping*/
  (hashfunc)0,			/*tp_hash*/
  (ternaryfunc)0,		/*tp_call*/
  (reprfunc)0,			/*tp_str*/
  (getattrofunc)0,
  0,				/*tp_setattro*/
  
  /* Space for future expansion */
  0L,0L,
  "Mapping type implemented as sorted list of items", 
  METHOD_CHAIN(BTree_methods),
  EXTENSIONCLASS_BASICNEW_FLAG | PERSISTENT_TYPE_FLAG,
};

static struct PyMethodDef module_methods[] = {
  {NULL,		NULL}		/* sentinel */
};

void 
INITMODULE ()
{
  PyObject *m, *d;

  UNLESS (PyExtensionClassCAPI=PyCObject_Import("ExtensionClass","CAPI"))
      return;

  if (cPersistenceCAPI=PyCObject_Import("cPersistence","CAPI"))
    {
	BucketType.methods.link=cPersistenceCAPI->methods;
	BucketType.tp_getattro=cPersistenceCAPI->getattro;
	BucketType.tp_setattro=cPersistenceCAPI->setattro;

	BTreeType.methods.link=cPersistenceCAPI->methods;
	BTreeType.tp_getattro=cPersistenceCAPI->getattro;
	BTreeType.tp_setattro=cPersistenceCAPI->setattro;
    }
  else return;

  BTreeItemsType.ob_type=&PyType_Type;

  /* Create the module and add the functions */
  m = Py_InitModule4(PREFIX "BTree", module_methods,
		     BTree_module_documentation,
		     (PyObject*)NULL,PYTHON_API_VERSION);

  /* Add some symbolic constants to the module */
  d = PyModule_GetDict(m);

  PyDict_SetItemString(d, "__version__",
		       PyString_FromString("$Revision: 1.3 $"));

  PyExtensionClass_Export(d,PREFIX "Bucket",BucketType);
  PyExtensionClass_Export(d,PREFIX "BTree",BTreeType);
 
  /* Check for errors */
  if (PyErr_Occurred())
    Py_FatalError("can't initialize module " PREFIX "BTree");
}
