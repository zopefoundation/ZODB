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

/****************************************************************************
 Set operations
 ****************************************************************************/

typedef struct SetIteration_s 
{
  PyObject *set;
  int position;
  int hasValue;
  KEY_TYPE key;
  VALUE_TYPE value;
  int (*next)(struct SetIteration_s*);
} SetIteration;

static int 
nextBucket(SetIteration *i)
{
  UNLESS(PER_USE(BUCKET(i->set))) return -1;
          
  if (i->position >= 0)
    {
      if (i->position)
        {
          DECREF_KEY(i->key);
          DECREF_VALUE(i->value);
        }

      if (i->position < BUCKET(i->set)->len)
        {
          COPY_KEY(i->key, BUCKET(i->set)->keys[i->position]);
          INCREF_KEY(i->key);
          COPY_VALUE(i->value, BUCKET(i->set)->values[i->position]);
          INCREF_VALUE(i->value);
          i->position ++;
        }
      else
        i->position = -1;
    }

  PER_ALLOW_DEACTIVATION(BUCKET(i->set));
          
  return 0;
}

static int 
nextSet(SetIteration *i)
{
  UNLESS(PER_USE(BUCKET(i->set))) return -1;
          
  if (i->position >= 0)
    {
      if (i->position)
        {
          DECREF_KEY(i->key);
        }

      if (i->position < BUCKET(i->set)->len)
        {
          COPY_KEY(i->key, BUCKET(i->set)->keys[i->position]);
          INCREF_KEY(i->key);
          i->position ++;
        }
      else
        i->position = -1;
    }

  PER_ALLOW_DEACTIVATION(BUCKET(i->set));
          
  return 0;
}

static int 
nextBTreeItems(SetIteration *i)
{
  if (i->position >= 0)
    {
      if (i->position)
        {
          DECREF_KEY(i->key);
          DECREF_VALUE(i->value);
        }
      
      if (BTreeItems_seek(ITEMS(i->set), i->position) >= 0)
        {
          Bucket *currentbucket;

          currentbucket = BUCKET(ITEMS(i->set)->currentbucket);

          UNLESS(PER_USE(currentbucket)) return -1;

          COPY_KEY(i->key, currentbucket->keys[ITEMS(i->set)->currentoffset]);
          INCREF_KEY(i->key);

          COPY_VALUE(i->value, 
                     currentbucket->values[ITEMS(i->set)->currentoffset]);
          COPY_VALUE(i->value, 
                   BUCKET(ITEMS(i->set)->currentbucket)
                   ->values[ITEMS(i->set)->currentoffset]);
          INCREF_VALUE(i->value);

          i->position ++;

          PER_ALLOW_DEACTIVATION(currentbucket);
        }
      else
        {
          i->position = -1;
          PyErr_Clear();
        }
    }
}

static int 
nextTreeSetItems(SetIteration *i)
{
  if (i->position >= 0)
    {
      if (i->position)
        {
          DECREF_KEY(i->key);
        }
      
      if (BTreeItems_seek(ITEMS(i->set), i->position) >= 0)
        {
          Bucket *currentbucket;

          currentbucket = BUCKET(ITEMS(i->set)->currentbucket);

          UNLESS(PER_USE(currentbucket)) return -1;

          COPY_KEY(i->key, currentbucket->keys[ITEMS(i->set)->currentoffset]);
          INCREF_KEY(i->key);

          i->position ++;

          PER_ALLOW_DEACTIVATION(currentbucket);
        }
      else
        {
          i->position = -1;
          PyErr_Clear();
        }
    }
}

static int
initSetIteration(SetIteration *i, PyObject *s, int w, int *merge)
{
  i->position=0;

  if (ExtensionClassSubclassInstance_Check(s, &BucketType))
    {
      i->set = s;
      Py_INCREF(s);

      if (w > 0) *merge=1;
      i->next=nextBucket;
      i->hasValue=1;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &BTreeType))
    {
      i->set=BTree_rangeSearch(BTREE(s), NULL, 'i');
      UNLESS(i->set) return -1;

      if (w > 0) *merge=1;
      i->next=nextBTreeItems;
      i->hasValue=1;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &SetType))
    {
      i->set = s;
      Py_INCREF(s);

      if (w > 1) *merge=1;
      i->next=nextSet;
      i->hasValue=0;
    }
  else if (ExtensionClassSubclassInstance_Check(s, &TreeSetType))
    {
      i->set=BTree_rangeSearch(BTREE(s), NULL, 'k');
      UNLESS(i->set) return -1;

      if (w > 1) *merge=1;
      i->next=nextTreeSetItems;
      i->hasValue=0;
    }
  else
    {
      PyErr_SetString(PyExc_TypeError, "invalid argument");
      return -1;
    }

  return 0;
}

#ifndef MERGE_WEIGHT
#define MERGE_WEIGHT(O, w) (O)
#endif

static int 
copyRemaining(Bucket *r, SetIteration *i, int merge, int w)
{
  while (i->position >= 0)
    {
      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) return -1;
      COPY_KEY(r->keys[r->len], i->key);
      INCREF_KEY(r->keys[r->len]);

      if (merge)
        {
          COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i->value, w));
          INCREF_VALUE(r->values[r->len]);
        }
      r->len++;
      if (i->next(i) < 0) return -1;
    }

  return 0;
}

static PyObject *
set_operation(PyObject *s1, PyObject *s2, 
              int w1, int w2,
              int c1, int c12, int c2)
{
  Bucket *r=0;
  SetIteration i1 = {0,0,0}, i2 = {0,0,0};
  int cmp, merge=0;

  if (s1==Py_None)
    {
      if (c12 || c2)
        {
          Py_INCREF(s2);
          return s2;
        }
      Py_INCREF(s1);
      return s1;
    }
  else if (s2==Py_None)
    {
      if (c12 || c1)
        {
          Py_INCREF(s1);
          return s1;
        }
      Py_INCREF(s2);
      return s2;
    }

  if (initSetIteration(&i1, s1, w1, &merge) < 0) return NULL;
  if (initSetIteration(&i2, s2, w2, &merge) < 0) return NULL;

  if (merge)
    {
#ifndef MERGE
      if (c12 && i1.hasValue && i2.hasValue) goto invalid_set_operation;
#endif
      if (! i1.hasValue && i2.hasValue)
        {
          SetIteration t;
          int i;

          t=i1; i1=i2; i2=t;
          i=c1; c1=c2; c2=i;
          i=w1; w1=w2; w2=i;
        }
#ifdef MERGE_DEFAULT
      i1.value=MERGE_DEFAULT;
      i2.value=MERGE_DEFAULT;
#else
      if (i1.hasValue)
        {
          if (! i2.hasValue && c2) goto invalid_set_operation;
        }
      else
        {
          if (c1 || c12) goto invalid_set_operation;
        }
#endif

      UNLESS(r=BUCKET(PyObject_CallObject(OBJECT(&BucketType), NULL)))
        goto err;
    }
  else
    {
      UNLESS(r=BUCKET(PyObject_CallObject(OBJECT(&SetType), NULL)))
        goto err;
    }

  if (i1.next(&i1) < 0) return NULL;
  if (i2.next(&i2) < 0) return NULL;

  while (i1.position >= 0 && i2.position >= 0)
    {
      cmp=TEST_KEY(i1.key, i2.key);
      if(cmp < 0)
	{
	  if(c1)
	    {
	      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) goto err;
              COPY_KEY(r->keys[r->len], i1.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
                  COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i1.value, w1));
                  INCREF_VALUE(r->values[r->len]);
                }
	      r->len++;
	    }
          if (i1.next(&i1) < 0) goto err;
	}
      else if(cmp==0)
	{
	  if(c12)
	    {
	      if(r->len >= r->size && Bucket_grow(r, ! merge) < 0) goto err;
              COPY_KEY(r->keys[r->len], i1.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
#ifdef MERGE
                  r->values[r->len] = MERGE(i1.value, w1, i2.value, w2);
#else
                  COPY_VALUE(r->values[r->len], i1.value);
                  INCREF_VALUE(r->values[r->len]);
#endif                    
                }
	      r->len++;
	    }
          if (i1.next(&i1) < 0) goto err;
          if (i2.next(&i2) < 0) goto err;
	}
      else
	{
	  if(c2)
	    {
	      if(r->len >= r->size && Bucket_grow(r,1) < 0) goto err;
              COPY_KEY(r->keys[r->len], i2.key);
              INCREF_KEY(r->keys[r->len]);
              if (merge)
                {
                  COPY_VALUE(r->values[r->len], MERGE_WEIGHT(i2.value, w2));
                  INCREF_VALUE(r->values[r->len]);
                }
	      r->len++;
	    }
          if (i2.next(&i2) < 0) goto err;
	}
    }
  if(c1 && copyRemaining(r, &i1, merge, w1) < 0) goto err;
  if(c2 && copyRemaining(r, &i2, merge, w2) < 0) goto err;

  Py_DECREF(i1.set);
  Py_DECREF(i2.set);

  return OBJECT(r);

invalid_set_operation:
  PyErr_SetString(PyExc_TypeError, "invalid set operation");
err:
  Py_XDECREF(i1.set);
  Py_XDECREF(i2.set);
  Py_XDECREF(r);
  return NULL;
}

static PyObject *
union_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;
  int w1=1, w2=1;

  UNLESS(PyArg_ParseTuple(args, "OO|ii", &o1, &o2, &w1, &w2)) return NULL;
  
  return set_operation(o1, o2, w1, w2, 1, 1, 1);
}         

static PyObject *
intersection_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;
  int w1=1, w2=1;

  UNLESS(PyArg_ParseTuple(args, "OO|ii", &o1, &o2, &w1, &w2)) return NULL;
  
  return set_operation(o1, o2, w1, w2, 0, 1, 0);
}         

static PyObject *
difference_m(PyObject *ignored, PyObject *args)
{
  PyObject *o1, *o2;
  int w1=1, w2=1;

  UNLESS(PyArg_ParseTuple(args, "OO|ii", &o1, &o2, &w1, &w2)) return NULL;
  
  return set_operation(o1, o2, w1, w2, 1, 0, 0);
}         
