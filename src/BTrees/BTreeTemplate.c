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

/*
** _BTree_get
**
*/
static PyObject *
_BTree_get(BTree *self, PyObject *keyarg, int has_key)
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
      
      if (SameType_Check(self, self->data[min].value)) 
        r=_BTree_get( BTREE(self->data[min].value), keyarg, has_key);
      else
        r=_bucket_get(BUCKET(self->data[min].value), keyarg, has_key);
    }
  else
    {  /* No data */
      UNLESS (has_key) 
        {
          PyErr_SetObject(PyExc_KeyError, keyarg);
          r=NULL;
        }
      else
        r=PyInt_FromLong(0);
    }

  PER_ALLOW_DEACTIVATION(self);
  return r;
}    

static PyObject *
BTree_get(BTree *self, PyObject *key)
{
  return _BTree_get(self, key, 0);
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

  if (SameType_Check(self, next->data->value)) 
    {
      PER_USE_OR_RETURN(BTREE(next->data->value), -1);
      next->firstbucket = BTREE(next->data->value)->firstbucket;
      Py_INCREF(self->firstbucket);
      PER_ALLOW_DEACTIVATION(BTREE(next->data->value));
    }
  else
    {
      next->firstbucket = BUCKET(next->data->value);
      Py_INCREF(next->firstbucket);
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
      if (SameType_Check(self, v))
        {
          i=BTree_split(  BTREE(v), -1,   BTREE(e));
        }
      else
        {
          i=bucket_split(BUCKET(v), -1, BUCKET(e));
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

      if (SameType_Check(self, v))
        {
          COPY_KEY(d->key, BTREE(e)->data->key);
        }
      else
        {
          COPY_KEY(d->key, BUCKET(e)->keys[0]);
        }
      INCREF_KEY(d->key);
      d->value=e;

      self->len++;

      if (self->len >= MAX_BTREE_SIZE(self) * 2) return BTree_clone(self);
    }
  else
    {
      UNLESS (d->value=PyObject_CallObject(OBJECT(&BucketType), NULL))
        return -1;
      self->len=1;
      Py_INCREF(d->value);
      self->firstbucket = BUCKET(d->value);
    }     
  
  return 0;
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

  UNLESS (SameType_Check(self, o)) return BUCKET(o);

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
_BTree_set(BTree *self, PyObject *keyarg, PyObject *value, 
           int unique, int noval)
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
  if (SameType_Check(self, d->value))
    grew= _BTree_set( BTREE(d->value), keyarg, value, unique, noval);
  else
    grew=_bucket_set(BUCKET(d->value), keyarg, value, unique, noval);
  if (grew < 0) goto err;

  if (grew)
    {
      if (value)			/* got bigger */
	{
          if (SameType_Check(self, d->value))
            {
              if ( BTREE(d->value)->len > MAX_BTREE_SIZE(d->value) 
                   && BTree_grow(self,min) < 0) 
                goto err;
            }          
          else
            {
              if ( BUCKET(d->value)->len > MAX_BUCKET_SIZE(d->value) 
                   && BTree_grow(self,min) < 0) 
                goto err;
            }          
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
                  if (SameType_Check(self, d->value))
                    {
                      if (0 && BTree_deleteNextBucket(BTREE(d[-1].value)) < 0)
                        goto err;
                    }
                  else
                    {
                      if (Bucket_deleteNextBucket(BUCKET(d[-1].value)) < 0)
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
  if (_BTree_set(self, key, v, 0, 0) < 0) return -1;
  return 0;
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

#ifdef PERSISTENT
static PyObject *
BTree__p_deactivate(BTree *self, PyObject *args)
{
  if (self->state==cPersistent_UPTODATE_STATE)
    {
      if (_BTree_clear(self) < 0) return NULL;
      self->state=cPersistent_GHOST_STATE;
    }

  Py_INCREF(Py_None);
  return Py_None;
}
#endif

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

  if (self->len)
    {
      if (_BTree_clear(self) < 0) goto err;
      if (PER_CHANGED(self) < 0) goto err;
    }

  PER_ALLOW_DEACTIVATION(self);

  Py_INCREF(Py_None); 
  return Py_None;

err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;
}

/*
** BTree_getstate
**
** Get a tuple of all objects in a BTree
**
*/
static PyObject *
BTree_getstate(BTree *self, PyObject *args)
{
  PyObject *r=0, *o, *item, *result;
  int i;

  PER_USE_OR_RETURN(self, NULL);

  if (self->len)
    {
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
      Py_DECREF(r);
    }
  else
    {
      result = Py_None;
      Py_INCREF(result);
    }  
      

  PER_ALLOW_DEACTIVATION(self);

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

 
  PER_PREVENT_DEACTIVATION(self); 

  if (_BTree_clear(self) < 0) goto err;

  if (state != Py_None)
    {

      if (!PyArg_ParseTuple(state,"O|O",&items, &firstbucket))
        goto err;

      if ((l=PyTuple_Size(items)) < 0) goto err;

      self->firstbucket = firstbucket;
      Py_INCREF(firstbucket);

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
    }

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

  if (SameType_Check(self, self->data[min].value)) 
    {
      self=BTREE(self->data[min].value);
      PER_USE_OR_RETURN(self, -1);
      i = BTree_findRangeEnd(self, keyarg, low, bucket, offset);
      PER_ALLOW_DEACTIVATION(self);
    }
  else
    {
      *bucket = BUCKET(self->data[min].value);
      if ((i=Bucket_findRangeEnd(*bucket, keyarg, low, offset)))
        Py_INCREF(*bucket);
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
  
  COPY_KEY_TO_OBJECT(key, bucket->keys[offset]);
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
BTree_rangeSearch(BTree *self, PyObject *args, char type)
{
  PyObject *f=0, *l=0;
  int rc;
  Bucket *lowbucket = NULL;
  Bucket *highbucket = NULL;
  int lowoffset;
  int highoffset;
  
  UNLESS (! args || PyArg_ParseTuple(args,"|OO",&f, &l)) return NULL;
  
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
  
  f=newBTreeItems(type, lowbucket, lowoffset, highbucket, highoffset);
  Py_DECREF(lowbucket);
  Py_DECREF(highbucket);
  return f;
  
 err:
  PER_ALLOW_DEACTIVATION(self);
  return NULL;

 empty:
  PER_ALLOW_DEACTIVATION(self);
  return newBTreeItems(type, 0, 0, 0, 0);
}

/*
** BTree_keys
*/
static PyObject *
BTree_keys(BTree *self, PyObject *args)
{
  return BTree_rangeSearch(self,args, 'k');
}

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

static PyObject *
BTree_byValue(BTree *self, PyObject *args)
{
  PyObject *r=0, *o=0, *item=0, *omin;
  VALUE_TYPE min;
  VALUE_TYPE v;
  int i, l, copied=1;
  SetIteration it={0,0};

  PER_USE_OR_RETURN(self, NULL);

  UNLESS (PyArg_ParseTuple(args, "O", &omin)) return NULL;
  COPY_VALUE_FROM_ARG(min, omin, &copied);
  UNLESS(copied) return NULL;
    
  UNLESS (r=PyList_New(0)) goto err;

  it.set=BTree_rangeSearch(self, NULL, 'i');
  UNLESS(it.set) goto err;

  if (nextBTreeItems(&it) < 0) goto err;

  while (it.position >= 0)
    {
      if (TEST_VALUE(it.value, min) >= 0)
        {      
          UNLESS (item = PyTuple_New(2)) goto err;

          COPY_KEY_TO_OBJECT(o, it.key);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(item, 1, o);

          COPY_VALUE(v, it.value);
          NORMALIZE_VALUE(v, min);
          COPY_VALUE_TO_OBJECT(o, v);
          DECREF_VALUE(v);
          UNLESS (o) goto err;
          PyTuple_SET_ITEM(item, 0, o);
      
          if (PyList_Append(r, item) < 0) goto err;
          Py_DECREF(item);
          item = 0;
        }
      if (nextBTreeItems(&it) < 0) goto err;
    }

  item=PyObject_GetAttr(r,sort_str);
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_CallObject(item, NULL));
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_GetAttr(r, reverse_str));
  UNLESS (item) goto err;
  ASSIGN(item, PyObject_CallObject(item, NULL));
  UNLESS (item) goto err;
  Py_DECREF(item);

  PER_ALLOW_DEACTIVATION(self);
  return r;

 err:
  PER_ALLOW_DEACTIVATION(self);
  Py_XDECREF(r);
  Py_XDECREF(it.set);
  Py_XDECREF(item);
  return NULL;
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

/*
** BTree_has_key
*/
static PyObject *
BTree_has_key(BTree *self, PyObject *args)
{
  PyObject *key;

  UNLESS (PyArg_ParseTuple(args,"O",&key)) return NULL;  return _BTree_get(self, key, 1);
}

static PyObject *
BTree_addUnique(BTree *self, PyObject *args)
{
  int grew;
  PyObject *key, *v;

  UNLESS (PyArg_ParseTuple(args, "OO", &key, &v)) return NULL;

  if ((grew=_BTree_set(self, key, v, 1, 0)) < 0) return NULL;
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
     "keys([min, max]) -- Return the keys"},
  {"values",	(PyCFunction) BTree_values,	METH_VARARGS,
     "values([min, max]) -- Return the values"},
  {"items",	(PyCFunction) BTree_items,	METH_VARARGS,
     "items([min, max]) -- Return the items"},
  {"byValue",	(PyCFunction) BTree_byValue,	METH_VARARGS,
   "byValue(min) -- "
   "Return value-keys with values >= min and reverse sorted by values"
  },
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
  {"clear",	(PyCFunction) BTree_clear,	METH_VARARGS,
   "clear() -- Remove all of the items from the BTree"},  
  {"insert", (PyCFunction)BTree_addUnique, METH_VARARGS,
   "insert(key, value) -- Add an item if the key is not already used.\n\n"
   "Return 1 if the item was added, or 0 otherwise"
  },
#ifdef PERSISTENT
  {"_p_deactivate", (PyCFunction) BTree__p_deactivate,	METH_VARARGS,
   "_p_deactivate() -- Reinitialize from a newly created copy"},
#endif
  {NULL,		NULL}		/* sentinel */
};

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
  EXTENSIONCLASS_BASICNEW_FLAG 
#ifdef PERSISTENT
  | PERSISTENT_TYPE_FLAG 
#endif
  | EXTENSIONCLASS_NOINSTDICT_FLAG,
};
