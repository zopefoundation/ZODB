/*****************************************************************************

  Copyright (c) 2001, 2002 Zope Corporation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#define BTREEITEMSTEMPLATE_C "$Id: BTreeItemsTemplate.c,v 1.10 2002/06/09 17:19:05 tim_one Exp $\n"

typedef struct {
  PyObject_HEAD
  Bucket *firstbucket;			/* First bucket known		*/
  Bucket *currentbucket;		/* Current bucket position	*/
  Bucket *lastbucket;			/* Last bucket position		*/
  int currentoffset;			/* Start count of current bucket*/
  int pseudoindex;			/* Its an indicator		*/
  int first, last;
  char kind;
} BTreeItems;

#define ITEMS(O)((BTreeItems*)(O))

static PyObject *
newBTreeItems(char kind,
              Bucket *lowbucket, int lowoffset,
              Bucket *highbucket, int highoffset);

static void
BTreeItems_dealloc(BTreeItems *self)
{
  Py_XDECREF(self->firstbucket);
  Py_XDECREF(self->lastbucket);
  Py_XDECREF(self->currentbucket);
  PyObject_DEL(self);
}

static int
BTreeItems_length_or_nonzero(BTreeItems *self, int nonzero)
{
  int r;
  Bucket *b, *next;

  b=self->firstbucket;
  UNLESS(b) return 0;

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
      PER_ACCESSED(b);
      Py_DECREF(b);
      b=next;
      PER_USE_OR_RETURN(b, -1);
    }
  PER_ALLOW_DEACTIVATION(b);
  PER_ACCESSED(b);
  Py_DECREF(b);

  return r >= 0 ? r : 0;
}

static int
BTreeItems_length( BTreeItems *self)
{
  return BTreeItems_length_or_nonzero(self, 0);
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

  currentbucket=self->currentbucket;
  UNLESS(currentbucket)
    {
      IndexError(i);
      return -1;
    }

  pseudoindex=self->pseudoindex;
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

  delta = i - pseudoindex;
  if (delta)
    while (delta)
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
                PER_ACCESSED(currentbucket);
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
                    if (currentbucket == self->lastbucket) goto no_match;

                    if ((b=currentbucket->next) == NULL) goto no_match;
                    delta -= currentbucket->len - currentoffset;
                    pseudoindex += (currentbucket->len - currentoffset);
                    Py_INCREF(b);
                    PER_ALLOW_DEACTIVATION(currentbucket);
                    PER_ACCESSED(currentbucket);
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

        delta = i - pseudoindex;
      }
  else
    {                           /* Sanity check current bucket/offset */
      if (currentbucket == self->firstbucket &&currentoffset < self->first)
        goto no_match;
      if (currentbucket == self->lastbucket && currentoffset > self->last)
        goto no_match;
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

  switch(self->kind) {

  case 'v':
    COPY_VALUE_TO_OBJECT(r, self->currentbucket->values[self->currentoffset]);
    break;

  case 'i':
    COPY_KEY_TO_OBJECT(k, self->currentbucket->keys[self->currentoffset]);
    UNLESS (k) return NULL;

    COPY_VALUE_TO_OBJECT(v, self->currentbucket->values[self->currentoffset]);
    UNLESS (v) return NULL;

    UNLESS (r=PyTuple_New(2)) goto err;

    PyTuple_SET_ITEM(r, 0, k);
    PyTuple_SET_ITEM(r, 1, v);
    break;

  default:
    COPY_KEY_TO_OBJECT(r, self->currentbucket->keys[self->currentoffset]);
    break;
  }

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

  return newBTreeItems(self->kind,
                       lowbucket, lowoffset, highbucket, highoffset);
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
  MOD_NAME_PREFIX "BTreeItems",	        /*tp_name*/
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

static PyObject *
newBTreeItems(char kind,
              Bucket *lowbucket, int lowoffset,
              Bucket *highbucket, int highoffset)
{
  BTreeItems *self;

  UNLESS (self = PyObject_NEW(BTreeItems, &BTreeItemsType)) return NULL;
  self->kind=kind;

  self->first=lowoffset;
  self->last=highoffset;

  if (! lowbucket || (lowbucket==highbucket && lowoffset > highoffset))
    {
      self->firstbucket   = 0;
      self->lastbucket    = 0;
      self->currentbucket = 0;
    }
  else
    {
      Py_INCREF(lowbucket);
      self->firstbucket = lowbucket;
      Py_XINCREF(highbucket);
      self->lastbucket = highbucket;
      Py_XINCREF(lowbucket);
      self->currentbucket = lowbucket;
    }

  self->currentoffset = lowoffset;
  self->pseudoindex = 0;

  return OBJECT(self);
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
  return 0;
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
  return 0;
}
