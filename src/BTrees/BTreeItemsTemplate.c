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

static PyObject *
newBTreeItems(char kind, 
              Bucket *lowbucket, int lowoffset,
              Bucket *highbucket, int highoffset);

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
