##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################

import OOBTree, Interface, Interface.Standard

class ICollection(Interface.Base):

    def clear():
        """Remove all of the items from the collection"""

    def __nonzero__():
        """Check if the collection is non-empty.

        Return a true value if the collection is non-empty and a
        false otherwise.
        """

class IReadSequence(Interface.Standard.Sequence):

    def __getslice__(index1, index2):
        """Return a subsequence from the original sequence

        Such that the subsequence includes the items from index1 up
        to, but not including, index2.
        """

class IKeyed(ICollection):

    def has_key(key):
        """Check whether the object has an item with the given key"""

    def keys(min=None, max=None):
        """Return an IReadSequence containing the keys in the collection

        The type of the IReadSequence is not specified. It could be a
        list or a tuple or some other type.

        If a min is specified, then output is constrained to
        items having keys greater than or equal to the given min.
        A min value of None is ignored.

        If a max is specified, then output is constrained to
        items having keys less than or equal to the given min.
        A max value of None is ignored.
        """

    def maxKey(key=None):
        """Return the maximum key

        If a key argument if provided, return the largest key that is
        less than or equal to the argument.
        """

    def minKey(key=None):
        """Return the minimum key

        If a key argument if provided, return the smallest key that is
        greater than or equal to the argument.
        """

class ISetMutable(IKeyed):
    
    def insert(key):
        """Add the key (value) to the set.

        If the key was already in the set, return 0, otherwise return 1.
        """

    def remove(key):
        """Remove the key from the set."""
        
    def update(seq):
        """Add the items from the given sequence to the set"""

class IKeySequence(IKeyed, Interface.Standard.Sized):

    def __getitem__(index):
        """Return the key in the given index position

        This allows iteration with for loops and use in functions,
        like map and list, that read sequences.
        """

class ISet(IKeySequence, ISetMutable):
    pass

class ITreeSet(IKeyed, ISetMutable):
    pass
    

class IDictionaryIsh(IKeyed, Interface.Standard.MinimalDictionary):

    def update(collection):
        """Add the items from the given collection object to the collection

        The input collection must be a sequence of key-value tuples,
        or an object with an 'items' method that returns a sequence of
        key-value tuples.
        """

    def values(min=None, max=None):
        """Return a IReadSequence containing the values in the collection

        The type of the IReadSequence is not specified. It could be a
        list or a tuple or some other type.

        If a min is specified, then output is constrained to
        items having keys greater than or equal to the given min.
        A min value of None is ignored.

        If a max is specified, then output is constrained to
        items having keys less than or equal to the given min.
        A max value of None is ignored.
        """

    def items(min=None, max=None):
        """Return a IReadSequence containing the items in the collection

        An item is a key-value tuple.

        The type of the IReadSequence is not specified. It could be a
        list or a tuple or some other type.

        If a min is specified, then output is constrained to
        items having keys greater than or equal to the given min.
        A min value of None is ignored.

        If a max is specified, then output is constrained to
        items having keys less than or equal to the given min.
        A max value of None is ignored.
        """

    def byValue(minValue):
        """Return a sequence of value-key pairs, sorted by value

        Values < min are ommitted and other values are "normalized" by
        the minimum value. This normalization may be a noop, but, for
        integer values, the normalization is division.
        """
    
class IBTree(IDictionaryIsh):

    def insert(key, value):
        """Insert a key and value into the collection.

        If the key was already in the collection, then there is no
        change and 0 is returned.

        If the key was not already in the collection, then the item is
        added and 1 is returned.

        This method is here to allow one to generate random keys and
        to insert and test whether the key was there in one operation.

        A standard idiom for generating new keys will be::

          key=generate_key()
          while not t.insert(key, value):
              key=generate_key()
        """

class IMerge(Interface.Base):
    """Object with methods for merging sets, buckets, and trees.

    These methods are supplied in modules that define collection
    classes with particular key and value types. The operations apply
    only to collections from the same module.  For example, the
    IIBTree.union can only be used with IIBTree.IIBTree,
    IIBTree.IIBucket, IIBTree.IISet, and IIBTree.IITreeSet.

    The implementing module has a value type. The IOBTree and OOBTree
    modules have object value type. The IIBTree and OIBTree modules
    have integer value tyoes. Other modules may be defined in the
    future that have other value types.

    The individual types are classified into set (Set and TreeSet) and
    mapping (Bucket and BTree) types.
    """

    def difference(c1, c2):
        """Return the keys or items in c1 for which there is no key in
        c2.

        If c1 is None, then None is returned.  If c2 is none, then c1
        is returned.
        """

    def union(c1, c2):
        """Compute the Union of c1 and c2.

        If c1 is None, then c2 is returned, otherwise, if c2 is None,
        then c1 is returned.

        The output is a Set containing keys from the input
        collections.
        """

    def intersection(c1, c2):
        """Compute the Union of c1 and c2.

        If c1 is None, then c2 is returned, otherwise, if c2 is None,
        then c1 is returned.

        The output is a Set containing matching keys from the input
        collections.
        """

class IIMerge(IMerge):
    """Merge collections with integer value type.

    A primary intent is to support operations with no or integer
    values, which are used as "scores" to rate indiviual keys. That
    is, in this context, a BTree or Bucket is viewed as a set with
    scored keys, using integer scores.
    """

    def weightedUnion(c1, c2, weight1=1, weight2=1):
        """Compute the weighted Union of c1 and c2.

        If c1 and c2 are None, the output is 0 and None

        if c1 is None and c2 is not None, the output is weight2 and
        c2.

        if c1 is not None and c2 not None, the output is weight1 and
        c1.

        If c1 and c2 are not None, the output is 1 and a Bucket
        such that the output values are::

          v1*weight1 + v2*weight2

          where:

            v1 is 0 if the key was not in c1. Otherwise, v1 is 1, if
            c1 is a set, or the value from c1.

            v2 is 0 if the key was not in c2. Otherwise, v2 is 2, if
            c2 is a set, or the value from c2.

        Note that c1 and c2 must be collections. None may not be
        passed as one of the collections.
        """

    def weightedIntersection(c1, c2, weight1=1, weight2=1):
        """Compute the weighted intersection of c1 and c2.

        If c1 and c2 are None, the output is None, None.

        if c1 is None and c2 is not None, the output is weight2 and
        c2.

        if c1 is not None and c2 not None, the output is weight1 and
        c1.

        If c1 and c2 are sets, the output is the sum of the weights
        and the (unweighted) intersection of the sets.

        If c1 and c2 are not None and not both sets, the output is 1
        and a Bucket such that the output values are::

          v1*weight1 + v2*weight2

          where:

            v1 is 0 if the key was not in c1. Otherwise, v1 is 1, if
            c1 is a set, or the value from c1.

            v2 is 0 if the key was not in c2. Otherwise, v2 is 2, if
            c2 is a set, or the value from c2.

        Note that c1 and c2 must be collections. None may not be
        passed as one of the collections.
        """

###############################################################
# IMPORTANT NOTE
#
# Getting the length of a BTree, TreeSet, or output of keys,
# values, or items of same is expensive. If you need to get the
# length, you need to maintain this separately.
#
# Eventually, I need to express this through the interfaces.
#
################################################################

OOBTree.OOSet.__implements__=ISet
OOBTree.OOTreeSet.__implements__=ITreeSet
OOBTree.OOBucket.__implements__=IDictionaryIsh
OOBTree.OOBTree.__implements__=IBTree
