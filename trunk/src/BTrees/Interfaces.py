
import OOBTree, Interface

class ICollection(Interface.Base):

    def clear():
        """Remove all of the items from the collection"""

    def __nonzero__():
        """Check if the collection is non-empty.

        Return a true value if the collection is non-empty and a
        false otherwise.
        """

class ISized(ICollection):

    def __len__():
        """Return the number of items in the set"""

class IReadSequence(Interface.Base):

    def __getitem__(index):
        """Return an item for a given index."""

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

class IKeySequence(IKeyed, ISized):

    def __getitem__(index):
        """Return the key in the given index position

        This allows iteration with for loops and use in functions,
        like map and list, that read sequences.
        """

class ISet(IKeySequence, ISetMutable):
    pass

class ITreeSet(IKeyed, ISetMutable):
    pass

class IDictionaryIsh(IKeyed, ISized):

    def __getitem__(key):
        """Get the value for the given key

        Raise a key error if the key if not in the collection.
        """

    def get(key, default=None):
        """Get the value for the given key

        Raise a key error if the key if not in the collection and no
        default is specified.

        Return the default if specified and the key is not in the
        collection.
        """

    def __setitem__(key, value):
        """Set the value for the given key"""

    def __delitem__(key):
        """delete the value for the given key

        Raise a key error if the key if not in the collection."""

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
    
class IBTree(IDictionaryIsh):

    def insert(key, value):
        """Insert a key and value into the colelction.

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
    
Interface.assertTypeImplements(OOBTree.OOSet, ISet)
Interface.assertTypeImplements(OOBTree.OOTreeSet, ITreeSet)
Interface.assertTypeImplements(OOBTree.OOBucket, IDictionaryIsh)
Interface.assertTypeImplements(OOBTree.OOBTree, IBTree)


