
import sys, time, os, random

import transaction
from persistent import Persistent

from ZEO import ClientStorage
import ZODB
from ZODB.POSException import ConflictError
from BTrees import OOBTree

class ChatSession(Persistent):

    """Class for a chat session.
    Messages are stored in a B-tree, indexed by the time the message
    was created.  (Eventually we'd want to throw messages out,

    add_message(message) -- add a message to the channel
    new_messages()       -- return new messages since the last call to
                            this method


    """

    def __init__(self, name):
        """Initialize new chat session.
        name -- the channel's name
        """

        self.name = name

        # Internal attribute: _messages holds all the chat messages.
        self._messages = OOBTree.OOBTree()


    def new_messages(self):
        "Return new messages."

        # self._v_last_time is the time of the most recent message
        # returned to the user of this class.
        if not hasattr(self, '_v_last_time'):
            self._v_last_time = 0

        new = []
        T = self._v_last_time

        for T2, message in self._messages.items():
            if T2 > T:
                new.append( message )
                self._v_last_time = T2

        return new

    def add_message(self, message):
        """Add a message to the channel.
        message -- text of the message to be added
        """

        while 1:
            try:
                now = time.time()
                self._messages[ now ] = message
                transaction.commit()
            except ConflictError:
                # Conflict occurred; this process should pause and
                # wait for a little bit, then try again.
                time.sleep(.2)
                pass
            else:
                # No ConflictError exception raised, so break
                # out of the enclosing while loop.
                break
        # end while

def get_chat_session(conn, channelname):
    """Return the chat session for a given channel, creating the session
    if required."""

    # We'll keep a B-tree of sessions, mapping channel names to
    # session objects.  The B-tree is stored at the ZODB's root under
    # the key 'chat_sessions'.
    root = conn.root()
    if not root.has_key('chat_sessions'):
        print 'Creating chat_sessions B-tree'
        root['chat_sessions'] = OOBTree.OOBTree()
        transaction.commit()

    sessions = root['chat_sessions']

    # Get a session object corresponding to the channel name, creating
    # it if necessary.
    if not sessions.has_key( channelname ):
        print 'Creating new session:', channelname
        sessions[ channelname ] = ChatSession(channelname)
        transaction.commit()

    session = sessions[ channelname ]
    return session


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Usage: %s <channelname>' % sys.argv[0]
        sys.exit(0)

    storage = ClientStorage.ClientStorage( ('localhost', 9672) )
    db = ZODB.DB( storage )
    conn = db.open()

    s = session = get_chat_session(conn, sys.argv[1])

    messages = ['Hi.', 'Hello', 'Me too', "I'M 3L33T!!!!"]

    while 1:
        # Send a random message
        msg = random.choice(messages)
        session.add_message( '%s: pid %i' % (msg,os.getpid() ))

        # Display new messages
        for msg in session.new_messages():
            print msg

        # Wait for a few seconds
        pause = random.randint( 1, 4 )
        time.sleep( pause )
