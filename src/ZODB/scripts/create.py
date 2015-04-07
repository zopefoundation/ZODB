import ZODB
import ZODB.FileStorage


def main():
    """
    Console script to create db
    """
    storage = ZODB.FileStorage.FileStorage('Data.fs')
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root
    # XXX Consider allowing end user to create a db w/values
    # e.g. createdb 'foo=bar'
    print('Created database with contents %s.' % root._root)


if __name__ == '__main__':

    """
    Execute console script to create db
    """
    main()
