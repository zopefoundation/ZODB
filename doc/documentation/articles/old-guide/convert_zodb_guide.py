# Use the python docs converter to convert to rst
# Requires http://svn.python.org/projects/doctools/converter

from converter import restwriter, convert_file

import sys
import os


if __name__ == '__main__':
    try:
        rootdir = sys.argv[1]
        destdir = os.path.abspath(sys.argv[2])
    except IndexError:
        print "usage: convert.py docrootdir destdir"
        sys.exit()

    os.chdir(rootdir)

    class IncludeRewrite:
        def get(self, a, b=None):
            if os.path.exists(a + '.tex'):
                return a + '.rst'
            print "UNKNOWN FILE %s" % a
            return a
    restwriter.includes_mapping = IncludeRewrite()

    for infile in os.listdir('.'):
        if infile.endswith('.tex'):
            convert_file(infile, os.path.join(destdir, infile[:-3]+'rst'))
