"""Wrapper script for zdctl.py that causes it to use the ZEO schema."""

import os

import ZEO
import zdaemon.zdctl


# Main program
def main(args=None):
    options = zdaemon.zdctl.ZDCtlOptions()
    options.schemadir = os.path.dirname(ZEO.__file__)
    options.schemafile = "zeoctl.xml"
    zdaemon.zdctl.main(args, options)


if __name__ == "__main__":
    main()
