@echo off

rem  Simple script to run the tests on Windows.

rem  The paths to different versions of Python need to be
rem  edited for the system this is being run on; comment
rem  out lines that aren't needed or wanted.

\Python213\python runtests.py
\Python221\python runtests.py
\Python222\python runtests.py
\Python230\python runtests.py
