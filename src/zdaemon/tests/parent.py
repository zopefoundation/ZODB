import time
import os
import sys

def main():
    # dummy zdctl startup of zdrun
    shutup()
    file = os.path.normpath(os.path.abspath(sys.argv[0]))
    dir = os.path.dirname(file)
    zctldir = os.path.dirname(dir)
    zdrun = os.path.join(zctldir, 'zdrun.py')
    args = [sys.executable, zdrun]
    args += ['-d', '-b', '10', '-s', os.path.join(dir, 'testsock'),
             '-x', '0,2', '-z', dir, os.path.join(dir, 'donothing.sh')]
    flag = os.P_NOWAIT
    #cmd = ' '.join([sys.executable] + args)
    #print cmd
    os.spawnvp(flag, args[0], args)
    while 1:
        # wait to be signaled
        time.sleep(1)

def shutup():
    os.close(0)
    sys.stdin = sys.__stdin__ = open("/dev/null")
    os.close(1)
    sys.stdout = sys.__stdout__ = open("/dev/null", "w")
    os.close(2)
    sys.stderr = sys.__stderr__ = open("/dev/null", "w")

if __name__ == '__main__':
    main()
