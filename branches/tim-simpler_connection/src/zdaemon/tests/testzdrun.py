"""Test suite for zdrun.py."""

import os
import sys
import time
import signal
import tempfile
import unittest
import socket

from StringIO import StringIO

import ZConfig

from zdaemon import zdrun, zdctl


class ConfiguredOptions:
    """Options class that loads configuration from a specified string.

    This always loads from the string, regardless of any -C option
    that may be given.
    """

    def set_configuration(self, configuration):
        self.__configuration = configuration
        self.configfile = "<preloaded string>"

    def load_configfile(self):
        sio = StringIO(self.__configuration)
        cfg = ZConfig.loadConfigFile(self.schema, sio, self.zconfig_options)
        self.configroot, self.confighandlers = cfg


class ConfiguredZDRunOptions(ConfiguredOptions, zdrun.ZDRunOptions):

    def __init__(self, configuration):
        zdrun.ZDRunOptions.__init__(self)
        self.set_configuration(configuration)


class ZDaemonTests(unittest.TestCase):

    python = os.path.abspath(sys.executable)
    assert os.path.exists(python)
    here = os.path.abspath(os.path.dirname(__file__))
    assert os.path.isdir(here)
    nokill = os.path.join(here, "nokill.py")
    assert os.path.exists(nokill)
    parent = os.path.dirname(here)
    zdrun = os.path.join(parent, "zdrun.py")
    assert os.path.exists(zdrun)

    ppath = os.pathsep.join(sys.path)

    def setUp(self):
        self.zdsock = tempfile.mktemp()
        self.new_stdout = StringIO()
        self.save_stdout = sys.stdout
        sys.stdout = self.new_stdout
        self.expect = ""

    def tearDown(self):
        sys.stdout = self.save_stdout
        for sig in (signal.SIGTERM,
                    signal.SIGHUP,
                    signal.SIGINT,
                    signal.SIGCHLD):
            signal.signal(sig, signal.SIG_DFL)
        try:
            os.unlink(self.zdsock)
        except os.error:
            pass
        output = self.new_stdout.getvalue()
        self.assertEqual(self.expect, output)

    def quoteargs(self, args):
        for i in range(len(args)):
            if " " in args[i]:
                args[i] = '"%s"' % args[i]
        return " ".join(args)

    def rundaemon(self, args):
        # Add quotes, in case some pathname contains spaces (e.g. Mac OS X)
        args = self.quoteargs(args)
        cmd = ('PYTHONPATH="%s" "%s" "%s" -d -s "%s" %s' %
               (self.ppath, self.python, self.zdrun, self.zdsock, args))
        os.system(cmd)
        # When the daemon crashes, the following may help debug it:
        ##os.system("PYTHONPATH=%s %s %s -s %s %s &" %
        ##    (self.ppath, self.python, self.zdrun, self.zdsock, args))

    def run(self, args):
        if type(args) is type(""):
            args = args.split()
        try:
            zdctl.main(["-s", self.zdsock] + args)
        except SystemExit:
            pass

    def testSystem(self):
        self.rundaemon(["echo", "-n"])
        self.expect = ""

##     def testInvoke(self):
##         self.run("echo -n")
##         self.expect = ""

##     def testControl(self):
##         self.rundaemon(["sleep", "1000"])
##         time.sleep(1)
##         self.run("stop")
##         time.sleep(1)
##         self.run("exit")
##         self.expect = "Sent SIGTERM\nExiting now\n"

##     def testStop(self):
##         self.rundaemon([self.python, self.nokill])
##         time.sleep(1)
##         self.run("stop")
##         time.sleep(1)
##         self.run("exit")
##         self.expect = "Sent SIGTERM\nSent SIGTERM; will exit later\n"

    def testHelp(self):
        self.run("-h")
        import __main__
        self.expect = __main__.__doc__

    def testOptionsSysArgv(self):
        # Check that options are parsed from sys.argv by default
        options = zdrun.ZDRunOptions()
        save_sys_argv = sys.argv
        try:
            sys.argv = ["A", "B", "C"]
            options.realize()
        finally:
            sys.argv = save_sys_argv
        self.assertEqual(options.options, [])
        self.assertEqual(options.args, ["B", "C"])

    def testOptionsBasic(self):
        # Check basic option parsing
        options = zdrun.ZDRunOptions()
        options.realize(["B", "C"], "foo")
        self.assertEqual(options.options, [])
        self.assertEqual(options.args, ["B", "C"])
        self.assertEqual(options.progname, "foo")

    def testOptionsHelp(self):
        # Check that -h behaves properly
        options = zdrun.ZDRunOptions()
        try:
            options.realize(["-h"], doc=zdrun.__doc__)
        except SystemExit, err:
            self.failIf(err.code)
        else:
            self.fail("SystemExit expected")
        self.expect = zdrun.__doc__

    def testSubprocessBasic(self):
        # Check basic subprocess management: spawn, kill, wait
        options = zdrun.ZDRunOptions()
        options.realize(["sleep", "100"])
        proc = zdrun.Subprocess(options)
        self.assertEqual(proc.pid, 0)
        pid = proc.spawn()
        self.assertEqual(proc.pid, pid)
        msg = proc.kill(signal.SIGTERM)
        self.assertEqual(msg, None)
        wpid, wsts = os.waitpid(pid, 0)
        self.assertEqual(wpid, pid)
        self.assertEqual(os.WIFSIGNALED(wsts), 1)
        self.assertEqual(os.WTERMSIG(wsts), signal.SIGTERM)
        proc.setstatus(wsts)
        self.assertEqual(proc.pid, 0)

    def testEventlogOverride(self):
        # Make sure runner.eventlog is used if it exists
        options = ConfiguredZDRunOptions("""\
            <runner>
              program /bin/true
              <eventlog>
                level 42
              </eventlog>
            </runner>

            <eventlog>
              level 35
            </eventlog>
            """)
        options.realize(["/bin/true"])
        self.assertEqual(options.config_logger.level, 42)

    def testEventlogWithoutOverride(self):
        # Make sure eventlog is used if runner.eventlog doesn't exist
        options = ConfiguredZDRunOptions("""\
            <runner>
              program /bin/true
            </runner>

            <eventlog>
              level 35
            </eventlog>
            """)
        options.realize(["/bin/true"])
        self.assertEqual(options.config_logger.level, 35)

    def testRunIgnoresParentSignals(self):
        # Spawn a process which will in turn spawn a zdrun process.
        # We make sure that the zdrun process is still running even if
        # its parent process receives an interrupt signal (it should
        # not be passed to zdrun).
        zdrun_socket = os.path.join(self.here, 'testsock')
        zdctlpid = os.spawnvp(
            os.P_NOWAIT,
            sys.executable,
            [sys.executable, os.path.join(self.here, 'parent.py')]
            )
        time.sleep(2) # race condition possible here
        os.kill(zdctlpid, signal.SIGINT)
        try:
            response = send_action('status\n', zdrun_socket) or ''
        except socket.error, msg:
            response = ''
        params = response.split('\n')
        self.assert_(len(params) > 1, repr(response))
        # kill the process
        send_action('exit\n', zdrun_socket)

    def testUmask(self):
        path = tempfile.mktemp()
        # With umask 666, we should create a file that we aren't able
        # to write.  If access says no, assume that umask works.
        try:
            touch_cmd = "/bin/touch"
            if not os.path.exists(touch_cmd):
                touch_cmd = "/usr/bin/touch" # Mac OS X
            self.rundaemon(["-m", "666", touch_cmd, path])
            for i in range(5):
                if not os.path.exists(path):
                    time.sleep(0.1)
            self.assert_(os.path.exists(path))
            self.assert_(not os.access(path, os.W_OK))
        finally:
            if os.path.exists(path):
                os.remove(path)

def send_action(action, sockname):
    """Send an action to the zdrun server and return the response.

    Return None if the server is not up or any other error happened.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(sockname)
        sock.send(action + "\n")
        sock.shutdown(1) # We're not writing any more
        response = ""
        while 1:
            data = sock.recv(1000)
            if not data:
                break
            response += data
        sock.close()
        return response
    except socket.error, msg:
        return None

def test_suite():
    suite = unittest.TestSuite()
    if os.name == "posix":
        suite.addTest(unittest.makeSuite(ZDaemonTests))
    return suite

if __name__ == '__main__':
    __file__ = sys.argv[0]
    unittest.main(defaultTest='test_suite')
