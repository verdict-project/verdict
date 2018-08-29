from .resultset import ResultSet
import time
import os
import subprocess
import atexit
import pkg_resources
from py4j.java_gateway import JavaGateway, launch_gateway

Gateway = None

class VerdictContext:
    """main interface to interact with the java objects
    """

    def __init__(self, url, usr, pwd):
        init()
        self._context = connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self._context.sql(query))

def init():
    """initialize a py4j gateway
    starting up a JVM only once
    """

    global Gateway
    version = pkg_resources.require("pyverdict")[0].version
    if Gateway is None:
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        jar_name = 'verdictdb-core-' + version + '-jar-with-dependencies.jar'
        jar_path = os.path.join(lib_dir, jar_name)
        Gateway = launch_gateway(classpath=jar_path)
        # subprocess.Popen(['java', '-cp', jar_path, 'org.verdictdb.VerdictGateway'])
        # time.sleep(1) # TODO: check whether the jvm started before connecting to it
        # Gateway = JavaGateway()
        atexit.register(close)
    return Gateway

def connect(url, usr, pwd):
    return Gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url, usr, pwd)

def close():
    Gateway.shutdown()
