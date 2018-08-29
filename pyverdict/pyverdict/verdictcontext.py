from .resultset import ResultSet
import time
import os
import subprocess
import atexit
import pkg_resources
from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters, java_import

Gateway = None

class VerdictContext:
    """main interface to interact with the java objects
    require specifying the jdbc driver jar path
    """

    def __init__(self, url, usr, pwd, jdbc_driver):
        init(jdbc_driver)
        self._context = connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self._context.sql(query))

def init(jdbc_driver):
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
        port = launch_gateway(classpath=jdbc_driver + ':' + jar_path + ':org.verdictdb.VerdictGateway')
        gp = GatewayParameters(port=port)
        Gateway = JavaGateway(gateway_parameters=gp)
        atexit.register(close)
    return Gateway

def connect(url, usr, pwd):
    Gateway.jvm.Class.forName("com.mysql.jdbc.Driver") # TODO: determine the right driver class from connection string
    return Gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url, usr, pwd)

def close():
    Gateway.shutdown()
