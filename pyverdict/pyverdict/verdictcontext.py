from .resultset import ResultSet
import os
import atexit
import pkg_resources
from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters, java_import


class VerdictContext:
    """main interface to interact with the java objects
    require specifying the jdbc driver jar path
    """

    Gateway = None

    def __init__(self, url, usr, pwd, jdbc_driver):
        self.init(jdbc_driver)
        self._context = self.connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self._context.sql(query))

    @classmethod
    def init(cls, jdbc_driver):
        """initialize a py4j gateway
        starting up a JVM only once
        """

        version = pkg_resources.require("pyverdict")[0].version
        if cls.Gateway is None:
            root_dir = os.path.dirname(os.path.abspath(__file__))
            lib_dir = os.path.join(root_dir, 'lib')
            jar_name = 'verdictdb-core-' + version + '-jar-with-dependencies.jar'
            jar_path = os.path.join(lib_dir, jar_name)
            port = launch_gateway(classpath=jdbc_driver + ':' + jar_path + ':org.verdictdb.VerdictGateway')
            gp = GatewayParameters(port=port)
            cls.Gateway = JavaGateway(gateway_parameters=gp)
            atexit.register(cls.close)
        return cls.Gateway

    @classmethod
    def connect(cls, url, usr, pwd):
        return cls.Gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url, usr, pwd)

    @classmethod
    def close(cls):
        cls.Gateway.shutdown()
