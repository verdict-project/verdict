from .resultset import ResultSet
import os
import pkg_resources
from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters, java_import
from time import sleep


class VerdictContext:
    """
    The main interface to interact with the java objects.
    The path to the jdbc drivers must be specified to use them.
    """

    def __init__(self, url, class_path):
        self._gateway = self.get_gateway(class_path)
        self._context = self.connect(url)

    def sql(self, query):
        return ResultSet(self._context.sql(query))

    def get_gateway(self, class_path):
        """
        initialize a py4j gateway.
        """
        version = pkg_resources.require("pyverdict")[0].version
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        verdict_jar_name = 'verdictdb-core-' + version + '-jar-with-dependencies.jar'
        verdict_jar = os.path.join(lib_dir, verdict_jar_name)
        port = launch_gateway(classpath=class_path + ':' + verdict_jar, die_on_exit=True)
        sleep(1)
        gp = GatewayParameters(port=port)
        gateway = JavaGateway(gateway_parameters=gp)
        return gateway

    def connect(self, url):
        return self._gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url)
