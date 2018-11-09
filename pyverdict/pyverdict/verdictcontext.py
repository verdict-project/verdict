import os
import pkg_resources
from . import verdictresult
from . import verdictcommon
from .verdictexception import *
from py4j.java_gateway import JavaGateway
from time import sleep


class VerdictContext:
    """The main Python interface to VerdictDB Java core.

    The path to the jdbc drivers must be specified to use them.

    Args:
        extra_class_path: The extra classpath used in addition to verdictdb's jar file. This arg
                          can either be a single str or a list of str; each str is an absolute path
                          to a jar file.
    """

    def __init__(self, url, extra_class_path=None):
        self._gateway = self._get_gateway(extra_class_path)
        self._context = self._get_context(self._gateway, url)
        self._dbtype = self._get_dbtype(url)
        self._url = url

    def sql(self, query):
        java_resultset = self._context.sql(query)
        if java_resultset is None:
            return None
        else:
            return verdictresult.SingleResultSet(java_resultset, self)

    def get_dbtype(self):
        return self._dbtype.lower()

    def _get_dbtype(self, url):
        tokenized_url = url.split(':')
        if tokenized_url[0] != 'jdbc':
            raise VerdictException('The url must start with \'jdbc\'')
        if len(tokenized_url) < 2:
            raise VerdictException(
                'This url does not seem to have valid connection information: ' + url)
        return tokenized_url[1]

    def _get_gateway(self, extra_class_path):
        """
        Initializes a py4j gateway.

        Args:
            class_path: Either a single str or a list of str; each str is an absolute path to the
            jar file.
        """
        class_path = self._get_class_path(extra_class_path)
        gateway = JavaGateway.launch_gateway(classpath=class_path, die_on_exit=True)
        sleep(1)
        return gateway

    def _get_class_path(self, extra_class_path):
        """
        Returns str class path including the one for verdict jar path
        """
        lib_jar_path = self._get_lib_jars_path()
        verdict_jar_path = self._get_verdict_jar_path()

        if not os.path.isfile(verdict_jar_path):
            raise VerdictException("VerdictDB's jar file is not found.")

        str_class_path = '{}:{}'.format(lib_jar_path, verdict_jar_path)

        if extra_class_path is None:
            pass
        if isinstance(extra_class_path, str):
            str_class_path = '{}:{}'.format(extra_class_path, lib_jar_path)
        elif isinstance(extra_class_path, list):
            extra_class_path_str = ':'.join(extra_class_path)
            str_class_path = '{}:{}'.format(extra_class_path_str, lib_jar_path)

        return str_class_path

    def _get_lib_jars_path(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        full_paths = []
        for filename in os.listdir(lib_dir):
            if filename[-3:] == 'jar':
                full_path = os.path.join(lib_dir, filename)
                full_paths.append(full_path)
        return ':'.join(full_paths)

    def _get_verdict_jar_path(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'verdict_jar')
        version = self._get_verdictdb_version();
        verdict_jar_file = 'verdictdb-core-{}-jar-with-dependencies.jar'.format(version)
        verdict_jar_path = os.path.join(lib_dir, verdict_jar_file)
        return verdict_jar_path

    def _get_verdictdb_version(self):
        return verdictcommon.get_verdictdb_version()

    def _get_context(self, gateway, url):
        return gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url)
