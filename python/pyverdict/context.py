from py4j.java_gateway import java_import
from pyspark.sql import DataFrame
from py4j.java_gateway import java_import
from pyspark.java_gateway import launch_gateway
from pyspark import HiveContext

#VERDICT_CONTEXT_CLASS = "edu.umich.verdict.VerdictSparkHiveContext"


class VerdictHiveContext(object):
    """
    :param sc: The :class:`HiveContext` wrapped by this class.
    """
    _python_sc = None

    _python_HiveContext = None

    _scala_HiveContext = None

    _java_VerdictHiveContext = None

    def __init__(self, sc):
        """
        Creates a new VerdictHiveContext. The parameter must be a SparkContext
        instance.
        """
        self._python_sc = sc
        self._python_HiveContext = HiveContext(sc)

    @property
    def _jhiveContext(self):
        if self._scala_HiveContext is None:
            self._scala_HiveContext = self._python_HiveContext._ssql_ctx
        return self._scala_HiveContext

    @property
    def _jsc(self):
        return self._python_sc._jsc.sc()

    @property
    def _jvm(self):
        return self._python_HiveContext._sc._gateway.jvm

    @property
    def _jverdictContext(self):
        if self._java_VerdictHiveContext is None:
            #java_import(self._jvm, VERDICT_CONTEXT_CLASS)
            self._java_VerdictHiveContext = self._jvm.edu.umich.verdict.VerdictSparkHiveContext(self._jsc)
        return self._java_VerdictHiveContext

    def toPyDf(self, jdf):
        return DataFrame(jdf, self._python_HiveContext)

    def sql(self, sql):
        jdf = self._jverdictContext.sql(sql)
        return self.toPyDf(jdf)

