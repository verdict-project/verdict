#from py4j.java_gateway import java_import
from pyspark.sql.dataframe import DataFrame
#from pyspark.java_gateway import launch_gateway
from pyspark.sql.session import SparkSession


class VerdictSparkSession(object):
    """
    Class for wrapping Spark2's SparkSession class.

    :param sc: The :class:`SparkContext` wrapped by this class.
    """
    _python_sc = None

    _python_SparkSession = None

    _java_SparkSession = None

    _java_VerdictSparkSession = None

    def __init__(self, sc):
        """
        Creates a new VerdictHiveContext. The parameter must be a SparkContext
        instance.
        """
        self._python_sc = sc
        self._python_SparkSession = SparkSession(sc)

    @property
    def _jsparkSession(self):
        if self._java_SparkSession is None:
            self._java_SparkSession = self._python_SparkSession._jsparkSession
        return self._java_SparkSession

    @property
    def _jsc(self):
        return self._python_sc._jsc.sc()

    @property
    def _jvm(self):
        return self._python_SparkSession._sc._jvm

    @property
    def _jverdictContext(self):
        if self._java_VerdictSparkSession is None:
            self._java_VerdictSparkSession = self._jvm.edu.umich.verdict.VerdictSpark2Context(self._jsc)
        return self._java_VerdictSparkSession

    def toPyDf(self, jdf):
        return DataFrame(jdf, self._python_SparkSession)

    def sql(self, sql):
        jdf = self._jverdictContext.sql(sql)
        return self.toPyDf(jdf)

