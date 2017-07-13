from py4j.java_gateway import java_import
from pyspark.sql import DataFrame
from py4j.java_gateway import java_import
from pyspark.java_gateway import launch_gateway

VERDICT_CONTEXT_CLASS = "edu.umich.verdict.VerdictSparkContext"


class VerdictSparkContext(object):
    """
    :param sqlContext: The :class:`SQLContext` wrapped by this class.
    """

    _python_SQLContext = None

    _scala_SQLContext = None

    _java_VerdictSparkContext = None

    def __init__(self, sqlContext):
        """
        Creates a new VerdictSparkContext. Either a SQLContext instance or a
        HiveContext instance must be passed.
        """
        if not type(sqlContext).__name__ == 'HiveContext' and not type(sqlContext).__name__ == 'SQLContext':
            print 'The of sqlContext parameter must be either HiveContext or SQLContext'
        self._python_SQLContext = sqlContext

    @property
    def _jsqlContext(self):
        if self._scala_SQLContext is None:
            self._scala_SQLContext = self._python_SQLContext._ssql_ctx
        return self._scala_SQLContext

    @property
    def _jvm(self):
        return self._python_SQLContext._sc._gateway.jvm

    @property
    def _jverdictContext(self):
        if self._java_VerdictSparkContext is None:
            java_import(self._jvm, VERDICT_CONTEXT_CLASS)
            self._java_VerdictSparkContext = self._jvm.VerdictSparkContext(self._jsqlContext)
        return self._java_VerdictSparkContext

    def toPyDf(self, jdf):
        return DataFrame(jdf, self._python_SQLContext)

    def sql(self, sql):
        jdf = self._jverdictContext.sql(sql)
        return self.toPyDf(jdf)

