from .verdictcontext import VerdictContext
from .verdictcommon import get_metadata
import json

__version__ = get_metadata('__version__')

def mysql_context(host, user, password, port=3306):
    return VerdictContext.new_mysql_context(host, user, password, port)

def presto_context(host, catalog, user, password=None, port=8081):
    return VerdictContext.new_presto_context(host, catalog, user, password, port)
