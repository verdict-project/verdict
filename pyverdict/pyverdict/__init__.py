'''
    Copyright 2018 University of Michigan

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''
from .verdictcontext import VerdictContext
from .verdictcommon import get_metadata
import json

__version__ = get_metadata('__version__')

__verdictdb_version__ = get_metadata('__verdictdb_version__')

def impala_context(host, port, schema=None, user=None, password=None):
    return VerdictContext.new_impala_context(host, port, schema, user, password)

def impala(host, port, schema=None, user=None, password=None):
    return impala_context(host, port, schema, user, password)

def mysql_context(host, user, password=None, port=3306):
    return VerdictContext.new_mysql_context(host, user, password, port)

def mysql(host, user, password=None, port=3306):
    return mysql_context(host, user, password, port)

def postgres_context(host, port, dbname, user, password=None):
    return VerdictContext.new_postgres_context(dbname, user, password, host, port)

def postgres(host, port, dbname, user, password=None):
    return postgres_context(host, port, dbname, user, password)

def presto_context(host, catalog, user, password=None, port=8080):
    return VerdictContext.new_presto_context(host, catalog, user, password, port)

def presto(host, catalog, user, password=None, port=8080):
    return presto_context(host, catalog, user, password, port)

def redshift(host, port, dbname='', user=None, password=None):
    return redshift_context(host, port, dbname, user, password)

def redshift_context(host, port, dbname='', user=None, password=None):
    return VerdictContext.new_redshift_context(host, port, dbname, user, password)

