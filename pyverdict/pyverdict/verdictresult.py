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
from datetime import date, datetime, timedelta
import decimal
import numpy as np
import pandas as pd


class SingleResultSet:
    """Reads the data from the Java SingleResultSet

    The data retrieval roughly follows PEP 249:
    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """

    python_type_to_numpy_type = {
        bool: np.bool_,
        int: np.int64,
        decimal.Decimal: np.float64,
        str: np.unicode_,
        bytes: np.unicode_,
        date: np.datetime64,
        datetime: np.datetime64,
        timedelta: np.timedelta64
    }

    def __init__(self, heading, column_types, rows, verdict_context):
        self._verdict_context = verdict_context
        # (heading, column_inttypes, column_types, rows) = self._read_all(resultset)
        self._heading = heading
        self._column_types = column_types
        self._rows = rows
        self.rowcount = len(rows)

    @classmethod
    def from_java_resultset(cls, resultset, verdict_context):
        (heading, column_inttypes, column_types, rows) = cls._read_all(resultset, verdict_context)
        return cls(heading, column_types, rows, verdict_context)

    @classmethod
    def status_result(cls, status_msg, verdict_context):
        heading = ['status']
        column_types = ['string']
        rows = [[status_msg]]
        return cls(heading, column_types, rows, verdict_context)

    def column_names(self):
        return self._heading

    def rows(self):
        return self._rows

    def fetchall(self):
        return self.rows()

    def types(self):
        return self._column_types

    # def type_java_int(self):
    #     return self._column_inttypes

    def to_df(self):
        """
        Converts to a pandas DataFrame.
        """
        rows = self.rows()
        columns = self.column_names()
        if len(columns) == 0 and len(rows) == 0:
            columns = ['empty']
        return pd.DataFrame(np.array(rows), columns=columns)

    @classmethod
    def _read_value(cls, resultset, index, col_type, verdict_context):
        dbtype = verdict_context.get_dbtype()
        if dbtype == 'mysql':
            return cls._read_value_mysql(resultset, index, col_type)
        elif dbtype == 'presto':
            return cls._read_value_presto(resultset, index, col_type)
        else:
            raise NotImplementedError

    @classmethod
    def _read_value_presto(cls, resultset, index, col_type):
        """
        Type conversion rule (Presto):
        'tinyint'    => int
        'boolean'    => int
        'smallint'   => int
        'integer'    => int
        'bigint'     => int
        'decimal'    => float
        'real'       => float
        'double'     => float
        'date'       => str
        'timestamp'  => str
        'char'       => str
        'varchar'    => str
        """
        type_to_read_in_str_for_presto = set(['decimal', 'date', 'timestamp'])

        if col_type in type_to_read_in_str_for_presto:
            value_str = resultset.getString(index)
            if value_str is None:
                return None

            if col_type == 'decimal':
                return float(value_str)
            elif col_type == 'date':
                return value_str
            elif col_type == 'timestamp':
                return value_str[:17] + '%06.3f'%float(value_str[18:])
            else:
                return None     # not supposed to reach here

        return resultset.getValue(index)

    @classmethod
    def _read_value_mysql(cls, resultset, index, col_type):
        """Reads the value in a type-sensitive way

        Due to the reliance on the underlying JDBC library, we cannot always distinguish the exact
        type of the underlying data; we can only set the type according to the type indentifier as
        indicated by the JDBC library itself. For example, MySQL's library does not distinguish bit and
        boolean by passing -7 for both types while their actual meanings are different and they are
        separate types defined in java.sql.Types.

        Type conversion rule (MySQL):
        'bit'                     => boolean,   # due to MySQL driver bug ('bit' is treated as boolean)
        'tinyint'                 => int,
        'bool'                    => boolean,
        'smallint'                => int,
        'medimumInteger'          => int,
        'int'                     => int,
        'integer                  => int,
        'bigint'                  => int,
        'decimal'                 => decimal.Decimal,
        'dec'                     => decimal.Decimal,
        'real'                    => float,
        'double'                  => float,
        'doubleprecision'         => float,
        'date'      => JavaObject => datetime.date,
        'datetime'  => JavaObject => datetime.datetime,
        'timestamp' => JavaObject => datetime.datetime,
        'time'      => JavaObject => datetime.timedelta,
        'year(2)'   => JavaObject => datetime.date,
        'year(4)'   => JavaObject => datetime.date,
        'char'                    => str,
        'varchar'                 => str,
        'binary'                  => bytes,
        'varbinary'               => bytes,
        'tinyblob'                => bytes,
        'tinytext'                => str,
        'blob'                    => bytes,
        'text'                    => str,
        'mediumBlob'              => bytes,
        'medimumText'             => str,
        'longBlob'                => bytes,
        'longText'                => str,
        'enumCol'                 => str,
        'setCol'                  => str

        Time-related Java objects are read as py4j.JavaObject by default. To avoid this, we read
        them as str and convert to an appropriate python object.

        Args
            resultset: the Java result set currently in process
            index: zero-based index of the column to read
            col_type: column type in str
        """
        type_to_read_in_str_for_mysql = set(['date', 'timestamp', 'time', 'bigint'])

        if col_type in type_to_read_in_str_for_mysql:
            value_str = resultset.getString(index)
            if value_str is None:
                return None

            if col_type == 'date':
                return datetime.strptime(value_str, "%Y-%m-%d").date()
            elif col_type == 'timestamp':
                value_str = value_str[:19]      # 19 == len('2018-12-31 00:00:01')
                return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")
            elif col_type == 'time':
                t = datetime.strptime(value_str, "%H:%M:%S")
                return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
            elif col_type == 'bigint':
                return int(value_str)
            else:
                return None         # not supposed to reach here
        # if col_type == 'bit':
        #     value = resultset.getByte(index)
        #     return value        # this will return int 0 or int 1
        else:
            return resultset.getValue(index)

    @classmethod
    def _read_all(cls, resultset, verdict_context):
        column_count = resultset.getColumnCount()
        heading = []          # column heading
        column_inttypes = []  # column types in java.sql.Types
        column_types = []     # column type
        rows = []             # data in the table

        for i in range(column_count):
            heading.append(resultset.getColumnName(i))
            column_inttypes.append(resultset.getColumnType(i))
            column_types.append(resultset.getColumnTypeName(i))

        while (resultset.next()):
            row = []
            for i in range(column_count):
                row.append(cls._read_value(resultset, i, column_types[i], verdict_context))
            rows.append(row)

        return (heading, column_inttypes, column_types, rows)
