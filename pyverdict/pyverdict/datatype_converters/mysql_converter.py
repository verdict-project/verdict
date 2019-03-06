from .converter_base import DatatypeConverterBase
import dateutil
import dateutil
import dateutil.tz
from datetime import date, datetime, timedelta


def _str_to_date(java_obj, idx):
    value_str = java_obj.getString(idx)
    return datetime.strptime(value_str, "%Y-%m-%d").date()

def _str_to_datetime(java_obj, idx):
    value_str = java_obj.getString(idx)
    value_str = value_str[:19]
    return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")

def _str_to_time(java_obj, idx):
    value_str = java_obj.getString(idx)
    t = datetime.strptime(value_str, "%H:%M:%S")
    return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)

def _str_to_bigint(java_obj, idx):
    value_str = java_obj.getString(idx)
    return int(value_str)


_typename_to_converter_fxn = {'date': _str_to_date, 'timestamp': _str_to_datetime, 'time': _str_to_time, 'bigint': _str_to_bigint}

class MysqlConverter(DatatypeConverterBase):
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
    @staticmethod
    def read_value(result_set, index, col_typename):
        if col_typename in _typename_to_converter_fxn:
            if result_set.getString(index) is None:
                return None

            return _typename_to_converter_fxn[col_typename](result_set, index)
        else:
            return result_set.getValue(index)
