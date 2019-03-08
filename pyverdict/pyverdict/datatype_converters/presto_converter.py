from .converter_base import DatatypeConverterBase
import dateutil
import dateutil
import dateutil.tz


def _str_to_date(java_obj, idx):
    return java_obj.getString(idx)

def _str_to_decimal(java_obj, idx):
    return float(java_obj.getString(idx))

def _str_to_datetime(java_obj, idx):
    value_str = java_obj.getString(idx)
    return value_str[:17] + '%06.3f'%float(value_str[18:])

_typename_to_converter_fxn = {'timestamp': _str_to_datetime, 'date': _str_to_date, 'decimal': _str_to_decimal}

class PrestoConverter(DatatypeConverterBase):
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
    @staticmethod
    def read_value(result_set, index, col_typename):
        if col_typename in _typename_to_converter_fxn:
            if result_set.getString(index) is None:
                return None

            return _typename_to_converter_fxn[col_typename](result_set, index)
        else:
            return result_set.getValue(index)
