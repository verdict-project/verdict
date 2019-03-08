from .converter_base import DatatypeConverterBase
import dateutil

def _str_to_datetime(java_obj, idx):
    return dateutil.parser.parse(java_obj.getString(idx))


_typename_to_converter_fxn = {'timestamp': _str_to_datetime}


class ImpalaConverter(DatatypeConverterBase):
    '''
    Type conversion rule:

        BIGINT           => int,
        BOOLEAN          => bool,
        CHAR             => str,
        DECIMAL          => decimal.Decimal,
        DOUBLE           => float,
        FLOAT            => float,
        REAL             => float,
        SMALLINT         => int,
        STRING           => str,
        TIMESTAMP        => datetime.datetime,
        TINYINT          => int,
        VARCHAR          => str

    '''

    @staticmethod
    def read_value(result_set, index, col_typename):
        if col_typename in _typename_to_converter_fxn:
            if result_set.getString(index) is None:
                return None

            return _typename_to_converter_fxn[col_typename](result_set, index)
        else:
            return result_set.getValue(index)

