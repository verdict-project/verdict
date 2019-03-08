from .converter_base import DatatypeConverterBase
import datetime
import dateutil
import dateutil.tz


def _str_to_datetime(java_obj, idx):
    return dateutil.parser.parse(java_obj.getString(idx))


def _str_to_date(java_obj, idx):
    return _str_to_datetime(java_obj, idx).date()


def _str_to_time(java_obj, idx):
    return _str_to_datetime(java_obj, idx).time()


def _get_base_timezone():
    return dateutil.tz.tzlocal()


def _str_to_timetz(java_obj, idx):
    result = _str_to_time(java_obj, idx)

    if result.tzinfo is None:
        return result.replace(tzinfo=_get_base_timezone())
    else:
        return result


def _str_to_datetimetz(java_obj, idx):
    result = _str_to_datetime(java_obj, idx)

    if result.tzinfo is None:
        return result.replace(tzinfo=_get_base_timezone())
    else:
        return result


_typename_to_converter_fxn = {
    'date': _str_to_date,
    'time': _str_to_time,
    'timetz': _str_to_timetz,
    'timestamp': _str_to_datetime,
    'timestamptz': _str_to_datetimetz,
}


class RedshiftConverter(DatatypeConverterBase):
    '''
    Type conversion rule:

        BIGINT                                    => int,
        BOOLEAN                                   => bool,
        BOOL                                      => bool,
        BPCHAR                                    => str,
        CHAR                                      => str,
        CHARACTER                                 => str,
        CHARACTER VARYING                         => str,
        DATE                        => JavaObject => datetime.datetime,
        DECIMAL                                   => decimal.Decimal,
        DOUBLE PRECISION                          => float,
        FLOAT                                     => float,
        FLOAT4                                    => float,
        FLOAT8                                    => float,
        INT                                       => int,
        INT2                                      => int,
        INT4                                      => int,
        INTEGER                                   => int,
        NCHAR                                     => str,
        NUMERIC                                   => decimal.Decimal,
        NVARCHAR                                  => str,
        REAL                                      => float,
        SMALLINT                                  => int,
        TEXT                                      => str,
        TIMESTAMP                   => JavaObject => datetime.datetime,
        TIMESTAMP WITHOUT TIME ZONE => JavaObject => datetime.datetime,
        TIMESTAMPTZ                 => JavaObject => datetime.datetime,
        TIMESTAMP WITH TIME ZONE    => JavaObject => datetime.datetime,
        VARCHAR                                   => str

    '''

    @staticmethod
    def read_value(result_set, index, col_typename):
        if col_typename in _typename_to_converter_fxn:
            if result_set.getString(index) is None:
                return None

            return _typename_to_converter_fxn[col_typename](result_set, index)
        else:
            return result_set.getValue(index)


