import psycopg2.tz

import json
import dateutil

def _bool_to_str(java_obj, idx):
    bool_result = java_obj.getValue(idx)

    if bool_result == True:
        return '1'
    else:
        return '0'


def _java_object_to_str(java_obj, idx):
    return java_obj.getString(idx)


def _bytes_to_memoryview(java_obj, idx):
    # java_obj.getValue should return a value of type bytes in this case
    return memoryview(java_obj.getValue(idx))


def _json_str_to_dict(java_obj, idx):
    return json.loads(java_obj.getString(idx))


def _float_to_money_str(java_obj, idx):
    return '${:.2f}'.format(java_obj.getValue(idx))


def _str_to_date(java_obj, idx):
    return dateutil.parser.parse(java_obj.getString(idx)).date()


def _str_to_time(java_obj, idx):
    return dateutil.parser.parse(java_obj.getString(idx)).time()


def _str_to_datetime(java_obj, idx):
    return dateutil.parser.parse(java_obj.getString(idx))


def _get_base_timezone():
    return psycopg2.tz.FixedOffsetTimezone(offset=0, name=None)


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


# NOTE: All functions in this map must take two arguments, the first
# being a JavaObject representing a ResultSet and the second being the index
# of the column the value to parse resides in in the ResultSet.
_typename_to_converter_fxn = (
    'bit': _bool_to_str,
    'varbit': _java_object_to_str,
    'box': _java_object_to_str,
    'bytea': _bytes_to_memoryview,
    'cidr': _java_object_to_str,
    'circle': _java_object_to_str,
    'date': _str_to_date,
    'inet': _java_object_to_str,
    'json': _json_str_to_dict,
    'line': _java_object_to_str,
    'lseg': _java_object_to_str,
    'macaddr': _java_object_to_str,
    'macaddr8': _java_object_to_str,
    'money': _float_to_money_str,
    'path': _java_object_to_str,
    'point': _java_object_to_str,
    'polygon': _java_object_to_str,
    'time': _str_to_time,
    'timestamp': _str_to_datetime,
    'uuid': _java_object_to_str,
    'xml': _java_object_to_str,
    'bit': _java_object_to_str,
    'timetz': _str_to_timetz,
    'timestamptz': _str_to_datetimetz,
)

def read_value(resultset, index, col_typename):
    if col_typename in _typename_to_converter_fxn:
        return _typename_to_converter_fxn[col_typename](resultset, index)
    else:
        return resultset.getValue(index)

