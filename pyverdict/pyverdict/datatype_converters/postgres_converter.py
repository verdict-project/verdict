from .converter_base import DatatypeConverterBase
import json
import datetime
import dateutil
import dateutil.tz

def _get_num_list_corr_types(str_nums):
    result = []

    for str_num in str_nums:
        parsed_float = float(str_num)

        if parsed_float.is_integer():
            result.append(int(parsed_float))
        else:
            result.append(parsed_float)

    return result


def _build_point_tuples_str(point_vals_list):
    point_tuple_strs = [
        '(%s,%s)' % (point_vals_list[i], point_vals_list[i + 1])
        for i in range(0, len(point_vals_list), 2)
    ]

    return ','.join(point_tuple_strs)


def _bool_to_str(java_obj, idx):
    bool_result = java_obj.getValue(idx)

    if bool_result == True:
        return '1'
    else:
        return '0'


def _box_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    box_points = [
        point_str.replace('(', '').replace(')', '')
        for point_str in unparsed_str.split(',')
    ]

    return '(%s,%s),(%s,%s)' % (*_get_num_list_corr_types(box_points),)


def _circle_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    circle_points = [
        point_str.replace('(', '').replace(')', '')
                 .replace('<', '').replace('>', '')
        for point_str in unparsed_str.split(',')
    ]

    return '<(%s,%s),%s>' % (*_get_num_list_corr_types(circle_points),)


def _line_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    line_points = [
        point_str.replace('{', '').replace('}', '')
        for point_str in unparsed_str.split(',')
    ]

    return '{%s,%s,%s}' % (*_get_num_list_corr_types(line_points),)


def _line_seg_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    line_seg_points = [
        point_str.replace('[', '').replace(']', '')
                 .replace('(', '').replace(')', '')
        for point_str in unparsed_str.split(',')
    ]

    return '[(%s,%s),(%s,%s)]' % (*_get_num_list_corr_types(line_seg_points),)


def _path_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    path_points = [
        point_str.replace('[', '').replace(']', '')
                 .replace('(', '').replace(')', '')
        for point_str in unparsed_str.split(',')
    ]

    intermed_str = _build_point_tuples_str(_get_num_list_corr_types(path_points))

    if unparsed_str[0] == '[':
        return '[%s]' % intermed_str
    else:
        return '(%s)' % intermed_str


def _point_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    points = [
        point_str.replace('(', '').replace(')', '')
        for point_str in unparsed_str.split(',')
    ]

    return '(%s,%s)' % (*_get_num_list_corr_types(points),)


def _polygon_to_str(java_obj, idx):
    unparsed_str = java_obj.getString(idx)

    polygon_points = [
        point_str.replace('[', '').replace(']', '')
                 .replace('(', '').replace(')', '')
        for point_str in unparsed_str.split(',')
    ]

    intermed_str = _build_point_tuples_str(_get_num_list_corr_types(polygon_points))

    return '(%s)' % intermed_str


def _java_object_to_str(java_obj, idx):
    return java_obj.getString(idx)


def _bytes_to_memoryview(java_obj, idx):
    # java_obj.getValue should return a value of type bytes in this case
    return memoryview(java_obj.getValue(idx))


def _json_str_to_dict(java_obj, idx):
    return json.loads(java_obj.getString(idx))


def _float_to_money_str(java_obj, idx):
    return '${:.2f}'.format(java_obj.getValue(idx))


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


def _xml_to_str(java_obj, idx):
    return java_obj.getSQLXML(idx).getString()


# NOTE: All functions in this map must take two arguments, the first
# being a JavaObject representing a ResultSet, and the second being the index
# of the column the value to parse resides in in the ResultSet.
_typename_to_converter_fxn = {
    'bit': _bool_to_str,
    'varbit': _java_object_to_str,
    'box': _box_to_str,
    'bytea': _bytes_to_memoryview,
    'cidr': _java_object_to_str,
    'circle': _circle_to_str,
    'date': _str_to_date,
    'inet': _java_object_to_str,
    'json': _json_str_to_dict,
    'line': _line_to_str,
    'lseg': _line_seg_to_str,
    'macaddr': _java_object_to_str,
    'macaddr8': _java_object_to_str,
    'money': _float_to_money_str,
    'path': _path_to_str,
    'point': _point_to_str,
    'polygon': _polygon_to_str,
    'time': _str_to_time,
    'timestamp': _str_to_datetime,
    'uuid': _java_object_to_str,
    'xml': _xml_to_str,
    'timetz': _str_to_timetz,
    'timestamptz': _str_to_datetimetz,
}


class PostgresConverter(DatatypeConverterBase):
    """
    Reads the value in a type-sensitive way

    Due to the reliance on the underlying JDBC library, we cannot always
    distinguish the exact type of the underlying data; we can only set
    the type according to the type indentifier as indicated by the JDBC
    library itself.  We note that the following type conversions are taken
    from psycopg2, the most commonly used python interface for postgres.

    Type conversion rule (Postgres):

    'bigint'                    => int,
    'bigserial'                 => int,
    'bit'               => bool => str,
    'varbit'      => JavaObject => str,
    'boolean'                   => bool,
    'box'         => JavaObject => str,
    'bytea'            => bytes => memoryview,
    'char'                      => str,
    'varchar'                   => str,
    'cidr'        => JavaObject => str,
    'circle'      => JavaObject => str,
    'date'        => JavaObject => datetime.date,
    'float8'                    => float,
    'inet'        => JavaObject => str,
    'integer'                   => int,
    'json'        => JavaObject => dict,
    'line'        => JavaObject => str,
    'lseg'        => JavaObject => str,
    'macaddr'     => JavaObject => str,
    'macaddr8'    => JavaObject => str,
    'money'            => float => str,
    'numeric'                   => decimal.Decimal,
    'path'        => JavaObject => str,
    'point'       => JavaObject => str,
    'polygon'     => JavaObject => str,
    'real'                      => float,
    'smallint'                  => int,
    'smallserial'               => int,
    'serial'                    => int,
    'text'                      => str,
    'time'        => JavaObject => datetime.time,
    'timestamp'   => JavaObject => datetime.datetime,
    'uuid'        => JavaObject => str,
    'xml'         => JavaObject => str,
    'bit'         => JavaObject => str,
    'int8'                      => int,
    'bool'                      => bool,
    'character'                 => str,
    'character'                 => str,
    'int'                       => int,
    'int4'                      => int,
    'double'                    => float,
    'decimal'                   => decimal.Decimal,
    'float'                     => float,
    'int2'                      => int,
    'serial2'                   => int,
    'serial4'                   => int,
    'timetz'      => JavaObject => datetime.time,
    'timestamptz' => JavaObject => datetime.datetime,
    'serial8'                   => int

    Time-related Java objects are read as py4j.JavaObject by default.
    To avoid this, we read them as str and convert to an appropriate
    python object.

    Args
        resultset: the Java result set currently in process
        index: zero-based index of the column to read
        col_type: column type in str
    """

    @staticmethod
    def read_value(resultset, index, col_typename):
        if col_typename in _typename_to_converter_fxn:
            if resultset.getString(index) is None:
                return None

            return _typename_to_converter_fxn[col_typename](resultset, index)
        else:
            return resultset.getValue(index)

