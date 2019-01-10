import psycopg2.tz
import json
import datetime
import dateutil
import dateutil.tz

NUM_MINUTES_IN_DAY = 1440

_utc_offset_timedelta = datetime.datetime.now(dateutil.tz.tzlocal()).utcoffset()
utc_offset_min = (_utc_offset_timedelta.seconds // 60) - NUM_MINUTES_IN_DAY

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
    return psycopg2.tz.FixedOffsetTimezone(
        offset=utc_offset_min,
        name=None,
    )


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

def read_value(resultset, index, col_typename):
    if col_typename in _typename_to_converter_fxn:
        if resultset.getString(index) is None:
            return None

        return _typename_to_converter_fxn[col_typename](resultset, index)
    else:
        return resultset.getValue(index)

