# -*- coding: utf-8 -*-


def table_parts(self, name: str) -> (str, str):
    """
    Returns a tuple with schema and table names
    :param self:
    :param name:
    :return:
    """
    if '.' in name:
        return name.split('.', 1)
    else:
        return 'public', name
