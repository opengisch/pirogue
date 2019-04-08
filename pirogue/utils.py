# -*- coding: utf-8 -*-


def table_parts(name: str) -> (str, str):
    """
    Returns a tuple with schema and table names
    :param name:
    :return:
    """
    if '.' in name:
        return name.split('.', 1)
    else:
        return 'public', name


def list2str(elements: list, sep: str= ', ', prepend: str='', append: str='') -> list:
    """
    Prepend to all strings in the list
    :param elements:
    :param sep: separator
    :param prepend:
    :param append:
    :return:
    """
    return sep.join([prepend+x+append for x in elements])
