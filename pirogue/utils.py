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


def list2str(elements: list, sep: str= ', ', prepend: str='', append: str='', prepend_to_list: str='') -> str:
    """
    Prepend to all strings in the list
    :param elements:
    :param sep: separator
    :param prepend:
    :param append:
    :param prepend_to_list: prepend to the return string, if elements is not None or empty
    :return:
    """
    if elements is None or len(elements) == 0:
        return ''
    return prepend_to_list + sep.join([prepend+x+append for x in elements])


def update_columns(columns: list, sep:str=', ') -> str:
    return sep.join(["{c} = NEW.{c}".format(c=col) for col in columns])
