# -*- coding: utf-8 -*-


class TableHasNoPrimaryKey(Exception):
    pass


class NoReferenceFound(Exception):
    pass


class InvalidSkipColumns(Exception):
    pass


class VariableError(Exception):
    pass


class InvalidDefinition(Exception):
    pass


class InvalidColumn(Exception):
    pass