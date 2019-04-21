# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from pirogue.utils import table_parts, list2str, update_columns, column_alias
from pirogue.information_schema import TableHasNoPrimaryKey, NoReferenceFound, \
    columns, reference_columns, primary_key, default_value


class ReferencedTableDefinedBeforeReferencing(Exception):
    pass

class InvalidDefinition(Exception):
    pass

class VariableError(Exception):
    pass


class Merge:
    """
    Creates a merge view with associated triggers to edit.
    """

    def __init__(self, definition: dict, pg_service: str = None, variables: dict = {}):
        """
        Produces the SQL code of the join table and triggers
        :param definition: the YAML definition of the merge view
        :param pg_service:
        :param variables: dictionary for variables to be used in SQL deltas ( name => value )
        """

        self.variables = variables

        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cursor = self.conn.cursor()

        # check definition validity
        for key in definition.keys():
            if key not in ('table', 'sql_definition', 'fkey_is_pkey', 'view_schema',
                           'view_name', 'alias', 'short_alias',
                           'type_name', 'columns', 'key', 'joins',
                           'columns_on_top', 'insert_trigger', 'update_trigger'):
                raise InvalidDefinition('key {k} is not a valid'.format(k=key))
        # check joins validity
        for alias, table_def in definition['joins'].items():
            for key in table_def.keys():
                if key not in ('table', 'short_alias', 'columns',
                               'skip_columns', 'fkey', 'is_type',
                               'referenced_by', 'referenced_by_key',
                               'remap_columns', 'prefix', 'columns_on_top',
                               'columns_at_end', 'columns_no_insert_or_update',
                               'insert_values'):
                    raise InvalidDefinition('in join {a} key "{k}" is not valid'.format(a=alias, k=key))
        if 'sql_definition' not in definition and 'table' not in definition:
            raise InvalidDefinition('Missing key: "table" or "sql_definition" should be provided.')
        if 'sql_definition' in definition and 'table' in definition:
            raise InvalidDefinition('Key error: "table" and "sql_definition" cannot be provided both.')
        if 'sql_definition' in definition and 'columns' not in definition:
            raise InvalidDefinition('If a SQL definition is given instead of a table, specifying the columns is required (TODO: with PG11+, it will solvable).')
        for mandatory_key in ['joins']:
            if mandatory_key not in definition:
                raise InvalidDefinition('Missing key: "{k}" should be provided.'.format(k=mandatory_key))
        # check trigger modifiers validity
        for trigger in ('insert_trigger', 'update_trigger'):
            for key in definition.get(trigger, {}):
                if key not in ('declare', 'pre', 'post'):
                    raise InvalidDefinition('key {k} is not valid in trigger definitions'.format(k=key))


        (self.master_schema, self.master_table) = table_parts(definition.get('table', None))

        # global options:
        self.sql_definition = definition.get('sql_definition', None)
        self.view_schema = definition.get('view_schema', self.master_schema)
        self.view_name = definition.get('view_name', "vw_merge_{t}".format(t=self.master_table))
        self.view_alias = definition.get('alias', self.master_table)
        self.short_alias = definition.get('short_alias', self.view_alias)
        self.type_name = definition.get('type_name', '{al}_type'.format(al=self.view_alias))
        self.fkey_is_pkey = definition.get('fkey_is_pkey', False)
        self.insert_trigger = definition.get('insert_trigger', {})
        self.update_trigger = definition.get('update_trigger', {})

        try:
            self.master_pkey = definition.get('key', None) or primary_key(self.cursor, self.master_schema, self.master_table)
        except TableHasNoPrimaryKey:
            raise TableHasNoPrimaryKey('{vn} has no primary key, specify it with "key"'.format(vn=self.view_alias))

        # columns are required if the definition is not a table but SQL definition.
        # with PG 11+ we could use \gdesc to find out the columns out of the request.
        # see https://www.depesz.com/2017/09/21/waiting-for-postgresql-11-add-gdesc-psql-command/
        self.master_cols = definition.get('columns', None) or columns(self.cursor, self.master_schema, self.master_table)
        self.master_cols_wo_pkey = list(self.master_cols)  # duplicate otherwise keeps reference
        if self.master_pkey in self.master_cols_wo_pkey:
            self.master_cols_wo_pkey.remove(self.master_pkey)


        # create a dictionnary so it can be appended to joined tables
        self.main_table_def = {self.view_alias: {'cols': self.master_cols,
                                                 'cols_wo_ref_key': self.master_cols,
                                                 'short_alias': self.short_alias,
                                                 'columns_on_top': definition.get('columns_on_top', [])}}

        # parse the joins definition
        self.joins = definition['joins']
        self.joined_ref_master_key = []
        for alias, table_def in self.joins.items():
            (table_def['table_schema'], table_def['table_name']) = table_parts(table_def['table'])
            table_def['short_alias'] = table_def.get('short_alias', alias)
            table_def['cols'] = table_def.get('columns', None) or columns(self.cursor, table_def['table_schema'],
                                                                          table_def['table_name'],
                                                                          skip_columns=table_def.get('skip_columns', []))
            table_def['cols_insert_update'] = [col for col in table_def['cols']
                                               if col not in table_def.get('columns_no_insert_or_update', [])]
            for col in table_def.get('insert_values', {}):
                if col not in table_def['cols_insert_update']:
                    table_def['cols_insert_update'].append(col)


            if 'fkey' in table_def:
                table_def['ref_master_key'] = table_def['fkey']
            else:
                try:
                    table_def['ref_master_key'] = reference_columns(self.cursor,
                                                                    table_def['table_schema'], table_def['table_name'],
                                                                    self.master_schema, self.master_table)[0]
                except NoReferenceFound as e:
                    # no reference found (this is probably because using a SQL definition instead of a proper table
                    # defaulting to primary key if allowed by global option
                    if self.fkey_is_pkey:
                        try:
                            table_def['ref_master_key'] = primary_key(self.cursor, table_def['table_schema'], table_def['table_name'])
                        except TableHasNoPrimaryKey:
                            raise NoReferenceFound('{ts}.{tn} has no reference to {ms}.{mt}'.format(ts=table_def['table_schema'],
                                                                                                    tn=table_def['table_name'],
                                                                                                    ms=self.master_schema,
                                                                                                    mt=self.master_table))
                    else:
                        raise e
            try:
                table_def['pkey'] = primary_key(self.cursor, table_def['table_schema'], table_def['table_name'])
            except TableHasNoPrimaryKey:
                table_def['pkey'] = table_def['ref_master_key']

            # make a copy, otherwise keeps reference
            table_def['cols_wo_ref_key'] = [col for col in table_def['cols'] if col != table_def['ref_master_key']]
            table_def['cols_insert_update_wo_ref_key'] = [col for col in table_def['cols_insert_update'] if col != table_def['ref_master_key']]

            # fix lower levels references (table not linked to the master table)
            if 'referenced_by' in table_def:
                table_def['reference_master_table'] = table_def.get('is_type', False)
                try:
                    table_def['referenced_by_key'] = table_def.get('referenced_by_key', None) or \
                                                     reference_columns(self.cursor,
                                                                       self.joins[table_def['referenced_by']]['table_schema'],
                                                                       self.joins[table_def['referenced_by']]['table_name'],
                                                                       table_def['table_schema'], table_def['table_name'])[0]
                except NoReferenceFound as e:
                    if self.fkey_is_pkey:
                        table_def['referenced_by_key'] = self.joins[table_def['referenced_by']]['pkey']
                    else:
                        raise e
                except KeyError:
                    raise ReferencedTableDefinedBeforeReferencing('"{rd}" should be defined after "{rg}"'.format(rd=alias,
                                                                                                                 rg=table_def['referenced_by']))
            else:
                table_def['reference_master_table'] = table_def.get('is_type', True)
                table_def['referenced_by'] = self.view_alias
                table_def['referenced_by_key'] = table_def.get('referenced_by_key', None) or self.master_pkey

            # control existence of columns
            for dct in ('remap_columns', 'insert_values', 'skip_columns',
                        'columns_on_top', 'columns_at_end', 'columns',
                        'columns_no_insert_or_update'):
                for col in table_def.get(dct, {}):
                    if col not in columns(self.cursor, table_def['table_schema'], table_def['table_name']):
                        raise InvalidDefinition('In {dct}, column "{col}" '
                                                'does not exist for table {al}'.format(dct=dct, col=col, al=alias))

    def create(self) -> bool:
        """

        :return:
        """
        for sql in [self.__view(),
                    self.__insert_trigger(),
                    self.__update_trigger()]:
            try:
                if self.variables:
                    self.cursor.execute(sql, self.variables)
                else:
                    self.cursor.execute(sql)
            except TypeError as e:
                print("*** Failing:\n{}\n***".format(sql))
                raise VariableError("An error in a SQL variable is probable. "
                                    "Check the variables in the SQL code "
                                    "(were given: {svars}). "
                                    "Also, any % character shall be escaped with %%"
                                    .format(svars=list(self.variables.keys())))
            except psycopg2.Error as e:
                print("*** Failing:\n{}\n***".format(sql))
                raise e
        self.conn.commit()
        self.conn.close()
        return True

    def column_values(self, table_def: dict, columns: list) -> list:
        """

        :param table_def:
        :param columns:
        :return:
        """
        values = []
        manual_values = table_def.get('insert_values', {})
        for col in columns:
            values.append(manual_values.get(col, 'NEW.{cal}'.format(cal=column_alias(col,
                                                                                     remap_columns=table_def.get('remap_columns', {}),
                                                                                     prefix=table_def.get('prefix', None),
                                                                                     field_if_no_alias=True))))
        return values

    @staticmethod
    def column_priority(table_def: dict, column: str) -> int:
        if column in table_def.get('columns_on_top', []):
            return 0
        elif column in table_def.get('columns_at_end', []):
            return 2
        else:
            return 1

    def __view(self) -> str:
        """
        :return:
        """

        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS
  SELECT
    CASE
      {types}
      ELSE 'unknown'::text
    END AS {type_name}{columns}
  FROM {mt} {sa}
    {joined_tables};        
""".format(vs=self.view_schema,
           vn=self.view_name,
           types=list2str(elements=["WHEN {shal}.{mrf} IS NOT NULL THEN '{al}'::text"
                                    .format(shal=table_def['short_alias'], mrf=table_def['ref_master_key'],al=alias)
                                    for alias, table_def in self.joins.items() if table_def['reference_master_table']],
                          sep='\n      '),
           type_name=self.type_name,
           columns=list2str(['{table_alias}.{column}{col_alias}'.format(table_alias=table_def['short_alias'],
                                                                        column=col,
                                                                        col_alias=column_alias(col,
                                                                                               remap_columns=table_def.get('remap_columns', {}),
                                                                                               prefix=table_def.get('prefix', None),
                                                                                               prepend_as=True))
                             for (alias, table_def, col)
                             in sorted([
                                (alias, table_def, col)
                                for alias, table_def in {**self.main_table_def, **self.joins}.items()
                                for col in table_def['cols_wo_ref_key']
                                # sort columns by 'columns_on_top'/normal/'columns_at_end' first, then by table, then y column order
                                ], key=lambda x: (self.column_priority(x[1], x[2]),
                                                  list(self.joins.keys()).index(x[0])+1 if x[0] in self.joins else 0,
                                                  x[1]['cols'].index(x[2]))
                             )],
                            prepend='\n    ', prepend_to_list=','),
           mt=self.sql_definition or '{mt}.{ms}'.format(mt=self.master_schema, ms=self.master_table),
           sa=self.short_alias,
           joined_tables=list2str(elements=["LEFT JOIN {tb} {al} ON {al}.{rmk} = {rba}.{mpk}"
                                            .format(al=table_def['short_alias'],
                                                    tb=table_def['table'],
                                                    rmk=table_def['ref_master_key'],
                                                    rba={**self.main_table_def, **self.joins}[table_def['referenced_by']]['short_alias'],
                                                    mpk=table_def['referenced_by_key']) for table_def in self.joins.values()],
                                  sep='\n    ')
                   )
        return sql

    def __insert_trigger(self) -> str:
        """

        :return:
        """
        sql = """-- INSERT TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_insert() RETURNS trigger AS
$BODY$
DECLARE
  {declare}
BEGIN
  {insert_trigger_pre}
  {insert_master}
  {insert_joins_wo_type}
  
  CASE {insert_type_joins}
  ELSE
     RAISE NOTICE '{vn} type not known ({percent_char})', NEW.{type_name}; -- ERROR
  END CASE;
  {insert_trigger_post}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER tr_{vn}_on_insert
INSTEAD OF INSERT ON {vs}.{vn}
FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_insert();
""".format(vs=self.view_schema,
           vn=self.view_name,
           declare=list2str(self.insert_trigger.get('declare', []), append=';', sep='\n  '),
           insert_trigger_pre=self.insert_trigger.get('pre', ''),
           insert_master='' if self.sql_definition else """
  INSERT INTO {ms}.{mt} ( {master_cols} )
    VALUES (
      COALESCE( NEW.{mpk}, {mkp_def} ),  {master_new_cols} );"""
                         .format(ms=self.master_schema,
                                 mt=self.master_table,
                                 mpk=self.master_pkey,
                                 master_cols=list2str(self.master_cols, prepend='\n        '),
                                 mkp_def=default_value(self.cursor, self.master_schema, self.master_table, self.master_pkey),
                                 master_new_cols=list2str(self.master_cols_wo_pkey, prepend='\n        NEW.', append='')),
           insert_joins_wo_type=list2str(["""
  INSERT INTO {js}.{jt}(
      {jpk},{join_cols} ) 
    VALUES ( 
      COALESCE( {jpk_val}, {jpk_def} ),  {join_new_cols} );"""
                                .format(js=table_def['table_schema'],
                                        jt=table_def['table_name'],
                                        jpk=table_def['pkey'],
                                        join_cols=list2str([col for col in table_def['cols_insert_update'] if col != table_def['pkey']], prepend='\n      '),
                                        jpk_val=table_def.get('insert_values', {}).get(table_def['pkey'], 'NEW.{jpk}'.format(jpk=column_alias(table_def['pkey'],
                                                                                                                                              remap_columns=table_def.get('remap_columns', {}),
                                                                                                                                              prefix=table_def.get('prefix', None),
                                                                                                                                              field_if_no_alias=True))),
                                        jpk_def=default_value(self.cursor, table_def['table_schema'], table_def['table_name'], table_def['pkey']),
                                        join_new_cols=list2str(self.column_values(table_def, table_def['cols_insert_update_wo_ref_key']), prepend='\n      ', append=''))
            for alias, table_def in self.joins.items() if not table_def.get('is_type', True)], sep='\n'),
           insert_type_joins=list2str(["""
    WHEN NEW.ws_type = '{alias}' THEN
      INSERT INTO {js}.{jt}( 
        {jpk},{join_cols} ) 
      VALUES ( 
        NEW.{jpk_val},  {join_new_cols} );"""
                                .format(alias=alias,
                                        js=table_def['table_schema'],
                                        jt=table_def['table_name'],
                                        jpk=table_def['pkey'],
                                        join_cols=list2str([col for col in table_def['cols_insert_update'] if col != table_def['pkey']], prepend='\n        '),
                                        jpk_val=column_alias(table_def['referenced_by_key'],
                                                             remap_columns={**self.main_table_def, **self.joins}[table_def['referenced_by']].get('remap_columns', {}),
                                                             prefix={**self.main_table_def, **self.joins}[table_def['referenced_by']].get('prefix', None),
                                                             field_if_no_alias=True),
                                        join_new_cols=list2str(self.column_values(table_def, table_def['cols_insert_update_wo_ref_key']), prepend='\n        ', append=''))
            for alias, table_def in self.joins.items() if table_def.get('is_type', True)], sep='\n'),
           percent_char='%%' if self.variables else '%',  # if variables, % should be escaped because cursor.execute is run with variables
           type_name=self.type_name,
           insert_trigger_post=self.insert_trigger.get('post', ''))
        return sql


    def __update_trigger(self):
        sql = """-- UPDATE TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_update() RETURNS trigger AS
$BODY$
DECLARE
  {declare}
BEGIN
  {update_trigger_pre}
  {update_master}
  {update_joins_wo_type}

  CASE {update_type_joins}
  ELSE
     RAISE NOTICE '{vn} type not known ({percent_char})', NEW.{type_name}; -- ERROR
  END CASE;
  {update_trigger_post}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER tr_{vn}_on_update
INSTEAD OF update ON {vs}.{vn}
FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_update();
        """.format(vs=self.view_schema,
                   vn=self.view_name,
                   declare=list2str(self.update_trigger.get('declare', []), append=';', sep='\n  '),
                   update_trigger_pre=self.update_trigger.get('pre', ''),
                   update_master='' if self.sql_definition else """
  UPDATE {ms}.{mt} SET {master_cols} 
    WHERE {mpk} = OLD.{mpk};"""
                   .format(ms=self.master_schema,
                           mt=self.master_table,
                           master_cols=update_columns(self.master_cols_wo_pkey, sep='\n    , '),
                           mpk=self.master_pkey),
                   update_joins_wo_type=list2str(["""
  UPDATE {js}.{jt} SET {join_cols}
    WHERE {jpk} = OLD.{jpk};"""
                                                 .format(js=table_def['table_schema'],
                                                         jt=table_def['table_name'],
                                                         jpk=table_def['pkey'],
                                                         join_cols=list2str(
                                                             ['{col} = {al}'
                                                                  .format(col=col,
                                                                          al=table_def.get('insert_values', {}).get(col, 'NEW.{cal}'.format(cal=column_alias(col, remap_columns=table_def.get('remap_columns', {}), prefix=table_def.get('prefix', None), field_if_no_alias=True))))
                                                              for col in table_def['cols_insert_update'] if
                                                              col != table_def['pkey']], prepend='\n    '))
                                                  for alias, table_def in self.joins.items() if
                                                  not table_def.get('is_type', True)], sep='\n'),
                   update_type_joins="WHEN FALSE THEN NULL",
                   percent_char='%%' if self.variables else '%',
                   # if variables, % should be escaped because cursor.execute is run with variables
                   type_name=self.type_name,
                   update_trigger_post=self.update_trigger.get('post', ''))
        print(sql)
        return sql
