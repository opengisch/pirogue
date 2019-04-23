# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from pirogue.utils import table_parts, column_alias, select_columns, insert_command, update_command
from pirogue.information_schema import TableHasNoPrimaryKey, NoReferenceFound, \
    columns, reference_columns, primary_key, default_value


class ReferencedTableDefinedBeforeReferencing(Exception):
    pass

class InvalidDefinition(Exception):
    pass

class VariableError(Exception):
    pass


class TableDef(object):
    table_schema = ''
    table_name = ''
    table_type = 'table'
    table_alias = None
    pkey = None  # determined automatically if not provided
    skip_columns = []
    comment_skipped = True
    remap_columns = {}
    prefix: str = None

    def __init__(self, table_schema: str, table_name: str):
        self.table_schema = table_schema
        self.table_name = table_name


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
            if key not in ('table', 'view_schema',
                           'view_name', 'alias', 'short_alias',
                           'type_name', 'joins',
                           'insert_trigger', 'update_trigger',
                           'allow_type_change', 'allow_parent_only'):
                raise InvalidDefinition('key {k} is not a valid'.format(k=key))
        # check joins validity
        for alias, table_def in definition['joins'].items():
            for key in table_def.keys():
                if key not in ('table', 'short_alias',
                               'skip_columns', 'fkey',
                               'remap_columns', 'prefix', 'insert_values', 'update_values'):
                    raise InvalidDefinition('in join {a} key "{k}" is not valid'.format(a=alias, k=key))
        for mandatory_key in ['joins']:
            if mandatory_key not in definition:
                raise InvalidDefinition('Missing key: "{k}" should be provided.'.format(k=mandatory_key))
        # check trigger modifiers validity
        for trigger in ('insert_trigger', 'update_trigger'):
            for key in definition.get(trigger, {}):
                if key not in ('declare', 'pre', 'post'):
                    raise InvalidDefinition('key {k} is not valid in trigger definitions'.format(k=key))

        (self.master_schema, self.master_table) = table_parts(definition.get('table', None))
        self.master_skip_colums = definition.get('skip_columns', [])
        self.master_prefix = definition.get('prefix', None)
        self.master_remap_columns = definition.get('remap_columns', {})

        # global options:
        self.view_schema = definition.get('view_schema', self.master_schema)
        self.view_name = definition.get('view_name', "vw_merge_{t}".format(t=self.master_table))
        self.view_alias = definition.get('alias', self.master_table)
        self.short_alias = definition.get('short_alias', self.view_alias)
        self.type_name = definition.get('type_name', '{al}_type'.format(al=self.view_alias))
        self.insert_trigger = definition.get('insert_trigger', {})
        self.update_trigger = definition.get('update_trigger', {})
        self.allow_parent_only = definition.get('allow_parent_only', False)
        self.allow_type_change = definition.get('allow_type_change', True)

        try:
            self.master_pkey = primary_key(self.cursor, self.master_schema, self.master_table)
        except TableHasNoPrimaryKey:
            raise TableHasNoPrimaryKey('{vn} has no primary key, specify it with "key"'.format(vn=self.view_alias))

        # parse the joins definition
        self.joins = definition['joins']
        self.joined_ref_master_key = []
        for alias, table_def in self.joins.items():
            (table_def['table_schema'], table_def['table_name']) = table_parts(table_def['table'])
            table_def['short_alias'] = table_def.get('short_alias', alias)

            if 'fkey' in table_def:
                table_def['ref_master_key'] = table_def['fkey']
            else:
                table_def['ref_master_key'] = reference_columns(self.cursor,
                                                                table_def['table_schema'], table_def['table_name'],
                                                                self.master_schema, self.master_table)[0]
            try:
                table_def['pkey'] = primary_key(self.cursor, table_def['table_schema'], table_def['table_name'])
            except TableHasNoPrimaryKey:
                table_def['pkey'] = table_def['ref_master_key']

    def create(self) -> bool:
        """

        :return:
        """

        for sql in [self.__view(),
                    self.__insert_trigger(),
                    self.__update_trigger(),
                    self.__delete_trigger()]:
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

    def __view(self) -> str:
        """
        :return:
        """

        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS
  SELECT
    CASE
      {types}
      ELSE {no_subtype}
    END AS {type_name},
    {master_columns},
    {joined_columns}
  FROM {mt}.{ms} {sa}
    {joined_tables};        
""".format(vs=self.view_schema,
           vn=self.view_name,
           types='\n      '.join(["WHEN {shal}.{mrf} IS NOT NULL THEN '{al}'::text"
                                 .format(shal=table_def['short_alias'], mrf=table_def['ref_master_key'], al=alias)
                                  for alias, table_def in self.joins.items()]),
           no_subtype="'unknown'::text",
           type_name=self.type_name,
           master_columns=select_columns(self.cursor, self.master_schema, self.master_table,
                                         table_alias=self.view_alias,
                                         skip_columns=self.master_skip_colums,
                                         prefix=self.master_prefix,
                                         remap_columns=self.master_remap_columns,
                                         indent=4),
           joined_columns=', '.join([select_columns(self.cursor, table_def['table_schema'], table_def['table_name'],
                                                    table_alias=table_def['short_alias'],
                                                    skip_columns=table_def.get('skip_columns', [])+[table_def['ref_master_key']],
                                                    prefix=table_def.get('prefix', None),
                                                    remove_pkey=False,
                                                    remap_columns=table_def.get('remap_columns', {}),
                                                    indent=4)
                                     for alias, table_def in self.joins.items()]),
           mt=self.master_schema,
           ms=self.master_table,
           sa=self.short_alias,
           joined_tables='\n    '.join(["LEFT JOIN {tbl} {tal} ON {tal}.{rmk} = {msa}.{mpk}"
                                            .format(tbl=table_def['table'],
                                                    tal=table_def['short_alias'],
                                                    rmk=table_def['ref_master_key'],
                                                    msa=self.short_alias,
                                                    mpk=self.master_pkey)
                                            for table_def in self.joins.values()])
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

  CASE 
    {insert_joins}
  ELSE
    {raise_notice}
  END CASE;

  {insert_trigger_post}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_insert ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_insert
    INSTEAD OF INSERT ON {vs}.{vn}
    FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_insert();
""".format(vs=self.view_schema,
           vn=self.view_name,
           declare='\n  '.join(['{d};'.format(d=declare) for declare in self.insert_trigger.get('declare', [])]),
           insert_trigger_pre=self.insert_trigger.get('pre', ''),
           insert_master=insert_command(self.cursor, self.master_schema, self.master_table,
                                        skip_columns=self.master_skip_colums,
                                        prefix=self.master_prefix,
                                        remap_columns=self.master_remap_columns,
                                        remove_pkey=False,
                                        indent=8,
                                        coalesce_pkey_default=True,
                                        returning='{mpk} INTO NEW.{mpk}'.format(mpk=self.master_pkey)),
           insert_joins='\n    '.join(["WHEN NEW.{type_name} = '{alias}' THEN"
                                       "\n      {insert_join}".format(type_name=self.type_name,
                                                                      alias=alias,
                                                                      insert_join=insert_command(self.cursor,
                                                                                                 table_def['table_schema'],
                                                                                                 table_def['table_name'],
                                                                                                 table_alias=table_def['short_alias'],
                                                                                                 skip_columns=table_def.get('skip_columns', []),
                                                                                                 prefix=table_def.get('prefix', None),
                                                                                                 insert_values={**{table_def['ref_master_key']: 'NEW.{c}'.format(c=self.master_pkey)},
                                                                                                                **table_def.get('insert_values', {})},
                                                                                                 remap_columns=table_def.get('remap_columns', {}),
                                                                                                 remove_pkey=False,
                                                                                                 indent=4))
                                   for alias, table_def in self.joins.items()]),
           raise_notice='NULL;' if self.allow_parent_only else "RAISE NOTICE '{vn} type not known ({percent_char})', NEW.{type_name}; -- ERROR"
                            .format(vn=self.view_name,
                                    percent_char='%%' if self.variables else '%',  # if variables, % should be escaped because cursor.execute is run with variables
                                    type_name=self.type_name),
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

  IF OLD.{type_name} <> NEW.{type_name} THEN
    {type_change}
  END IF;

  CASE {update_joins}
  ELSE
     {raise_notice}
  END CASE;
  {update_trigger_post}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_update ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_update
    INSTEAD OF update ON {vs}.{vn}
    FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_update();
        """.format(vs=self.view_schema,
                   vn=self.view_name,
                   declare='\n  '.join(['{d};'.format(d=declare) for declare in self.update_trigger.get('declare', [])]),
                   update_trigger_pre=self.update_trigger.get('pre', ''),
                   update_master=update_command(self.cursor, self.master_schema, self.master_table,
                                                skip_columns=self.master_skip_colums,
                                                prefix=self.master_prefix,
                                                remap_columns=self.master_remap_columns,
                                                indent=8),
                   type_name=self.type_name,
                   type_change="RAISE EXCEPTION 'Type change not allowed for {alias}'"
                               "\n      USING HINT = 'You cannot switch from ' "
                               "|| OLD.{type_name} || ' to ' || NEW.{type_name};"
                                .format(alias=self.view_alias, type_name=self.type_name)
                               if not self.allow_type_change else
                               "CASE"
                               "\n      {deletes}"
                               "\n    END CASE;"
                               "\n    CASE"
                               "\n      {inserts}"
                               "\n      ELSE -- do nothing"
                               "\n    END CASE;"
                               .format(deletes='\n      '.join(["WHEN OLD.{type_name} = '{alias}' "
                                                                "THEN DELETE FROM {ts}.{tn} "
                                                                "WHERE {rmk} = OLD.{mpk};"
                                                                .format(type_name=self.type_name,
                                                                        alias=alias,
                                                                        ts=table_def['table_schema'],
                                                                        tn=table_def['table_name'],
                                                                        rmk=table_def['ref_master_key'],
                                                                        mpk=self.master_pkey)
                                                                for alias, table_def in self.joins.items()]),
                                       inserts='\n      '.join(["WHEN NEW.{type_name} = '{alias}' "
                                                                "THEN INSERT INTO {ts}.{tn} "
                                                                "({rmk}) VALUES (OLD.{mpk});"
                                                               .format(type_name=self.type_name,
                                                                       alias=alias,
                                                                       ts=table_def['table_schema'],
                                                                       tn=table_def['table_name'],
                                                                       rmk=table_def['ref_master_key'],
                                                                       mpk=self.master_pkey)
                                                                for alias, table_def in self.joins.items()])),
                   update_joins='\n    '.join(["WHEN NEW.{type_name} = '{alias}' THEN"
                                               "\n      {update_join}".format(type_name=self.type_name,
                                                                              alias=alias,
                                                                              update_join=update_command(self.cursor, table_def['table_schema'], table_def['table_name'],
                                                                                                         table_alias=table_def['short_alias'],
                                                                                                         pkey=table_def['pkey'],
                                                                                                         skip_columns=table_def.get('skip_columns', []),
                                                                                                         prefix=table_def.get('prefix', None),
                                                                                                         remap_columns=table_def.get('remap_columns', {}),
                                                                                                         update_values={**{table_def['pkey']: 'OLD.{c}'.format(c=self.master_pkey)},
                                                                                                                        **table_def.get('update_values', {})},
                                                                                                         indent=4))
                                   for alias, table_def in self.joins.items()]),
                   raise_notice='NULL;' if self.allow_parent_only
                                else "RAISE NOTICE '{vn} type not known ({percent_char})', NEW.{type_name}; -- ERROR"
                                     .format(vn=self.view_name,
                                             percent_char='%%' if self.variables else '%',  # if variables, % should be escaped because cursor.execute is run with variables
                                             type_name=self.type_name),
                   update_trigger_post=self.update_trigger.get('post', ''))
        return sql

    def __delete_trigger(self):
        sql = """
CREATE FUNCTION {vs}.ft_{vn}_delete() RETURNS trigger AS
    $BODY$
    BEGIN
    CASE
        {deletes}
    END CASE;
    DELETE FROM {ts}.{tn} WHERE {mpk} = OLD.{mpk};
    RETURN NULL;
    END;
    $BODY$
    LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS tr_{vn}_on_delete ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_delete
    INSTEAD OF DELETE ON {vs}.{vn}
    FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_delete();
""".format(vn=self.view_name,
           deletes='\n      '.join(["WHEN OLD.{type_name} = '{alias}' "
                                    "THEN DELETE FROM {ts}.{tn} "
                                    "WHERE {rmk} = OLD.{mpk};"
                                   .format(type_name=self.type_name,
                                           alias=alias,
                                           ts=table_def['table_schema'],
                                           tn=table_def['table_name'],
                                           rmk=table_def['ref_master_key'],
                                           mpk=self.master_pkey)
                                    for alias, table_def in self.joins.items()]),
           ts=self.master_schema,
           tn=self.master_table,
           mpk=self.master_pkey,
           vs=self.view_schema)
        return sql