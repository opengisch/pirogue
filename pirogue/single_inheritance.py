import psycopg

from pirogue.exceptions import TableHasNoPrimaryKey
from pirogue.information_schema import default_value, primary_key, reference_columns
from pirogue.utils import insert_command, select_columns, table_parts, update_command


class SingleInheritance:
    """
    Creates a join view with associated triggers to edit for a single inheritance.
    """

    def __init__(
        self,
        *,
        connection: psycopg.Connection,
        parent_table: str,
        child_table: str,
        view_schema: str = None,
        view_name: str = None,
        pkey_default_value: bool = False,
        inner_defaults: dict = {},
    ):
        """
        Produces the SQL code of the join table and triggers

        Parameters
        ----------
        connection
            a psycopg.Connection instance
        parent_table
            the parent table, can be schema specified
        child_table
            the child table, can be schema specified
        view_schema
            the schema where the view will written to
        view_name
            the name of the created view, defaults to vw_{parent_table}_{join_table}
        pkey_default_value
            the primary key column of the view will have a default value according to the child primary key table
        inner_defaults
            dictionary of other columns to default to in case the provided value is null or empty
        """

        self.conn = connection

        self.pkey_default_value = pkey_default_value
        self.inner_defaults = inner_defaults

        (self.parent_schema, self.parent_table) = table_parts(parent_table)
        (self.child_schema, self.child_table) = table_parts(child_table)

        if view_schema is None:
            if self.parent_schema != self.child_schema:
                raise ValueError(
                    "Destination schema cannot be guessed if different on sources tables."
                )
            else:
                self.view_schema = self.parent_schema
        else:
            self.view_schema = view_schema

        self.view_name = view_name or "vw_{pt}_{ct}".format(
            pt=self.parent_table, ct=self.child_table
        )

        (self.ref_parent_key, parent_referenced_key) = reference_columns(
            self.conn,
            self.child_schema,
            self.child_table,
            foreign_table_schema=self.parent_schema,
            foreign_table_name=self.parent_table,
        )
        try:
            self.child_pkey = primary_key(self.conn, self.child_schema, self.child_table)
        except TableHasNoPrimaryKey:
            self.child_pkey = self.ref_parent_key
        self.parent_pkey = primary_key(self.conn, self.parent_schema, self.parent_table)

        assert self.parent_pkey == parent_referenced_key

    def create(self, commit: bool = True) -> bool:
        """
        Creates the merge view on the specified service
        Returns True in case of success

        Parameters
        ----------
        commit : bool
            Whether to commit the transaction after executing the SQL statements.
        """
        success = True
        for sql in [
            self.__view(),
            self.__insert_trigger(),
            self.__update_trigger(),
            self.__delete_trigger(),
            self.__extras(),
        ]:
            try:
                if sql:
                    cursor = self.conn.cursor()
                    cursor.execute(sql)
            except psycopg.Error as e:
                success = False
                print(f"*** Failing:\n{sql}\n***")
                raise e
        if commit:
            self.conn.commit()
        return success

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS SELECT
  {child_cols},
  {parent_cols}
  FROM {cs}.{ct}
  LEFT JOIN {ps}.{pt} ON {pt}.{prk} = {ct}.{rpk};
""".format(
            vs=self.view_schema,
            vn=self.view_name,
            parent_cols=select_columns(
                connection=self.conn,
                table_schema=self.parent_schema,
                table_name=self.parent_table,
                table_alias=self.parent_table,
                remove_pkey=True,
            ),
            child_cols=select_columns(
                connection=self.conn,
                table_schema=self.child_schema,
                table_name=self.child_table,
                table_alias=self.child_table,
            ),
            cs=self.child_schema,
            ct=self.child_table,
            ps=self.parent_schema,
            pt=self.parent_table,
            rpk=self.ref_parent_key,
            prk=self.parent_pkey,
        )

        return sql

    def __insert_trigger(self) -> str:
        """

        :return:
        """
        sql = """
-- INSERT TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_insert() RETURNS trigger AS
$BODY$
BEGIN
{insert_parent}

{insert_child}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_insert ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_insert
  INSTEAD OF INSERT ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_insert();
""".format(
            vs=self.view_schema,
            vn=self.view_name,
            insert_parent=insert_command(
                connection=self.conn,
                table_schema=self.parent_schema,
                table_name=self.parent_table,
                remove_pkey=False,
                coalesce_pkey_default=True,
                coalesce_pkey_default_value=default_value(
                    self.conn, self.child_schema, self.child_table, self.child_pkey
                ),
                remap_columns={self.parent_pkey: self.ref_parent_key},
                inner_defaults=self.inner_defaults,
                returning="{ppk} INTO NEW.{prk}".format(
                    ppk=self.parent_pkey, prk=self.ref_parent_key
                ),
            ),
            insert_child=insert_command(
                connection=self.conn,
                table_schema=self.child_schema,
                table_name=self.child_table,
                remove_pkey=False,
                pkey=self.child_pkey,
            ),
        )
        return sql

    def __update_trigger(self):
        sql = """
-- UPDATE TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_update() RETURNS trigger AS
$BODY$
BEGIN
{update_master}

{update_child}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_update ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_update
  INSTEAD OF UPDATE ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_update();
""".format(
            vs=self.view_schema,
            vn=self.view_name,
            update_master=update_command(
                connection=self.conn,
                table_schema=self.parent_schema,
                table_name=self.parent_table,
                remap_columns={self.parent_pkey: self.ref_parent_key},
            ),
            update_child=update_command(
                connection=self.conn,
                table_schema=self.child_schema,
                table_name=self.child_table,
                pkey=self.child_pkey,
                remove_pkey=False,
            ),
        )
        return sql

    def __delete_trigger(self):
        sql = """
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_delete() RETURNS trigger AS
$BODY$
BEGIN
  DELETE FROM {cs}.{ct} WHERE {rpk} = OLD.{rpk};
  DELETE FROM {ps}.{pt} WHERE {ppk} = OLD.{rpk};
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_delete ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_delete
  INSTEAD OF DELETE ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_delete();
""".format(
            vs=self.view_schema,
            vn=self.view_name,
            cs=self.child_schema,
            ct=self.child_table,
            rpk=self.ref_parent_key,
            ppk=self.parent_pkey,
            ps=self.parent_schema,
            pt=self.parent_table,
        )
        return sql

    def __extras(self):
        sql = ""
        if self.pkey_default_value:
            sql += "ALTER VIEW {vs}.{vn} ALTER {rpk} SET DEFAULT {dv};".format(
                vs=self.view_schema,
                vn=self.view_name,
                rpk=self.child_pkey,
                dv=default_value(self.conn, self.child_schema, self.child_table, self.child_pkey),
            )
        return sql
