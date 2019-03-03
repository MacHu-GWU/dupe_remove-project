# -*- coding: utf-8 -*-

"""
Implement remove duplicate data worker.
"""

from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import select, text, func


class Worker(object):
    def __init__(self,
                 conn_str=None,
                 engine=None,
                 table_name=None,
                 id_col_name=None,
                 sort_col_name=None):
        self.conn_str = conn_str
        if engine is None:
            self.engine = create_engine(conn_str)
        else:
            self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.table = self.metadata.tables[table_name]
        self.table_name = table_name
        self.id_col = self.table.columns[id_col_name]
        self.id_col_name = id_col_name
        self.sort_col = self.table.columns[sort_col_name]
        self.sort_col_name = sort_col_name

        self.table_name_dupe_ids = "temp_{}_dupe_ids".format(table_name)
        self.table_name_distinct = "temp_{}_distinct".format(table_name)
        try:
            self.table_dupe_ids = Table(
                self.table_name_dupe_ids, self.metadata,
                Column(self.id_col.name, self.id_col.type),
            )
            self.table_dupe_ids.create(self.engine)
        except:  # pragma: no cover
            self.table_dupe_ids = self.metadata.tables[self.table_name_dupe_ids]

        try:
            self.table_distinct = Table(
                self.table_name_distinct, self.metadata,
                *[Column(c.name, c.type) for c in self.table.columns]
            )
            self.table_distinct.create(self.engine)
        except:  # pragma: no cover
            self.table_distinct = self.metadata.tables[self.table_name_distinct]

    def sql_insert_dupe_ids(self, lower, upper):
        """
        Alternative SQL statement::

            sql = self.table_dupe_ids.insert() \
                .from_select(
                [
                    self.table_dupe_ids.columns[self.id_col_name],
                ],
                select([self.id_col, ]) \
                    .where(and_(self.sort_col >= lower, self.sort_col <= upper)
                           ).group_by(self.id_col_name).having(func.count(self.id_col) > 1)
            )

        :param lower: optional, lower bound value for sort key column
        :param upper: optional, upper bound value for sort key column

        :return: (n_total, n_distinct, n_dupes)
        """
        sql = """
            INSERT INTO {table_name_dupe_ids} ({id_col_name})
                (
                    SELECT
                        {table_name}.{id_col_name} AS {id_col_name}
                    FROM {table_name}
                    WHERE 
                        {table_name}.{sort_col_name} BETWEEN :lower AND :upper
                    GROUP BY {id_col_name}
                    HAVING COUNT(*) > 1
                );
            """.format(
            table_name=self.table_name,
            id_col_name=self.id_col_name,
            sort_col_name=self.sort_col_name,
            table_name_dupe_ids=self.table_name_dupe_ids,
        )
        stmt = text(sql)
        stmt = stmt.bindparams(lower=lower, upper=upper)
        return stmt

    def sql_insert_distinct_copy(self, lower, upper):
        """
        Alternative SQL statement::

            sql = self.table_distinct.insert() \
                .from_select(
                [self.table_distinct],
                select([self.table]).distinct() \
                    .where(
                        and_(
                            self.sort_col >= lower,
                            self.sort_col <= upper,
                            self.id_col.in_(
                                select([self.table_dupe_ids.columns[self.id_col_name]])
                            )
                        )
                )
            )

        :param lower: optional, lower bound value for sort key column
        :param upper: optional, upper bound value for sort key column

        :return: (n_total, n_distinct, n_dupes)
        """
        sql = """
            INSERT INTO {table_name_distinct}
            (
                SELECT 
                    DISTINCT *
                FROM {table_name}
                WHERE
                    {table_name}.{sort_col_name} BETWEEN :lower AND :upper
                    AND {table_name}.{id_col_name} IN(
                        SELECT {table_name_dupe_ids}.{id_col_name} AS {id_col_name}
                        FROM {table_name_dupe_ids}
                    )
            );
            """.format(
            table_name=self.table_name,
            id_col_name=self.id_col_name,
            sort_col_name=self.sort_col_name,
            table_name_dupe_ids=self.table_name_dupe_ids,
            table_name_distinct=self.table_name_distinct,
        )
        stmt = text(sql)
        stmt = stmt.bindparams(lower=lower, upper=upper)
        return stmt

    def sql_remove_dupe_rows(self, lower, upper):
        """
        :param lower: optional, lower bound value for sort key column
        :param upper: optional, upper bound value for sort key column

        :return: (n_total, n_distinct, n_dupes)
        """
        sql = """
            DELETE FROM {table_name}
            WHERE
                {table_name}.{sort_col_name} BETWEEN :lower AND :upper
                AND {table_name}.{id_col_name} IN(
                    SELECT {table_name_dupe_ids}.{id_col_name} AS {id_col_name}
                    FROM {table_name_dupe_ids}
                );
            """.format(
            table_name=self.table_name,
            id_col_name=self.id_col_name,
            sort_col_name=self.sort_col_name,
            table_name_dupe_ids=self.table_name_dupe_ids,
            table_name_distinct=self.table_name_distinct,
        )
        stmt = text(sql)
        stmt = stmt.bindparams(lower=lower, upper=upper)
        return stmt

    def sql_insert_back(self):
        sql = """
            INSERT INTO {table_name}
            SELECT * FROM {table_name_distinct};
            """.format(
            table_name=self.table_name,
            table_name_distinct=self.table_name_distinct,
        )
        return text(sql)

    def delete_temp_table_data(self):  # pragma: no cover
        with self.engine.begin() as connection:
            connection.execute(self.table_dupe_ids.delete())
            connection.execute(self.table_distinct.delete())

    def create_temp_table(self):
        self.table_dupe_ids.create(self.engine, checkfirst=True)
        self.table_distinct.create(self.engine, checkfirst=True)

    def drop_temp_table(self):
        self.table_dupe_ids.drop(self.engine, checkfirst=True)
        self.table_distinct.drop(self.engine, checkfirst=True)

    def remove_duplicate(self, lower, upper, _raise_error=False):
        """
        Remove duplicate data that ``sort_col BETWEEN lower AND upper``.

        :param _raise_error: bool, if True, will raise an error right after
            the dupes data are removed from main table and not insert back.
            This flag is for testing the transaction only. The entire
            ``remove_duplicate()`` should be atomic.
        """
        self.drop_temp_table()
        self.create_temp_table()
        with self.engine.begin() as connection:
            connection.execute(self.sql_insert_dupe_ids(lower, upper))
            connection.execute(self.sql_insert_distinct_copy(lower, upper))
            connection.execute(self.sql_remove_dupe_rows(lower, upper))
            if _raise_error:
                raise InterruptedError("Manually raise error to test atomic")
            connection.execute(self.sql_insert_back())
        self.drop_temp_table()

    def count_duplicates(self, lower=None, upper=None):
        """
        return number of total rows, distinct rows, duplicate rows.

        :param lower: optional, lower bound value for sort key column
        :param upper: optional, upper bound value for sort key column

        :return: (n_total, n_distinct, n_dupes)
        """
        where_crts = list()
        stmt_kwargs = dict()
        if lower is not None:
            where_crts.append(
                "{table_name}.{sort_col_name} >= :lower".format(
                    table_name=self.table_name,
                    sort_col_name=self.sort_col_name,
                )
            )
            stmt_kwargs["lower"] = lower

        if upper is not None:
            where_crts.append(
                "{table_name}.{sort_col_name} <= :upper".format(
                    table_name=self.table_name,
                    sort_col_name=self.sort_col_name,
                )
            )
            stmt_kwargs["upper"] = upper

        if len(where_crts):
            where_clause = "WHERE {}".format(" AND ".join(where_crts))
        else:
            where_clause = ""

        sql = """
        SELECT
            COUNT(T.{id_col_name}) AS n_total,
            COUNT(DISTINCT(T.{id_col_name})) AS n_distinct
        FROM (
            SELECT
                {table_name}.{id_col_name} AS {id_col_name}
            FROM {table_name}
            {where_clause}
        ) T
        """.format(
            table_name=self.table_name,
            id_col_name=self.id_col_name,
            where_clause=where_clause,
        )
        stmt = text(sql)
        stmt = stmt.bindparams(**stmt_kwargs)

        n_total, n_distinct = self.engine.execute(stmt).fetchall()[0]
        n_dupes = n_total - n_distinct
        return n_total, n_distinct, n_dupes

    def sort_key_min_max(self):
        """
        Return min / max value in sort key.
        """
        sql = select([func.min(self.sort_col), func.max(self.sort_col)])
        results = self.engine.execute(sql).fetchall()
        if len(results) == 1:
            min_value, max_value = results[0]
            return min_value, max_value
        else:
            raise ValueError
