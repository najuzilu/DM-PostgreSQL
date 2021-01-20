import logging
import psycopg2
from typing import Optional, Tuple
import psycopg2.extensions as psycopg2Ext
from sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger(__name__)


def create_database() -> Optional[Tuple[psycopg2Ext.cursor, psycopg2Ext.connection]]:
    """
    Description: This method will create and connect to the sparkifydb.
    If the connections is successful, it will return the cursor and
    connection to sparkifydb.

    Returns:
        Optional[tuple, None]: Tuple with cursor and connection to sparkifydb.
            If method terminates prior to full execution, return `None`.
    """
    # connect to default database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=studentdb user=student password=student"
        )
    except psycopg2.Error as e:
        msg = "ERROR: Could not make connection to studentdb database."
        logger.warning(msg, e)
        return

    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        msg = "ERROR: Could not get cursor to studentdb database."
        logger.warning(msg, e)
        return

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        msg = "ERROR: There was an issue while dropping sparkifydb."
        logger.warning(msg, e)
        return

    try:
        cur.execute(
            "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0"
        )
    except psycopg2.Error as e:
        msg = "ERROR: Could not create sparkifydb."
        logger.warning(msg, e)
        return

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student"
        )
    except psycopg2.Error as e:
        msg = "ERROR: Could not make connection to sparkify database."
        logger.warning(msg, e)
        return

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        msg = "ERROR: Could not get cursor to sparkify database."
        logger.warning(msg, e)
        return

    return cur, conn


def drop_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: This method drops each table using the queries in
        `drop_table_queries` list.

    Arguments:
        cur (psycopg2Ext.cursor): the cursor object.
        conn (psycopg2Ext.connection): the connection object.

    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Could not drop table with query: {query}"
            logger.warning(msg, e)
            continue
        conn.commit()


def create_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: This method creates each table using the queries in
        `create_table_queries` list.

    Arguments:
        cur (psycopg2Ext.cursor): the cursor object.
        conn (psycopg2Ext.connection): the connection object.

    Returns:
        None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Could not create table with query: {query}"
            logger.warning(msg, e)
            continue
        conn.commit()


def main() -> None:
    """
    Description: Thie method drops (if exists) and creates the sparkify database.
        It then established connection with the sparkify database and gets cursor
        to it. It will drop all the tables, create all the neeed tables and
        finally close the connection.

    Returns:
        None
    """

    db = create_database()

    # If no error has occurred...
    if db:
        cur, conn = db[0], db[1]
        drop_tables(cur, conn)
        create_tables(cur, conn)
        conn.close()


if __name__ == "__main__":
    main()
