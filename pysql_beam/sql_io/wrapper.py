"""
This contains code for MySQL, MSSQL and Postgres sql database sql commands wrapeprs

"""
import datetime
import decimal
import uuid
import logging
import six

from apache_beam.utils import retry

from psycopg2.extensions import Column

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass

import apache_beam as beam
from .exceptions import ExceptionNoColumns, ExceptionBadRow, ExceptionInvalidWrapper

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
MAX_RETRIES = 5
TABLE_TRUNCATE_SLEEP = 150
DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
CONNECTION_INIT_TIMEOUT = 60
AUTO_COMMIT = False
READ_BATCH = 5000


class SQLDisposition(object):
    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'
    CREATE_NEVER = 'CREATE_NEVER'
    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'

    @staticmethod
    def validate_create(disposition):
        values = (SQLDisposition.CREATE_NEVER, SQLDisposition.CREATE_IF_NEEDED)
        if disposition not in values:
            raise ValueError('Invalid write disposition {}. Expecting {}'.format(disposition, values))
        return disposition

    @staticmethod
    def validate_write(disposition):
        values = (SQLDisposition.WRITE_TRUNCATE,
                  SQLDisposition.WRITE_APPEND,
                  SQLDisposition.WRITE_EMPTY)
        if disposition not in values:
            raise ValueError('Invalid write disposition {}. Expecting {}'.format(disposition, values))
        return disposition


class TableSchema(object):
    fields = None
    pass


class TableFieldSchema(object):
    pass


class BaseWrapper(object):
    """
    This eventually need to be replaced with integration with SQLAlchemy
    """

    TEMP_TABLE = 'temp_table_'
    TEMP_database = 'temp_database_'

    def __init__(self, connection):
        self.connection = connection
        self._unique_row_id = 0
        # For testing scenarios where we pass in a client we do not want a
        # randomized prefix for row IDs.
        self._row_id_prefix = '' if connection else uuid.uuid4()
        self._temporary_table_suffix = uuid.uuid4().hex

    def escape_name(self, name):
        """Escape name to avoid SQL injection and keyword clashes.
        Doubles embedded backticks, surrounds the whole in backticks.
        Note: not security hardened, caveat emptor.

        """
        return '`{}`'.format(name.replace('`', '``'))

    @property
    def unique_row_id(self):
        """Returns a unique row ID (str) used to avoid multiple insertions.

        If the row ID is provided, we will make a best effort to not insert
        the same row multiple times for fail and retry scenarios in which the insert
        request may be issued several times. This comes into play for sinks executed
        in a local runner.

        Returns:
          a unique row ID string
        """
        self._unique_row_id += 1
        return '%s_%d' % (self._row_id_prefix, self._unique_row_id)

    @staticmethod
    def row_as_dict(row, schema):
        """
        postgres cursor object contains the description in this format
        (Column(name='id', type_code=23), Column(name='name', type_code=25))

        pymysql cursor description has below format
        ((col1, 123,123,1,23), (col2, 23,123,1,23))
        :param row: database row, tuple/list of objects
        :param schema:
        :return:
        """
        row_dict = {}
        for index, column in enumerate(schema):
            if isinstance(column, Column):
                row_dict[column.name] = row[index]
            else:
                row_dict[column[0]] = row[index]
        return row_dict

    def _get_temp_table(self):
        return "{}.{}".format(self.TEMP_database, self.TEMP_TABLE)

    def _create_table(self, database, table, schema):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_or_create_database(self, database):
        """
        :param database: Check if database already exists otherwise create it
        :return:
        """
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _is_table_empty(self, database, table):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _delete_table(self, database, table):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _delete_database(self, project_id, database, delete_contents=True):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _get_query_results(self, project_id, job_id,
                         page_token=None, max_results=10000):
        raise NotImplementedError

    @retry.with_exponential_backoff(num_retries=MAX_RETRIES,
                                    retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_table(self, database, table):
        """Lookup a table's metadata object.

        Args:
          database, table: table lookup parameters

        Returns:
          SQL.Table instance
        Raises:
          HttpError if lookup failed.
        """
        raise NotImplementedError

    def _convert_cell_value_to_dict(self, value, field):
        if field.type == 'STRING':
            # Input: "XYZ" --> Output: "XYZ"
            return value
        elif field.type == 'BOOLEAN':
            # Input: "true" --> Output: True
            return value in ['true', 1, '1', 'True']
        elif field.type == 'INTEGER':
            # Input: "123" --> Output: 123
            return int(value)
        elif field.type == 'FLOAT':
            # Input: "1.23" --> Output: 1.23
            return float(value)
        elif field.type == 'TIMESTAMP':
            # The UTC should come from the timezone library but this is a known
            # issue in python 2.7 so we'll just hardcode it as we're reading using
            # utcfromtimestamp.
            # Input: 1478134176.985864 --> Output: "2016-11-03 00:49:36.985864 UTC"
            dt = datetime.datetime.utcfromtimestamp(float(value))
            return dt.strftime(DATETIME_FMT)
        elif field.type == 'BYTES':
            # Input: "YmJi" --> Output: "YmJi"
            return value
        elif field.type == 'DATE':
            # Input: "2016-11-03" --> Output: "2016-11-03"
            return value
        elif field.type == 'DATETIME':
            # Input: "2016-11-03T00:49:36" --> Output: "2016-11-03T00:49:36"
            return value
        elif field.type == 'TIME':
            # Input: "00:49:36" --> Output: "00:49:36"
            return value
        elif field.type == 'RECORD':
            # Note that a schema field object supports also a RECORD type. However
            # when querying, the repeated and/or record fields are flattened
            # unless we pass the flatten_results flag as False to the source
            return self.convert_row_to_dict(value, field)
        elif field.type == 'NUMERIC':
            return decimal.Decimal(value)
        elif field.type == 'GEOGRAPHY':
            return value
        else:
            raise RuntimeError('Unexpected field type: %s' % field.type)

    @staticmethod
    def convert_row_to_dict(row, schema):
        """Converts a TableRow instance using the schema to a Python dict."""
        result = {}
        for index, col in enumerate(schema):
            if isinstance(col, dict):
                result[col['name']] = row[index]
            else:
                result[col] = row[index]
            # result[field.name] = self._convert_cell_value_to_dict(value, field)
        return result

    def _get_cols(self, row, lst_only=False):
        """
        return a sting of columns
        :param row: can be either dict or schema from cursor.description
        :return: string to be placed in insert command of sql
        """
        names = []
        if isinstance(row, dict):
            names = list(row.keys())
        elif isinstance(row, tuple):
            for column in row:
                if isinstance(column, tuple):
                    names.append(column[0])  # columns name is the first attribute in cursor.description
                else:
                    raise ExceptionNoColumns("Not a valid column object")
        if len(names):
            if lst_only:
                return names
            else:
                cols = ', '.join(map(self.escape_name, names))
            return cols
        else:
            raise ExceptionNoColumns("No columns to make")

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def count(self, query):
        with self.connection.cursor() as cursor:
            logging.info("Estimating size for query")
            logging.info(cursor.mogrify(query))
            cursor.execute(query)
            row_count = cursor.rowcount
            return row_count

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def read(self, query, batch=READ_BATCH):
        """
        Execute the query and return the result in batch

        or read in batch

        # for i in range((size//batch)+1):
        #     records = cursor.fetchmany(size=batch)
        #     yield records, schema
        TODO://
        1. Add batch read

        :param query: query to execute
        :param batch: size of batch to read
        :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.debug("Executing Read query")
            #logging.debug(cursor.mogrify(query))
            paginated_query, status = self.paginated_query(query, limit=batch, offset=0)
            if status:
                size = batch
                offset = 0
                while size >= batch:
                    logging.debug("Paginated query")
                    logging.debug(paginated_query)
                    print(paginated_query)
                    cursor.execute(paginated_query)
                    schema = cursor.description
                    size = cursor.rowcount
                    records = cursor.fetchall()
                    yield records, schema
                    offset = offset + batch
                    print("second pagination?", query)
                    paginated_query, status = self.paginated_query(query, limit=batch, offset=offset)
            else:
                print(paginated_query)
                cursor.execute(paginated_query)
                schema = cursor.description
                size = cursor.rowcount
                if " limit " in paginated_query:
                    records = cursor.fetchall()
                    yield records, schema
                else:
                    for i in range((size//batch)+1):
                        records = cursor.fetchmany(size=batch)
                        yield records, schema
            # records = cursor.fetchall()
            # yield records, schema

    @staticmethod
    def paginated_query(query, limit, offset=0):
        if " limit " in query.lower():
            return query, False
        else:
            query = query.strip(";")
            return "{query} LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset), True

    @staticmethod
    def _convert_to_str(value):
        if isinstance(value, six.string_types):
            return value.replace("'", "''")
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return str(value)
        else:
            return value

    @staticmethod
    def _get_data_row(cols, rows):
        data_rows = []
        row_format = ("'{}',"*(len(cols))).rstrip(',')
        for row in rows:
            data = []
            for col in cols:
                _value = row[col]
                _value = BaseWrapper._convert_to_str(_value)
                data.append(_value)
            data_rows.append(row_format.format(*data))
        return tuple(data_rows)

    @staticmethod
    def format_data_rows_query(data_rows):
        rows_format = ("({}),"*len(data_rows)).rstrip(',')
        formatted_rows = rows_format.format(*data_rows)
        return formatted_rows

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        """Inserts rows into the specified table.

        Args:
          table: The table id. either {database}.{table} format or {table} format
          rows: A list of plain Python dictionaries. Each dictionary is a row and
            each key in it is the name of a field.
          skip_invalid_rows: If there are rows with insertion errors, whether they
            should be skipped, and all others should be inserted successfully.

        """
        if not len(rows):
            return None
        with self.connection.cursor() as cursor:
            cols = self._get_cols(rows[0], lst_only=True)
            data_rows = self._get_data_row(cols, rows)
            cols_str = self._get_cols(rows[0], lst_only=False)
            format_data_rows = self.format_data_rows_query(data_rows)

            insert_query = "INSERT INTO {table} ({cols_str}) VALUES {values}  ;".format(
                table=table,
                cols_str=cols_str,
                values=format_data_rows)

            logging.info("Executing insert query")
            logging.debug(cursor.mogrify(insert_query))
            inserted_row_count = cursor.execute(insert_query)
            if hasattr(cursor, 'rowcount'):
                inserted_row_count = cursor.rowcount
            logging.info("Inserted {}".format(inserted_row_count))

            return inserted_row_count

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        """Gets or creates a table based on create and write dispositions.

        The function mimics the behavior of mysql import jobs when using the
        same create and write dispositions.

        Args:
          database: The database id owning the table.
          table: The table id.
          schema: A TableSchema instance or None.
          create_disposition: CREATE_NEVER or CREATE_IF_NEEDED.
          write_disposition: WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.

        Returns:
          A mysql.Table instance if table was found or created.

        Raises:
          RuntimeError: For various mismatches between the state of the table and
            the create/write dispositions passed in. For example if the table is not
            empty and WRITE_EMPTY was specified then an error will be raised since
            the table was expected to be empty.
        """

        found_table = None
        try:
            found_table = self.get_table(database, table)
        except HttpError as exn:
            if exn.status_code == 404:
                if create_disposition == SQLDisposition.CREATE_NEVER:
                    raise RuntimeError(
                        'Table %s.%s not found but create disposition is CREATE_NEVER.' % (database, table))
            else:
                raise

        # If table exists already then handle the semantics for WRITE_EMPTY and
        # WRITE_TRUNCATE write dispositions.
        if found_table:
            table_empty = self._is_table_empty(database, table)
            if write_disposition == SQLDisposition.WRITE_TRUNCATE:
                self._delete_table(database, table)

        # Create a new table potentially reusing the schema from a previously
        # found table in case the schema was not specified.
        if schema is None and found_table is None:
            raise RuntimeError(
                'Table %s.%s requires a schema. None can be inferred because the '
                'table does not exist.' % (database, table))
        if found_table and write_disposition != SQLDisposition.WRITE_TRUNCATE:
            return found_table
        else:
            created_table = self._create_table(database=database,
                                               table=table,
                                               schema=schema or found_table.schema)
            logging.info('Created table %s.%s with schema %s. Result: %s.', database, table,
                         schema or found_table.schema,
                         created_table)
            # if write_disposition == mysqlDisposition.WRITE_TRUNCATE we delete
            # the table before this point.
            if write_disposition == SQLDisposition.WRITE_TRUNCATE:
                # mysql can route data to the old table for TABLE_TRUNCATE_SLEEP seconds max so wait
                # that much time before creating the table and writing it
                logging.warning('Sleeping for {} seconds before the write as ' +
                                'mysql inserts can be routed to deleted table ' +
                                'for {} seconds after the delete and create.'.format(TABLE_TRUNCATE_SLEEP))
                return created_table
            else:
                return created_table


class MySQLWrapper(BaseWrapper):
    """mysql client wrapper with utilities for querying.

    The wrapper is used to organize all the mysql integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(MySQLWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(MySQLWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)

class MSSQLWrapper(BaseWrapper):
    """mssql client wrapper with utilities for querying.

    The wrapper is used to organize all the mssql integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(MSSQLWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(MSSQLWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)
    
    @staticmethod
    def paginated_query(query, limit, offset=0):
        if " fetch " in query.lower():
            return query, False
        else:
            query = query.strip(";")
            if 'LIMIT' in query:
                indx=query.index('LIMIT')
                query= query[:indx-1]
                #print(indx, "HERE", query[:indx-1])
            return "{query} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;".format(query=query, limit=limit, offset=offset), True


    
class PostgresWrapper(BaseWrapper):
    """postgres client wrapper with utilities for querying.

    The wrapper is used to organize all the postgres integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(PostgresWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(PostgresWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)

    def escape_name(self, name):
        """Escape name to avoid SQL injection and keyword clashes.
        Doubles embedded backticks, surrounds the whole in backticks.
        Note: not security hardened, caveat emptor.

        """
        return self.connection.cursor().mogrify(name.replace('`', '``')).decode('utf-8')


class SQLWriteDoFn(beam.DoFn):
    """Takes in a set of elements, and inserts them to Mysql/Postgres via batch loads.
    """

    def __init__(self,
                 host,
                 port,
                 database,
                 username,
                 password,
                 destination,
                 batch_size,
                 autocommit,
                 wrapper,
                 schema=None,
                 create_disposition=None,
                 write_disposition=None,
                 validate=True):

        super(SQLWriteDoFn, self).__init__()

        self.wrapper = wrapper
        self.destination = destination
        self.schema = schema
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.batch_size = batch_size
        self._validate = validate
        if self._validate:
            self.verify()

        self._elements = None

        self.client = None
        self.connection = None

        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.autocommit = autocommit

    def verify(self):
        pass

    def _build_connection_mysql(self):
        """
        Create connection object with mysql or postgres based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        # if self.connection is not None and hasattr(self.connection, 'close'):
        #     return self.connection

        if self.wrapper == MySQLWrapper:
            import pymysql
            connection = pymysql.connect(host=self.host, port=int(self.port),
                                         user=self.username, password=self.password,
                                         database=self.database,
                                         connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.wrapper == PostgresWrapper:
            import psycopg2
            connection = psycopg2.connect(host=self.host, port=int(self.port),
                                          user=self.username, password=self.password,
                                          database=self.database)
            connection.autocommit = self.autocommit or AUTO_COMMIT
        elif self.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")
        self.connection = connection
        return connection

    def _build_value(self, keys):
        from .sql import SQLSource
        for key in keys:
            setattr(self, key, SQLSource.get_value(getattr(self, key)))

    def start_bundle(self):

        self._build_value(['host', 'port', 'username', 'password', 'database',
                           'destination', 'schema', 'batch_size', 'autocommit'])

        if '.' in self.destination:
            self.destination = self.destination
        else:
            self.destination = "{}.{}".format(self.database, self.destination)

        self.connection = self._build_connection_mysql()
        self.client = self.wrapper(connection=self.connection)
        self._elements = []

    def process(self, element, *args, **kwargs):
        if not isinstance(element, dict):
            raise ExceptionBadRow("{} should be dict type instead of {}".format(element, type(element)))
        if self._elements and (len(self._elements) > self.batch_size):
            self._flush_batch()

        self._elements.append(element)
        if len(self._elements) >= self.batch_size:
            self._flush_batch()

    def finish_bundle(self):
        if len(self._elements):
            self._flush_batch()
        if hasattr(self.connection, 'close'):
            self.connection.close()

    def _flush_batch(self):
        logging.debug("Writing %d records ", len(self._elements))
        if len(self._elements):
            try:
                self.client.insert_rows(self.destination, self._elements)
            except Exception as ex:
                logging.error("Exception when inserting into SQL Database")
                logging.error(ex)
                self.connection.rollback()
                raise ex
            finally:
                self.connection.commit()
        self._elements = []
