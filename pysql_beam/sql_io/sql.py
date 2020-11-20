"""
This module implements the bounded source for Postgres read


TODO://
1. Add transaction level for select, read committed etc

"""
import decimal
import json
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms.display import DisplayDataItem
from apache_beam import coders
import os
import requests
import logging

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass

import apache_beam as beam
from apache_beam.options import value_provider
from apache_beam.transforms import Reshuffle

from .wrapper import BaseWrapper, MySQLWrapper, MSSQLWrapper, PostgresWrapper, SQLDisposition, TableSchema, TableFieldSchema, SQLWriteDoFn
from .exceptions import ExceptionInvalidWrapper

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
MAX_RETRIES = 5
TABLE_TRUNCATE_SLEEP = 150
DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
CONNECTION_INIT_TIMEOUT = 60
READ_COMMITTED = True
AUTO_COMMIT = False
READ_BATCH = 1000
WRITE_BATCH = 1000
MAX_QUERY_SPLIT = 50000

def default_encoder(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(
        "Object of type '%s' is not JSON serializable" % type(obj).__name__)


class RowAsDictJsonCoder(coders.Coder):
    """A coder for a table row (represented as a dict) to/from a JSON string.

    This is the default coder for sources and sinks if the coder argument is not
    specified.
    """

    def encode(self, table_row):
        # The normal error when dumping NAN/INF values is:
        # ValueError: Out of range float values are not JSON compliant
        # This code will catch this error to emit an error that explains
        # to the programmer that they have used NAN/INF values.
        try:
            return json.dumps(
                table_row, allow_nan=False, default=default_encoder).encode('utf-8')
        except ValueError as e:
            raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

    def decode(self, encoded_table_row):
        return json.loads(encoded_table_row.decode('utf-8'))


class SQLReader(dataflow_io.NativeSourceReader):
    """A reader for a mysql Database source."""

    def __init__(self, source, use_legacy_sql=True, flatten_results=True, kms_key=None):
        self.source = source
        self.connection = None

        self.row_as_dict = isinstance(self.source.coder, RowAsDictJsonCoder)
        # Schema for the rows being read by the reader. It is initialized the
        # first time something gets read from the table. It is not required
        # for reading the field values in each row but could be useful for
        # getting additional details.
        self.schema = None
        self.use_legacy_sql = use_legacy_sql
        self.flatten_results = flatten_results
        self.kms_key = kms_key

        if self.source.table is not None:
            self.query = """SELECT * FROM {};""".format(self.source.table)
        elif self.source.query is not None:
            self.query = self.source.query
        else:
            raise ValueError("mysqlSource must have either a table or query")

    def _build_connection_mysql(self):
        """
        Create connection object with mysql, mssql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        # if self.connection is not None and hasattr(self.connection, 'close'):
        #     return self.connection

        if self.source.wrapper == MySQLWrapper:
            import pymysql
            connection = pymysql.connect(host=self.source.host, port=int(self.source.port),
                                         user=self.source.username, password=self.source.password,
                                         database=self.source.database,
                                         connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.source.wrapper == PostgresWrapper:
            import psycopg2
            connection = psycopg2.connect(host=self.source.host, port=int(self.source.port),
                                          user=self.source.username, password=self.source.password,
                                          database=self.source.database)
            connection.autocommit = self.source.autocommit or AUTO_COMMIT
        elif self.source.wrapper == MSSQLWrapper:
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

    def __enter__(self):
        self._build_connection_mysql()
        self.client = self.source.wrapper(connection=self.connection)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """
        Perform clean up tasks like delete tmp file/bucket/table/database
        :param exception_type:
        :param exception_value:
        :param traceback:
        :return:
        """
        if hasattr(self.connection, 'close'):
            self.connection.close()

    def __iter__(self):
        for records, schema in self.client.read(self.query, batch=self.source.batch):
            if self.schema is None:
                self.schema = schema
            for row in records:
                yield self.client.row_as_dict(row, schema)


class SQLSouceInput(object):
    def __init__(self, host=None, port=None, username=None, password=None,
                 database=None, table=None, query=None, sql_url=None, sql_url_auth_header=None,
                 validate=False, coder=None, batch=READ_BATCH, autocommit=AUTO_COMMIT, wrapper=MySQLWrapper, schema_only=False, *args, **kwargs):
        """

        :param host: db host ip address or domain
        :param port: db port 
        :param username: db username
        :param password: db password
        :param database: db connecting database
        :param table: table to fetch data, all data will be fetched in cursor
        :param query: query sting to fetch. either query or table can be passed
        :param sql_url: url of sql file to download and use the sql url as query
        :param sql_url_auth_header: auth header to download from sql_url, should be json string, which will be decoded at calling time, default to no header
        :param validate: validation ? not used as of now
        :param coder: default coder to use
        :param batch: size of match to read the records default to READ_BATCH, not used
        :param autocommit: connection autocommit
        :param wrapper: which wrapper to use, mysql or postgres
        :param schema_only: return schema or data
        """

        self.database = database
        self.validate = validate
        self.coder = coder or RowAsDictJsonCoder()

        # connection
        self.table = table
        self.query = query
        self.sql_url = sql_url
        self.sql_url_auth_header = sql_url_auth_header
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.batch = batch
        self.autocommit = autocommit
        if wrapper in [BaseWrapper, MySQLWrapper, MSSQLWrapper, PostgresWrapper]:
            self.wrapper = wrapper
        else:
            raise ExceptionInvalidWrapper("Wrapper can be [BaseWrapper, MySQLWrapper, MSSQLWrapper,  PostgresWrapper]")

        self._connection = None
        self._client = None
        self.schema = None
        self.schema_only = schema_only

        self.runtime_params = ['host', 'port', 'username', 'password', 'database',
                               'table', 'query', 'sql_url', 'sql_url_auth_header',
                               'batch', 'schema_only']

    @staticmethod
    def _build_value(source, keys):
        for key in keys:
            setattr(source, key, SQLSource.get_value(getattr(source, key, None)))


class ReadFromSQL(beam.PTransform):
    def __init__(self, *args, **kwargs):
        self.source = SQLSouceInput(*args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (pcoll.pipeline
                | 'UserQuery' >> beam.Create([1])
                | 'SplitQuery' >> beam.ParDo(PaginateQueryDoFn(*self.args, **self.kwargs))
                | "reshuffle" >> Reshuffle()
                | 'Read' >> beam.ParDo(SQLSourceDoFn(*self.args, **self.kwargs))
                )


class RowJsonDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        for row in element[0]:
            cols = [{'name': col.name, 'type_code': col.type_code} for col in element[1]]
            yield BaseWrapper.convert_row_to_dict(row, cols)


class PaginateQueryDoFn(beam.DoFn):
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def process(self, query, *args):
            source = SQLSource(*self.args, **self.kwargs)
            SQLSouceInput._build_value(source, source.runtime_params)

            if query != 1:
                source.query = query
            else:
                source.query = source.query.strip(";")
            query = source.query
            row_count_query = "select count(1) as row_count from ({}) as subq".format(query)
            row_count = 0
            queries = []
            try:
                batch = source.batch
                for records, schema in source.client.read(row_count_query, batch=batch):
                    row_count = records[0][0]
                offsets = list(range(0, row_count, batch))
                for offset in offsets:
                    paginated_query, status = self.paginated_query(query, batch, offset)
                    queries.append(paginated_query)
                    logging.debug(("paginated query", paginated_query))
                    if not status:
                        break
            except Exception as ex:
                logging.error(ex)
                queries.append(query)

            return list(set(queries))

        @staticmethod
        def paginated_query(query, limit, offset=0):
            query = query.strip(";")
            if " limit " in query.lower():
                query = "SELECT * from ({query}) as sbq LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset)
                return query, True
                # return query, False
            else:
                query = query.strip(";")
                return "{query} LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset), True


class SQLSourceDoFn(beam.DoFn):
    """
    This needs to be called as beam.io.Read(SQLSource(*arguments))

    TODO://
    1. To accept dsn or connection string

    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @property
    def client(self):
        """
        Create connection object with mysql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """

        if self.source._connection is not None and hasattr(self._connection, 'close'):
            self._client = self.source.wrapper(connection=self._connection)
            return self._client

        self._connection = self._create_connection()
        self._client = self.source.wrapper(connection=self._connection)
        return self._client

    def _create_connection(self):
        if self.source.wrapper == MySQLWrapper:
            import pymysql
            _connection = pymysql.connect(host=self.source.host, port=int(self.source.port),
                                          user=self.source.username, password=self.source.password,
                                          database=self.source.database,
                                          connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.source.wrapper == PostgresWrapper:
            import psycopg2
            _connection = psycopg2.connect(host=self.source.host, port=int(self.source.port),
                                           user=self.source.username, password=self.source.password,
                                           database=self.source.database)
            _connection.autocommit = self.source.autocommit or AUTO_COMMIT
        elif self.source.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            _connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")

        return _connection

    def process(self, query, *args, **kwargs):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        print(query)
        source = SQLSource(*self.args, **self.kwargs)
        SQLSouceInput._build_value(source, source.runtime_params)
        self.source = source
        #logging.debug("Processing - {}".format(query))
        print(source.client, query)
        # records_schema = source.client.read(query)
        for records, schema in source.client.read(query):
            for row in records:
                yield source.client.row_as_dict(row, schema)

        # return records_schema


class SQLSource(SQLSouceInput, beam.io.iobase.BoundedSource):
    """
    This needs to be called as beam.io.Read(SQLSource(*arguments))

    TODO://
    1. To accept dsn or connection string

    """

    @property
    def client(self):
        """
        Create connection object with mysql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        self._build_value(self.runtime_params)
        if self._connection is not None and hasattr(self._connection, 'close'):
            self._client = self.wrapper(connection=self._connection)
            return self._client

        self._connection = self._create_connection()
        self._client = self.wrapper(connection=self._connection)
        return self._client

    def _create_connection(self):
        if self.wrapper == MySQLWrapper:
            import pymysql
            _connection = pymysql.connect(host=self.host, port=int(self.port),
                                          user=self.username, password=self.password,
                                          database=self.database,
                                          connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.wrapper == PostgresWrapper:
            import psycopg2
            _connection = psycopg2.connect(host=self.host, port=int(self.port),
                                           user=self.username, password=self.password,
                                           database=self.database)
            _connection.autocommit = self.autocommit or AUTO_COMMIT
        elif self.wrapper == MSSQLWrapper:
            print('host', self.host)
            print('port', self.port)
            print('database', self.database)
            print('username', self.username)
            print('password', self.password)
            print('query', self.query)
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            _connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.host+';PORT='+ str(int(self.port)) +
                                        ';DATABASE='+self.database+';UID='+
                                        self.username+';PWD='+self.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")

        return _connection

    def estimate_size(self):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self.client.count(self.query)

    def get_range_tracker(self, start_position, stop_position):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = beam.io.range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = beam.io.range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        self._build_value(self.runtime_params)

        if self.table is not None:
            self.query = """SELECT * FROM {}""".format(self.table)
        elif self.query is not None:
            self.query = self.query
        elif self.sql_url is not None:
            self.query = self.download_sql(self.sql_url, self.sql_url_auth_header)
        else:
            raise ValueError("Source must have either a table or query or sql_url")

        if self.schema_only:
            if not self.table:
                raise Exception("table argument is required for schema")
            self.query = "DESCRIBE {}".format(self.table)

        for records, schema in self.client.read(self.query, batch=self.batch):
            if self.schema is None:
                self.schema = schema

            if self.schema_only:
                yield records
                break

            for row in records:
                yield self.client.row_as_dict(row, schema)

    def _build_value(self, keys):
        for key in keys:
            setattr(self, key, self.get_value(getattr(self, key, None)))

    def _validate(self):
        if self.table is not None and self.query is not None:
            raise ValueError('Both a table and a query were specified.'
                             ' Please specify only one of these.')
        elif self.table is None and self.query is None:
            raise ValueError('A table or a query must be specified')
        elif self.table is not None:
            self.table = self.table
            self.query = None
        else:
            self.query = self.query
            self.table = None

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield beam.io.iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

    @staticmethod
    def get_value(value_obj):
        """
        Extract the value from value_obj, if value object is a runtime value provider
        :param value_obj:
        :return:
        """
        if callable(value_obj):
            return value_obj()
        if hasattr(value_obj, 'get'):
            return value_obj.get()
        elif hasattr(value_obj, 'value'):
            return value_obj.value
        else:
            return value_obj

    @staticmethod
    def download_sql(url, auth_headers):
        try:
            headers = json.loads(auth_headers)
        except Exception as ex:
            logging.debug("Could not json.loads the auth headers, {}, exception {}".format(auth_headers, ex))
            headers = None
        if os.path.exists(url) and os.path.isfile(url):
            with open(url, 'r') as fobj:
                return fobj.read()
        else:
            logging.info("Downloading form {}".format(url))
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                query = res.text
                logging.debug(("Downloaded query ", query))
                return query
            else:
                raise Exception("Could not successfully download data from {}, text, {}, status: {}".format(url, res.text, res.status_code))


class SQLWriter(beam.PTransform):

    def __init__(self,
                 host,
                 port,
                 username,
                 password,
                 database,
                 table,
                 schema=None,
                 create_disposition=SQLDisposition.CREATE_NEVER,
                 write_disposition=SQLDisposition.WRITE_APPEND,
                 batch_size=WRITE_BATCH,
                 test_client=None,
                 insert_retry_strategy=None,
                 validate=True,
                 coder=None,
                 autocommit=AUTO_COMMIT,
                 wrapper=MySQLWrapper):

        """
        TODO://
        1. use create disposition and write disposition
        2. user batch_size as input, currently default bundle size is used


        :param host:
        :param port:
        :param username:
        :param password:
        :param database:
        :param table:
        :param schema: not used
        :param create_disposition: not used
        :param write_disposition: not used
        :param batch_size: not used
        :param test_client:
        :param insert_retry_strategy:
        :param validate:
        :param coder:
        :param autocommit:
        :param wrapper:
        """
        self.create_disposition = SQLDisposition.validate_create(
            create_disposition)
        self.write_disposition = SQLDisposition.validate_write(
            write_disposition)
        self.schema = SQLWriter.get_dict_table_schema(schema)
        self.batch_size = batch_size
        self.test_client = test_client

        self.insert_retry_strategy = insert_retry_strategy
        self._validate = validate

        # connection
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if wrapper in [MySQLWrapper, MSSQLWrapper, PostgresWrapper]:
            self.wrapper = wrapper
        else:
            raise ExceptionInvalidWrapper("Wrapper can be in [MySQLWrapper, MSSQLWrapper, PostgresWrapper]")

        self.table = table
        self.database = database
        self.autocommit = autocommit
        self.coder = coder

    @staticmethod
    def get_table_schema_from_string(schema):
        table_schema = TableSchema()
        schema_list = [s.strip() for s in schema.split(',')]
        for field_and_type in schema_list:
            field_name, field_type = field_and_type.split(':')
            field_schema = TableFieldSchema()
            field_schema.name = field_name
            field_schema.type = field_type
            field_schema.mode = 'NULLABLE'
            table_schema.fields.append(field_schema)
        return table_schema

    @staticmethod
    def table_schema_to_dict(table_schema):
        """Create a dictionary representation of table schema for serialization
        """
        def get_table_field(field_spec):
            """Create a dictionary representation of a table field
            """
            result = dict()
            result['name'] = field_spec.name
            result['type'] = field_spec.type
            result['mode'] = getattr(field_spec, 'mode', 'NULLABLE')
            if hasattr(field_spec, 'description') and field_spec.description is not None:
                result['description'] = field_spec.description
            if hasattr(field_spec, 'fields') and field_spec.fields:
                result['fields'] = [get_table_field(f) for f in field_spec.fields]
            return result

        if not isinstance(table_schema, TableSchema):
            raise ValueError("Table schema must be of the type sql.TableSchema")
        schema = {'fields': []}
        for field in table_schema.fields:
            schema['fields'].append(get_table_field(field))
        return schema

    @staticmethod
    def get_dict_table_schema(schema):
        if isinstance(schema, (dict, value_provider.ValueProvider)) or callable(schema) or schema is None:
            return schema
        elif isinstance(schema, (str, )):
            table_schema = SQLWriter.get_table_schema_from_string(schema)
            return SQLWriter.table_schema_to_dict(table_schema)
        elif isinstance(schema, TableSchema):
            return SQLWriter.table_schema_to_dict(schema)
        else:
            raise TypeError('Unexpected schema argument: %s.' % schema)

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
        elif self.wrapper ==MSSQLWrapper:
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

    def expand(self, pcoll):
        p = pcoll.pipeline

        return (pcoll
                | beam.ParDo(SQLWriteDoFn(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    username=self.username,
                    password=self.password,
                    destination=self.table,
                    batch_size=self.batch_size,
                    autocommit=self.autocommit,
                    wrapper=self.wrapper,
                    schema=self.schema,
                    create_disposition=self.create_disposition,
                    write_disposition=self.write_disposition,
                    validate=self._validate))
                )

    def display_data(self):
        res = {}
        if self.table is not None:
            res['table'] = DisplayDataItem(self.table, label='Table')
        return res
