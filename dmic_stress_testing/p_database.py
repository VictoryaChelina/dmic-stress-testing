from infi.clickhouse_orm import Database, DatabaseException
from requests_toolbelt.adapters.source import SourceAddressAdapter
import requests

class p_db(Database):
    def __init__(
            self,
            db_name, 
            db_url='http://localhost:8123/',
            username=None,
            password=None,
            source_ip=None,
            readonly=False,
            autocreate=True,
            timeout=60,
            verify_ssl_cert=True,
            log_statements=False
            ):
        self.db_name = db_name
        self.db_url = db_url
        self.readonly = False
        self.timeout = timeout

        self.request_session = requests.Session()
        if source_ip is not None:
            source_adapter = SourceAddressAdapter(source_ip)
            self.request_session.mount('http://', source_adapter)

        self.request_session.verify = verify_ssl_cert
        if username:
            self.request_session.auth = (username, password or '')
        self.log_statements = log_statements
        self.settings = {}
        self.db_exists = False # this is required before running _is_existing_database
        self.db_exists = self._is_existing_database()
        if readonly:
            if not self.db_exists:
                raise DatabaseException('Database does not exist, and cannot be created under readonly connection')
            self.connection_readonly = self._is_connection_readonly()
            self.readonly = True
        elif autocreate and not self.db_exists:
            self.create_database()
        self.server_version = self._get_server_version()
        # Versions 1.1.53981 and below don't have timezone function
        self.server_timezone = self._get_server_timezone() if self.server_version > (1, 1, 53981) else pytz.utc
        # Versions 19.1.16 and above support codec compression
        self.has_codec_support = self.server_version >= (19, 1, 16)
        # Version 19.0 and above support LowCardinality
        self.has_low_cardinality_support = self.server_version >= (19, 0)