import os
import valkey

class ConfigurationError(ValueError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ConnectionError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ValkeyClient:
    def __init__(
        self,
        host: str,
        port: int,
        db: int = 0,
        password: str | None = None,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5,
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        try:
            self.client = valkey.Valkey(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                health_check_interval=30 # Periodically check connection health
            )
            self.client.ping()
        except (valkey.ConnectionError, valkey.TimeoutError, valkey.AuthenticationError) as e:
            raise ConnectionError(f"Failed to connect to or ping Valkey instance at {host}:{port}") from e
        except Exception as e:
             raise ConnectionError(f"An unexpected error occurred during Valkey client initialization for {host}:{port}") from e

    @staticmethod
    def new() -> 'ValkeyClient':
        try:
            host = os.environ.get('VALKEY_HOST', 'localhost')
            port_str = os.environ.get('VALKEY_PORT', '6379')
            db_str = os.environ.get('VALKEY_DB', '0')
            pw = os.environ.get('VALKEY_PW') # Returns None if not set, which is fine

            if not port_str.isdigit():
                raise ConfigurationError(f"Invalid VALKEY_PORT: '{port_str}'. Must be an integer.")
            port = int(port_str)

            if not db_str.isdigit():
                 raise ConfigurationError(f"Invalid VALKEY_DB: '{db_str}'. Must be an integer.")
            db = int(db_str)

            return ValkeyClient(host=host, port=port, db=db, password=pw)
        except ConnectionError as ce:
            raise ConfigurationError(f'Failed to connect to Valkey with derived settings: {ce}') from ce
        except ConfigurationError as conf_err:
             raise conf_err
        except ValueError as ve:
             raise ConfigurationError(f'Invalid numeric value provided in environment variable: {ve}') from ve

    def close(self):
        """Closes the connection to the Valkey server."""
        if self.client:
            self.client.close()
