import os
import valkey

class ConfigurationError(ValueError):
    """Custom exception for configuration-related errors, typically from environment variables."""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ConnectionError(Exception):
    """Custom exception for Valkey connection-related errors."""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class ValkeyClient:
    """A client for interacting with a Valkey (or Redis) server."""
    def __init__(
        self,
        host: str,
        port: int,
        db: int = 0,
        password: str | None = None,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5,
    ) -> None:
        """Initializes the Valkey client and connects to the specified instance.

        Attempts to ping the server upon connection to verify reachability.

        Args:
            host: The hostname or IP address of the Valkey server.
            port: The port number of the Valkey server.
            db: The database number to connect to (default is 0).
            password: The password for authentication (optional).
            socket_timeout: Timeout in seconds for socket operations (default 5).
            socket_connect_timeout: Timeout in seconds for establishing connection (default 5).

        Raises:
            ConnectionError: If the client fails to connect to or ping the Valkey instance.
        """
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
        """Creates a new ValkeyClient instance using settings from environment variables.

        Reads 'VALKEY_HOST', 'VALKEY_PORT', 'VALKEY_DB', and 'VALKEY_PW' environment
        variables, providing defaults for host ('localhost'), port (6379), and db (0).

        Returns:
            A configured and connected ValkeyClient instance.

        Raises:
            ConfigurationError: If environment variables contain invalid values (e.g., non-integer port)
                               or if the connection fails using these settings (rethrown from __init__).
        """
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
