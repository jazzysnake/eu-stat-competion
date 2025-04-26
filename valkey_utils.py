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
                decode_responses=True
            )
        except Exception as e:
            raise ConnectionError('Failed to connect to valkey instance') from e

    @staticmethod
    def new() -> 'ValkeyClient':
        try:
            host = os.environ.get('VALKEY_HOST', 'localhost')
            port = int(os.environ.get('VALKEY_PORT', 6379))
            db = int(os.environ.get('VALKEY_DB', 0))
            pw = os.environ.get('VALKEY_PW')
            return ValkeyClient(host=host, port=port, db=db, password=pw)
        except ConnectionError as ce:
            raise ConfigurationError('Failed to connect to valkey with specified settings') from ce
        except ValueError as ve:
            raise ConfigurationError('Invalid value provided in environment variable') from ve
