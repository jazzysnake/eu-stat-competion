import os
import glob
import queue
import threading
import concurrent.futures as cf
from google.cloud import storage
from google.oauth2 import service_account

from valkey_utils import ConfigurationError

class GCSClient:
    def __init__(
        self,
        project_id: str,
        keyfile_path: str,
        bucket_name: str,
    ) -> None:
        if not os.path.exists(keyfile_path):
            raise ConfigurationError(f'Credentials dont exist at specified path {keyfile_path}')

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.svc_acc = service_account.Credentials.from_service_account_file(keyfile_path)
        self.client = storage.Client(credentials=self.svc_acc, project=project_id)

    @staticmethod
    def new() -> 'GCSClient':
        try:
            project_id = os.environ['GOOGLE_PROJECT_ID']
            keyfile_path = os.environ['GOOGLE_CREDENTIALS_PATH']
            bucket_name = os.environ['GOOGLE_STORAGE_BUCKET_NAME']
            return GCSClient(project_id, keyfile_path, bucket_name)
        except KeyError as e:
            raise ConfigurationError(f'Failed to initialize GCSClient') from e
        except ConfigurationError as e:
            raise e
    
    def upload_blob(self, local_path: str, destination_name: str) -> str:
        if not os.path.exists(local_path):
            raise ValueError(f'Cant locate file at {local_path}')
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(destination_name)
            blob.upload_from_filename(local_path)
            return blob.path
        except FileNotFoundError as e:
            raise Exception(f'Unexpectedly failed to load file from local path {local_path}') from e
        except Exception as e:
            raise Exception(f'Unexpected Error during file upload') from e

class GCSBatchUploader:
    def __init__(
        self,
        project_id: str,
        keyfile_path: str,
        bucket_name: str,
        num_clients: int,
    ) -> None:
        if not os.path.exists(keyfile_path):
            raise ConfigurationError(f'Credentials dont exist at specified path {keyfile_path}')

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.keyfile_path = keyfile_path
        self.num_clients = num_clients
        self.__thread_local_storage = threading.local()
        self.task_queue = queue.Queue()

    @staticmethod
    def new(num_clients:int=4) -> 'GCSBatchUploader':
        try:
            project_id = os.environ['GOOGLE_PROJECT_ID']
            keyfile_path = os.environ['GOOGLE_CREDENTIALS_PATH']
            bucket_name = os.environ['GOOGLE_STORAGE_BUCKET_NAME']
            return GCSBatchUploader(project_id, keyfile_path, bucket_name, num_clients)
        except KeyError as e:
            raise ConfigurationError(f'Failed to initialize GCSClient') from e
        except ConfigurationError as e:
            raise e

    def __upload_worker(self, task: tuple[str,str]) -> str | Exception:
        local_path, destination_name = task
        client: GCSClient | None = getattr(self.__thread_local_storage, 'gcs_client', None)
        if client is None:
            client = GCSClient(
                self.project_id,
                self.keyfile_path,
                self.bucket_name,
            )
            self.__thread_local_storage.gcs_client = client
        try:
            return client.upload_blob(local_path, destination_name)
        except Exception as e:
            return e

    def upload_dir(
        self,
        directory: str,
    ) -> None:
        files = glob.glob(os.path.join(directory, '*'))
        upload_names = [os.path.basename(f) for f in files]
        self.upload_blobs(files, upload_names)
        

    def upload_blobs(
        self,
        local_paths: list[str],
        destination_names: list[str]
    ) -> list[tuple[str, str | Exception]]:
        """batch uploads blobs to gcs, returns (local_paths, gcs_link) for successful uploads."""
        threadpool = cf.ThreadPoolExecutor(max_workers=self.num_clients)
        results = []
        for res in threadpool.map(self.__upload_worker, zip(local_paths, destination_names)):
            results.append(res)
        final = [
            (
                local,
                f"https://storage.cloud.google.com/{self.bucket_name}/{dest}"
                if type(res)==str else res
            )
            for local, dest, res in zip(local_paths, destination_names, results)
        ]
        return final 
            



