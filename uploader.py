import os
import logging
import traceback
from azure.storage.blob import BlobServiceClient, BlobBlock
import uuid
import time
import json

logger = logging.getLogger(__name__)


class BlobUploader:
    def __init__(self, connection_string: str, container_name: str):
        if not connection_string or not container_name:
            logger.error("Azure storage connection string or container name is missing.")
            raise ValueError("Missing Azure connection string or container name")

        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        self.progress = 0
        self.upload_state_file = "upload_state.json"

    #def upload_stream(self, file_path: str, blob_name: str, chunk_size: int = 4 * 1024 * 1024, max_retries: int = 3):
    #made the chunk sizr to 100 mb ..time saved wrt 4mb chunks was more than 120 seconds
    def upload_stream(self, file_path: str, blob_name: str, chunk_size: int = 100 * 1024 * 1024,
                          max_retries: int = 3):

        MAX_FILE_SIZE = 100 * 1024 * 1024 * 1024  #100 gb
        try:
            logger.info(f"Starting upload of {file_path} ({os.path.getsize(file_path)} bytes) as {blob_name}")

            file_size = os.path.getsize(file_path)
            if file_size > MAX_FILE_SIZE:
                raise ValueError(f"File size exceeds the maximum limit of {MAX_FILE_SIZE} bytes")

            blob_client = self.container_client.get_blob_client(blob_name)
            uploaded_size = 0
            block_ids = []

            if os.path.exists(self.upload_state_file):
                with open(self.upload_state_file, "r") as f:
                    upload_state = json.load(f)

                if upload_state["blob_name"] == blob_name:
                    uploaded_size = upload_state["uploaded_size"]
                    block_ids = upload_state["block_ids"]

                    self.progress = (uploaded_size / file_size) * 100
                    logger.info(f"Resuming upload from {uploaded_size} bytes of {file_size} ({self.progress:.2f}%)")

            with open(file_path, "rb") as file:

                file.seek(uploaded_size)

                retries = 0
                while True:
                    try:
                        chunk = file.read(chunk_size)
                        if not chunk:
                            break

                        block_id = str(uuid.uuid4())
                        block_ids.append(block_id)

                        blob_client.stage_block(block_id=block_id, data=chunk)
                        uploaded_size += len(chunk)

                        self.progress = (uploaded_size / file_size) * 100
                        logger.info(
                            f"Uploaded {uploaded_size} bytes of {file_size / (1024 * 1024)} ({self.progress:.2f}%) - Printed {len(chunk)} bytes")
                            # f"Uploaded {uploaded_size} bytes of {file_size} ({self.progress:.2f}%) - Printed {len(chunk)} bytes")
                        upload_state = {
                            "blob_name": blob_name,
                            "uploaded_size": uploaded_size,
                            "block_ids": block_ids
                        }
                        with open(self.upload_state_file, "w") as f:
                            json.dump(upload_state, f)

                        retries = 0
                        logger.info(f"PROGRESS = {self.progress} %")
                        print(f"--------------")

                    except Exception as e:
                        retries += 1
                        if retries > max_retries:
                            logger.error(f"Maximum number of retries exceeded. Error during file upload: {str(e)}")
                            logger.error(traceback.format_exc())
                            raise

                        logger.warning(
                            f"Transient error occurred during file upload. Retrying ({retries}/{max_retries})...")
                        time.sleep(2 ** retries)

            blob_client.commit_block_list(block_ids)
            logger.info(f"Upload of {blob_name} completed successfully")


            os.remove(file_path)

            if os.path.exists(self.upload_state_file):
                os.remove(self.upload_state_file)

        except Exception as e:
            logger.error(f"Error during file upload: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def get_progress(self):
        return self.progress





