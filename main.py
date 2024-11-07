import time

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
import os
import logging
import tempfile
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from uploader import BlobUploader
import traceback

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI(timeout=3000)  # 50minutes

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
blob_uploader = BlobUploader(connection_string, container_name)


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"An error occurred: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "An internal server error occurred."}
    )


@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        start_time=time.time()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name

            chunk_size = 1024 * 1024
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                temp_file.write(chunk)

        try:

            blob_uploader.upload_stream(temp_file_path, file.filename)

        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail="Failed to upload file.")

        #os.remove(temp_file_path)
        end_time=time.time()
        duration=end_time-start_time
        logger.info(f"Process completed in {duration} seconds ++++ {float(duration /60)}")
        return {"message": "Upload completed successfully."}

    except Exception as e:
        logger.error(f"Failed to upload file due to: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to upload file.")


@app.get("/upload/progress")
async def get_upload_progress():
    return {"progress": f"{blob_uploader.get_progress():.2f}%"}
