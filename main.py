import asyncio
import gc
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path

import aiofiles
from fastapi import FastAPI, HTTPException, Request, File, UploadFile, Form, Response
from fastapi.responses import FileResponse, JSONResponse

from config import ADMIN_PASSWORD, MAX_FILE_SIZE, STORAGE_CHANNEL
from utils.directoryHandler import getRandomID
from utils.downloader import download_file, get_file_info_from_url
from utils.extra import auto_ping_website, convert_class_to_dict, reset_cache_dir
from utils.logger import Logger
from utils.streamer import media_streamer
from utils.uploader import start_file_uploader

# Import clients only when needed
from utils.clients import initialize_clients

# ====================== Logger First ======================
logger = Logger(__name__)

# Global for lazy clients
_clients = None


async def get_clients():
    global _clients
    if _clients is None:
        try:
            logger.info("Initializing Telegram clients (lazy)...")
            _clients = await initialize_clients()
            logger.info("Clients initialized successfully")
        except Exception as e:
            logger.error(f"Client init failed: {e}", exc_info=True)
            raise
    return _clients


@asynccontextmanager
async def lifespan(app: FastAPI):
    gc.enable()
    gc.collect()

    try:
        reset_cache_dir()
        logger.info("Cache reset")
        asyncio.create_task(auto_ping_website())
        logger.info("Auto-ping started")
    except Exception as e:
        logger.error(f"Lifespan error: {e}", exc_info=True)
        raise

    yield
    logger.info("Shutdown")


app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# ... (rest of your routes - home, static, /file, api endpoints, smartBulkImport, etc.)

# Keep all your API routes exactly as in the last version I gave you
# (checkPassword, createNewFolder, getDirectory, upload, progress routes, rename, trash, delete, move, copy, getFolderTree, URL downloader, smartBulkImport, checkChannelAdmin)

# Example of one smart import route (make sure the others follow the same pattern):
@app.post("/api/smartBulkImport")
async def smart_bulk_import(request: Request):
    from utils.fast_import import SMART_IMPORT_MANAGER
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"smartBulkImport {data}")
    try:
        client = (await get_clients())  # Use lazy clients
        # If you have a get_client() helper, use it instead
        imported_count, total_files, used_fast_import = await SMART_IMPORT_MANAGER.smart_bulk_import(
            client, data["channel"], data["path"], data.get("start_msg_id"), data.get("end_msg_id"), data.get("import_mode", "auto")
        )
        return JSONResponse({
            "status": "ok",
            "imported": imported_count,
            "total": total_files,
            "method": "fast_import" if used_fast_import else "regular_import"
        })
    except Exception as e:
        logger.error(f"Smart bulk import error: {e}", exc_info=True)
        return JSONResponse({"status": str(e)})
