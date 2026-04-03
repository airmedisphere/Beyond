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

# Lazy client initialization to prevent OOM
from utils.clients import initialize_clients

# ====================== Logger First ======================
logger = Logger(__name__)

# Global clients cache
_clients = None


async def get_clients():
    """Lazy initialization of Telegram clients"""
    global _clients
    if _clients is None:
        try:
            logger.info("Initializing Telegram clients lazily...")
            _clients = await initialize_clients()
            logger.info("Telegram clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}", exc_info=True)
            raise
    return _clients


# ====================== Lifespan ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    gc.enable()
    gc.collect()

    try:
        reset_cache_dir()
        logger.info("Cache directory reset")
        asyncio.create_task(auto_ping_website())
        logger.info("Auto-ping task started")
    except Exception as e:
        logger.error(f"Lifespan startup error: {e}", exc_info=True)
        raise

    yield
    logger.info("Application shutting down...")


# ====================== FastAPI App ======================
app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# ====================== Web Pages ======================
@app.get("/")
async def home_page():
    try:
        return FileResponse("website/home.html")
    except Exception as e:
        logger.error(f"Error serving home page: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.get("/stream")
async def stream_page():
    try:
        return FileResponse("website/VideoPlayer.html")
    except Exception as e:
        logger.error(f"Error serving stream page: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/fast-player")
async def fast_player_page():
    try:
        return FileResponse("website/FastPlayer.html")
    except Exception as e:
        logger.error(f"Error serving fast player: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/pdf-viewer")
async def pdf_viewer_page():
    try:
        return FileResponse("website/PDFViewer.html")
    except Exception as e:
        logger.error(f"Error serving PDF viewer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/static/{file_path:path}")
async def static_files(file_path: str):
    if "apiHandler.js" in file_path:
        try:
            with open(Path("website/static/js/apiHandler.js"), encoding="utf-8") as f:
                content = f.read().replace("MAX_FILE_SIZE__SDGJDG", str(MAX_FILE_SIZE))
            return Response(content=content, media_type="application/javascript")
        except Exception as e:
            logger.error(f"Error serving apiHandler.js: {e}")
            raise HTTPException(status_code=500, detail="Static file error")
    try:
        return FileResponse(f"website/static/{file_path}")
    except Exception:
        raise HTTPException(status_code=404, detail="Static file not found")


@app.get("/file")
async def dl_file(request: Request):
    try:
        from utils.directoryHandler import DRIVE_DATA

        path = request.query_params.get("path")
        if not path:
            raise HTTPException(status_code=400, detail="Path parameter is required")

        file = DRIVE_DATA.get_file(path)
        if not file:
            raise HTTPException(status_code=404, detail="File not found")

        # Support fast import files
        channel = getattr(file, 'source_channel', STORAGE_CHANNEL) if getattr(file, 'is_fast_import', False) else STORAGE_CHANNEL

        return await media_streamer(channel, file.file_id, file.name, request)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error streaming file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ====================== API Routes ======================
@app.post("/api/checkPassword")
async def check_password(request: Request):
    data = await request.json()
    status = "ok" if data.get("pass") == ADMIN_PASSWORD else "Invalid password"
    return JSONResponse({"status": status})


@app.post("/api/createNewFolder")
async def api_new_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"createNewFolder {data}")
    folder = DRIVE_DATA.get_directory(data["path"])
    for f in folder.contents.values():
        if f.type == "folder" and f.name == data["name"]:
            return JSONResponse({"status": "Folder with the name already exist in current directory"})

    DRIVE_DATA.new_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/getDirectory")
async def api_get_directory(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()
    is_admin = data.get("password") == ADMIN_PASSWORD
    auth = data.get("auth")
    sort_by = data.get("sort_by", "date")
    sort_order = data.get("sort_order", "desc")

    logger.info(f"getDirectory {data}")

    if data["path"] == "/trash":
        contents = DRIVE_DATA.get_trashed_files_folders()
        folder_data = convert_class_to_dict({"contents": contents}, isObject=False, showtrash=True,
                                           sort_by=sort_by, sort_order=sort_order)

    elif "/search_" in data["path"]:
        query = urllib.parse.unquote(data["path"].split("_", 1)[1])
        contents = DRIVE_DATA.search_file_folder(query)
        folder_data = convert_class_to_dict({"contents": contents}, isObject=False, showtrash=False,
                                           sort_by=sort_by, sort_order=sort_order)

    elif "/share_" in data["path"]:
        path = data["path"].split("_", 1)[1]
        folder_obj, auth_home_path = DRIVE_DATA.get_directory(path, is_admin, auth)
        auth_home_path = auth_home_path.replace("//", "/") if auth_home_path else None
        folder_data = convert_class_to_dict(folder_obj, isObject=True, showtrash=False,
                                           sort_by=sort_by, sort_order=sort_order)
        return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": auth_home_path})

    else:
        folder_obj = DRIVE_DATA.get_directory(data["path"])
        folder_data = convert_class_to_dict(folder_obj, isObject=True, showtrash=False,
                                           sort_by=sort_by, sort_order=sort_order)

    return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": None})


SAVE_PROGRESS = {}


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    path: str = Form(...),
    password: str = Form(...),
    id: str = Form(...),
    total_size: str = Form(...),
):
    global SAVE_PROGRESS

    if password != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    total_size = int(total_size)
    SAVE_PROGRESS[id] = ("running", 0, total_size)

    ext = file.filename.lower().split(".")[-1]
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_location = cache_dir / f"{id}.{ext}"

    file_size = 0
    try:
        async with aiofiles.open(file_location, "wb") as buffer:
            while chunk := await file.read(1024 * 1024):   # 1MB chunks
                file_size += len(chunk)
                SAVE_PROGRESS[id] = ("running", file_size, total_size)

                if file_size > MAX_FILE_SIZE:
                    file_location.unlink(missing_ok=True)
                    raise HTTPException(status_code=400,
                                      detail=f"File size exceeds {MAX_FILE_SIZE} bytes limit")

                await buffer.write(chunk)

        SAVE_PROGRESS[id] = ("completed", file_size, file_size)
        asyncio.create_task(start_file_uploader(file_location, id, path, file.filename, file_size))

        return JSONResponse({"id": id, "status": "ok"})

    except Exception as e:
        logger.error(f"Upload error for {id}: {e}")
        file_location.unlink(missing_ok=True)
        SAVE_PROGRESS[id] = ("failed", file_size, total_size)
        raise


# ====================== Progress & Control Routes ======================
@app.post("/api/getSaveProgress")
async def get_save_progress(request: Request):
    global SAVE_PROGRESS
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    try:
        return JSONResponse({"status": "ok", "data": SAVE_PROGRESS[data["id"]]})
    except KeyError:
        return JSONResponse({"status": "not found"})


@app.post("/api/getUploadProgress")
async def get_upload_progress(request: Request):
    from utils.uploader import PROGRESS_CACHE
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    try:
        return JSONResponse({"status": "ok", "data": PROGRESS_CACHE[data["id"]]})
    except KeyError:
        return JSONResponse({"status": "not found"})


@app.post("/api/cancelUpload")
async def cancel_upload(request: Request):
    from utils.uploader import STOP_TRANSMISSION
    from utils.downloader import STOP_DOWNLOAD
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"cancelUpload {data}")
    STOP_TRANSMISSION.append(data["id"])
    STOP_DOWNLOAD.append(data["id"])
    return JSONResponse({"status": "ok"})


# ====================== File Management Routes ======================
@app.post("/api/renameFileFolder")
async def rename_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"renameFileFolder {data}")
    DRIVE_DATA.rename_file_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/trashFileFolder")
async def trash_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"trashFileFolder {data}")
    DRIVE_DATA.trash_file_folder(data["path"], data["trash"])
    return JSONResponse({"status": "ok"})


@app.post("/api/deleteFileFolder")
async def delete_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"deleteFileFolder {data}")
    DRIVE_DATA.delete_file_folder(data["path"])
    return JSONResponse({"status": "ok"})


@app.post("/api/moveFileFolder")
async def move_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"moveFileFolder {data}")
    try:
        DRIVE_DATA.move_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/copyFileFolder")
async def copy_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"copyFileFolder {data}")
    try:
        DRIVE_DATA.copy_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFolderTree")
async def get_folder_tree(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"getFolderTree {data}")
    try:
        folder_tree = DRIVE_DATA.get_folder_tree()
        return JSONResponse({"status": "ok", "data": folder_tree})
    except Exception as e:
        return JSONResponse({"status": str(e)})


# ====================== URL Downloader Routes ======================
@app.post("/api/getFileInfoFromUrl")
async def getFileInfoFromUrl(request: Request):
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"getFileInfoFromUrl {data}")
    try:
        file_info = await get_file_info_from_url(data["url"])
        return JSONResponse({"status": "ok", "data": file_info})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/startFileDownloadFromUrl")
async def startFileDownloadFromUrl(request: Request):
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    logger.info(f"startFileDownloadFromUrl {data}")
    try:
        id = getRandomID()
        asyncio.create_task(
            download_file(data["url"], id, data["path"], data["filename"], data.get("singleThreaded", False))
        )
        return JSONResponse({"status": "ok", "id": id})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFileDownloadProgress")
async def getFileDownloadProgress(request: Request):
    from utils.downloader import DOWNLOAD_PROGRESS
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    try:
        return JSONResponse({"status": "ok", "data": DOWNLOAD_PROGRESS[data["id"]]})
    except KeyError:
        return JSONResponse({"status": "not found"})


@app.post("/api/getFolderShareAuth")
async def getFolderShareAuth(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    try:
        auth = DRIVE_DATA.get_folder_auth(data["path"])
        return JSONResponse({"status": "ok", "auth": auth})
    except Exception:
        return JSONResponse({"status": "not found"})


# ====================== Smart Bulk Import Routes ======================
@app.post("/api/smartBulkImport")
async def smart_bulk_import(request: Request):
    from utils.fast_import import SMART_IMPORT_MANAGER
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"smartBulkImport {data}")
    try:
        client = await get_clients()          # Lazy client
        imported_count, total_files, used_fast_import = await SMART_IMPORT_MANAGER.smart_bulk_import(
            client,
            data["channel"],
            data["path"],
            data.get("start_msg_id"),
            data.get("end_msg_id"),
            data.get("import_mode", "auto")
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


@app.post("/api/checkChannelAdmin")
async def check_channel_admin(request: Request):
    from utils.fast_import import SMART_IMPORT_MANAGER
    data = await request.json()
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        client = await get_clients()
        is_valid, result, is_admin = await SMART_IMPORT_MANAGER.validate_channel_access(client, data["channel"])
        if not is_valid:
            return JSONResponse({"status": "error", "message": result})
        return JSONResponse({
            "status": "ok",
            "is_admin": is_admin,
            "channel_name": getattr(result, "title", None) or getattr(result, "username", None) or str(getattr(result, "id", result))
        })
    except Exception as e:
        logger.error(f"Check channel admin error: {e}", exc_info=True)
        return JSONResponse({"status": "error", "message": str(e)})
