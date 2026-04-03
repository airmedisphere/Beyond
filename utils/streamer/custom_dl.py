import asyncio
from typing import Dict, Union
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from utils.logger import Logger

logger = Logger(__name__)


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 15 * 60  # 15 minutes
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, channel, message_id: int) -> FileId:
        """Get file properties with caching"""
        try:
            if message_id not in self.cached_file_ids:
                await self.generate_file_properties(channel, message_id)
            return self.cached_file_ids[message_id]
        except Exception as e:
            logger.error(f"Error getting file properties: {e}")
            return None

    async def generate_file_properties(self, channel, message_id: int) -> FileId:
        """Generate and cache file properties"""
        try:
            file_id = await get_file_ids(self.client, channel, message_id)
            if not file_id:
                raise Exception("FileNotFound")
            self.cached_file_ids[message_id] = file_id
            return self.cached_file_ids[message_id]
        except Exception as e:
            logger.error(f"Error generating file properties for message {message_id}: {e}")
            raise

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        """
        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client, file_id.dc_id, await client.storage.test_mode()
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for _ in range(6):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )

                    try:
                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug(f"Invalid authorization bytes for DC {file_id.dc_id}")
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

            logger.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            logger.debug(f"Using cached media session for DC {file_id.dc_id}")

        return media_session

    @staticmethod
    async def get_location(file_id: FileId):
        """Returns the file location for the media file."""
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """Custom generator that yields the bytes of the media file."""
        client = self.client
        logger.debug(f"Starting to yield file with chunk size {chunk_size}")

        media_session = await self.generate_media_session(client, file_id)
        current_part = 1
        location = await self.get_location(file_id)

        try:
            r = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=chunk_size
                )
            )

            while True:
                if not isinstance(r, raw.types.upload.File) or not r.bytes:
                    break

                chunk = r.bytes
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk

                current_part += 1
                offset += chunk_size

                if current_part > part_count:
                    break

                r = await media_session.invoke(
                    raw.functions.upload.GetFile(
                        location=location, offset=offset, limit=chunk_size
                    )
                )

        except (TimeoutError, AttributeError) as e:
            logger.warning(f"Timeout/Attribute error during streaming: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during file streaming: {e}")
            raise
        finally:
            logger.debug(f"Finished yielding file with {current_part-1} parts.")

    async def clean_cache(self) -> None:
        """Periodically clean cache to reduce memory usage"""
        while True:
            await asyncio.sleep(self.clean_timer)
            cache_size = len(self.cached_file_ids)
            self.cached_file_ids.clear()
            logger.debug(f"Cleaned cache - removed {cache_size} entries")
