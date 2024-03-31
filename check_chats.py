import asyncio
import uuid
import json
import warnings
from datetime import timedelta, datetime

import httpx
import click
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from tqdm import tqdm, TqdmWarning
from pydantic import BaseModel

logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
warnings.filterwarnings("ignore", category=TqdmWarning)

endpoint = "https://gql.twitch.tv/gql"
session = httpx.AsyncClient(headers={"Client-ID": "kd1unb4b3q4t58fwlpcbzcbnm76a8fp", "Content-Type": "application/json"})


def calculate_time_difference(start_time: str, current_time: str):
    start_datetime = datetime.fromisoformat(start_time)
    current_datetime = datetime.fromisoformat(current_time)
    return (current_datetime - start_datetime).total_seconds()


class SimpleChat(BaseModel):
    id: str
    cursor: str
    login: str
    displayName: str
    user_id: str
    text: str
    offset: float

    @classmethod
    def from_raw_data(cls, raw_data: dict, start_time: str):
        return cls(id=raw_data["node"]["id"], cursor=raw_data["cursor"], login=raw_data["node"]["commenter"]["login"],
                   displayName=raw_data["node"]["commenter"]["displayName"],
                   user_id=raw_data["node"]["commenter"]["id"],
                   text="".join([fragment["text"] for fragment in raw_data["node"]["message"]["fragments"]]),
                   offset=calculate_time_difference(start_time, raw_data["node"]["createdAt"]))


class ExportChat(BaseModel):
    displayName: str
    text: str
    offset: float

    @classmethod
    def from_simple_chat(cls, raw_data: SimpleChat, start_seconds):
        return cls(displayName=raw_data.displayName, text=raw_data.text,
                   offset=raw_data.offset - start_seconds)


def VideoCommentByCursorReq(video_id: int, cursor: str):
    return [
        {
            "operationName": "VideoCommentsByOffsetOrCursor",
            "variables": {
                "videoID": str(video_id),
                "cursor": cursor
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a"
                }
            }
        }
    ]


def VideoCommentByOffsetReq(video_id: int, offset: int):
    return [
        {
            "operationName": "VideoCommentsByOffsetOrCursor",
            "variables": {
                "videoID": str(video_id),
                "contentOffsetSeconds": offset
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a"
                }
            }
        }
    ]


def ChannelVideoCoreReq(video_id: int):
    return [
        {
            "operationName": "ChannelVideoCore",
            "variables": {
                "videoID": str(video_id)
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "cf1ccf6f5b94c94d662efec5223dfb260c9f8bf053239a76125a58118769e8e2"
                }
            }
        }
    ]


def VideoMetadataReq(channel_login: str, video_id: int):
    return [
        {
            "operationName": "VideoMetadata",
            "variables": {
                "channelLogin": channel_login,
                "videoID": str(video_id)
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "c25707c1e5176320ceac6b447d052480887e23bc794ca1d02becd0bcc91844fe"
                }
            }
        }
    ]


def get_sec(time_str):
    """Get seconds from time."""
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


@retry(stop=stop_after_attempt(5), retry=retry_if_exception_type(httpx.TimeoutException))
async def get_chat_by_offset(video_id: int, offset: int, start_time: str):
    resp = (await session.post(endpoint, json=VideoCommentByOffsetReq(video_id, offset))).json()
    chats = resp[0]["data"]["video"]["comments"]["edges"]
    chats = [i for i in chats if i["node"]["commenter"]]
    next_page = resp[0]["data"]["video"]["comments"]["pageInfo"]["hasNextPage"]
    part_time = resp[0]["data"]["video"]["comments"]["edges"][-1]["node"]["contentOffsetSeconds"]
    return {"next": next_page, "chats": [SimpleChat.from_raw_data(raw_data, start_time) for raw_data in chats],
            "part_time": part_time}


@retry(stop=stop_after_attempt(5), retry=retry_if_exception_type(httpx.TimeoutException))
async def get_chat_by_cursor(video_id: int, cursor: str, start_time: str):
    resp = (await session.post(endpoint, json=VideoCommentByCursorReq(video_id, cursor))).json()
    chats = resp[0]["data"]["video"]["comments"]["edges"]
    chats = [i for i in chats if i["node"]["commenter"]]
    next_page = resp[0]["data"]["video"]["comments"]["pageInfo"]["hasNextPage"]
    part_time = resp[0]["data"]["video"]["comments"]["edges"][-1]["node"]["contentOffsetSeconds"]
    return {"next": next_page, "chats": [SimpleChat.from_raw_data(raw_data, start_time) for raw_data in chats],
            "part_time": part_time}


async def get_video_start_time(video_id: int):
    channel_resp = (await session.post(endpoint, json=ChannelVideoCoreReq(video_id))).json()
    channel_login = channel_resp[0]["data"]["video"]["owner"]["login"]
    metadata = (await session.post(endpoint,
                                   json=VideoMetadataReq(channel_login=channel_login, video_id=video_id))).json()
    return metadata[0]["data"]["video"]["createdAt"]


async def get_video_length(video_id: int):
    channel_resp = (await session.post(endpoint, json=ChannelVideoCoreReq(video_id))).json()
    channel_login = channel_resp[0]["data"]["video"]["owner"]["login"]
    metadata = (await session.post(endpoint,
                                   json=VideoMetadataReq(channel_login=channel_login, video_id=video_id))).json()
    return metadata[0]["data"]["video"]["lengthSeconds"]


async def get_all_chat(video_id: int):
    start_time = await get_video_start_time(video_id)
    video_length = await get_video_length(video_id)
    pbar = tqdm(total=video_length)
    result = []
    cursor = None
    while True:
        logger.info("Fetching...")
        if not cursor:
            resp = await get_chat_by_offset(video_id, 0, start_time)
        else:
            resp = await get_chat_by_cursor(video_id, cursor, start_time)
        result.extend(resp["chats"])
        pbar.update(resp["part_time"] - pbar.n)
        if resp["next"]:
            cursor = resp["chats"][-1].cursor
        else:
            break
    return result


async def get_part_chat(video_id: int, start_seconds: int, stop_seconds: int):
    logger.info("Fetching chats...")
    start_time = await get_video_start_time(video_id)
    pbar = tqdm(total=stop_seconds - start_seconds)
    result = []
    cursor = None
    stop = True
    while stop:
        if not cursor:
            resp = await get_chat_by_offset(video_id, start_seconds, start_time)
        else:
            resp = await get_chat_by_cursor(video_id, cursor, start_time)
        for chat in resp["chats"]:
            if chat.offset > stop_seconds:
                stop = False
                break
            result.append(chat)
            pbar.update(chat.offset - start_seconds - pbar.n)
        if resp["next"]:
            cursor = resp["chats"][-1].cursor
        else:
            break
    return result


def search_keywords(chats, keywords, start_seconds):
    results = []
    for chat in chats:
        for keyword in keywords:
            if keyword in chat.displayName or keyword in chat.text:
                logger.info(f"[{timedelta(seconds=chat.offset - start_seconds)}]{chat.displayName}:{chat.text}")
                results.append(chat)
    return results


def export(chats, start_seconds):
    with open(f"export_{uuid.uuid4().hex[:6]}.json", "w") as f:
        export_chats = [ExportChat.from_simple_chat(chat, start_seconds).model_dump() for chat in chats]
        final_chats = [dict(t) for t in {tuple(d.items()) for d in export_chats}]
        final_chats.sort(key=lambda x: x["offset"])
        json.dump(final_chats, f)


async def main(video_id, start_time, end_time, keywords):
    chats = await get_part_chat(video_id, get_sec(start_time), get_sec(end_time))
    matched_chats = search_keywords(chats, keywords, get_sec(start_time))
    if matched_chats:
        export(matched_chats, get_sec(start_time))


@click.command()
@click.option("--video_id", required=True, type=int)
@click.option("--start_time", help="HH:mm:ss", required=True)
@click.option("--end_time", help="HH:mm:ss", required=True)
@click.option("--keywords", required=True)
def process(video_id, start_time, end_time, keywords):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(video_id, start_time, end_time, keywords))


if __name__ == '__main__':
    process()
