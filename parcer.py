import json
import csv

from telethon.sync import TelegramClient
from telethon import connection

# для корректного переноса времени сообщений в json
from datetime import date, datetime

# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.messages import GetRepliesRequest
import telethon


api_id = 19089654
api_hash = 'b753ec7c3e76f9fd7a9b6a1bcf6354c7'
client = TelegramClient('session_name', api_id, api_hash)

# Присваиваем значения внутренним переменным


client.start()


async def dump_all_messages(channel):
    offset_msg = 0  # номер записи, с которой начинается считывание
    limit_msg = 1  # максимальное число записей, передаваемых за один раз

    all_messages = []  # список всех сообщений
    total_messages = 0
    total_count_limit = 1  # поменяйте это значение, если вам нужны не все сообщения

    class DateTimeEncoder(json.JSONEncoder):

        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()
            if isinstance(o, bytes):
                return list(o)
            return json.JSONEncoder.default(self, o)

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None, add_offset=0,
            limit=limit_msg, max_id=0, min_id=0,
            hash=0))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    for message in all_messages:
        post = message['id']
        all_comments = []
        comments = await client(GetRepliesRequest(
            peer=channel, msg_id=post, limit=100, offset_id=0, offset_date=None, add_offset=0, max_id=0, min_id=0, hash=0))
        for comment in comments.messages:
            all_comments.append(comment.to_dict())
        post_text = message['message']
        full_comments_text = ""
        for comment_text in all_comments:
            text = comment_text['message']
            full_comments_text = full_comments_text + "/" + text

        with open('tg_full_data.csv', 'a', newline='', encoding='utf8') as csvfile:
            fieldnames = ['id', 'post_text', 'views', 'forwards', 'comments_count', 'comments_text']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow({'id': message['id'], 'post_text': post_text, 'views': message['views'], 'forwards': message['forwards'], 'comments_count': comments.count, 'comments_text': full_comments_text})

    with open('channel_messages.json', 'w', encoding='utf8') as outfile:
        json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)


async def main():
    channels = ['https://t.me/nexta_live']
    for url in channels:
        channel = await client.get_entity(url)
        await dump_all_messages(channel)


with client:
    client.loop.run_until_complete(main())
