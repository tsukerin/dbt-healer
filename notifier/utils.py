import asyncio
import os
import requests
import asyncpg

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from common.config import GITHUB_USERNAME, REPO_NAME, GITHUB_TOKEN, BOT_TOKEN, DB_USERNAME, DB_PASSWORD, DBT_PROJECT_NAME

def get_last_pr():
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{REPO_NAME}/pulls"
    headers = {}
    headers['Authorization'] = f'token {GITHUB_TOKEN}'

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    return data

async def notify_about_pr():
    conn = await asyncpg.connect(database=DBT_PROJECT_NAME, user=DB_USERNAME, password=DB_PASSWORD)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    last_pr = get_last_pr()[0]['html_url']

    try:
        async with conn.transaction(): 
            async for id in conn.cursor("select tid from meta.ids"):
                await bot.send_message(chat_id=id['tid'], text=f"Новый PR!\nURL: {last_pr}")
    finally:
        await bot.session.close()
    