import asyncio
import requests
import asyncpg

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

from common.config import get_config

config = get_config()

def get_last_pr():
    url = f"https://api.github.com/repos/{config.github_name}/{config.github_repo}/pulls"
    headers = {}
    headers['Authorization'] = f'token {config.github_token}'

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    return data

async def notify_about_pr(files: str) -> None:
    conn = await asyncpg.connect(
        database=config.db_database,
        user=config.db_username,
        password=config.db_password,
        port=config.db_port,
    )
    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    last_pr = get_last_pr()[0]['html_url']

    try:
        async with conn.transaction(): 
            async for id in conn.cursor("select tid from meta.ids"):
                if files:
                    try:
                        await bot.send_message(chat_id=id['tid'], 
                                            text=f"""
                                            При попытке теста обновленных моделей найдены ошибки в файлах {files}
                                            \nPR с предположительным решением ошибки: {last_pr}"""
                                            )
                    except TelegramBadRequest as e:
                        if 'chat not found' in str(e):
                            print(f"Чат с id {id['tid']} не найден. Пропускаю...")
                else:
                    await bot.send_message(chat_id=id['tid'], 
                                            text=f"""
                                            Упал пайплайн при попытке сборки коммита с обновленными моделями"""
                                            )
    finally:
        await bot.session.close()
    
