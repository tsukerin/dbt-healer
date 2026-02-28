import asyncio
import logging
import sys
import asyncpg

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from common.config import get_config

config = get_config()
bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    This handler receives messages with `/start` command and adding users to db table
    """

    conn = await asyncpg.connect(
        database=config.dbt_project_name,
        port=config.db_port,
        user=config.db_username,
        password=config.db_password,
    )
    await conn.execute(
        f"insert into meta.ids (tid) values ($1) on conflict on constraint ids_tid_key do nothing;",
        str(message.chat.id)
    )
    await conn.close()

    await message.answer(f"Привет, {html.bold(message.from_user.full_name)}!\nТы успешно подписался на обновление Pull Request!")

async def main() -> None:
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
