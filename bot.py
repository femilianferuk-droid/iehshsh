import asyncio
import logging
import multiprocessing
import os
import signal
import sys
import json
from typing import Dict
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import aiohttp

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
MAX_BOTS = 10  # Максимальное количество одновременно запущенных ботов
BOTS_DATA_FILE = 'running_bots.json'  # Файл для сохранения информации о ботах


class BotProcess(multiprocessing.Process):
    """Класс для запуска бота в отдельном процессе"""
    
    def __init__(self, token: str, bot_id: str):
        super().__init__()
        self.token = token
        self.bot_id = bot_id
        self.daemon = True
        
    def run(self):
        """Запуск бота в процессе"""
        try:
            asyncio.run(run_child_bot(self.token, self.bot_id))
        except Exception as e:
            logger.error(f"Error in bot process {self.bot_id}: {e}")


class BotManager:
    """Менеджер для управления запущенными ботами"""
    
    def __init__(self):
        self.processes: Dict[str, BotProcess] = {}
        self.tokens: Dict[str, str] = {}
        self.load_saved_bots()
        
    def load_saved_bots(self):
        """Загрузка информации о сохраненных ботах"""
        try:
            if os.path.exists(BOTS_DATA_FILE):
                with open(BOTS_DATA_FILE, 'r') as f:
                    data = json.load(f)
                    self.tokens = data.get('tokens', {})
                    logger.info(f"Loaded {len(self.tokens)} saved bots")
                    
                    # Перезапускаем сохраненных ботов
                    for bot_id, token in self.tokens.items():
                        logger.info(f"Restarting bot {bot_id}")
                        process = BotProcess(token, bot_id)
                        process.start()
                        self.processes[bot_id] = process
                        
        except Exception as e:
            logger.error(f"Error loading saved bots: {e}")
            
    def save_bots(self):
        """Сохранение информации о запущенных ботах"""
        try:
            with open(BOTS_DATA_FILE, 'w') as f:
                json.dump({'tokens': self.tokens}, f)
        except Exception as e:
            logger.error(f"Error saving bots: {e}")
            
    def add_bot(self, bot_id: str, token: str, process: BotProcess):
        """Добавление нового бота"""
        self.processes[bot_id] = process
        self.tokens[bot_id] = token
        self.save_bots()
        logger.info(f"Added bot {bot_id}. Total bots: {len(self.processes)}")
        
    def remove_bot(self, bot_id: str):
        """Удаление бота"""
        if bot_id in self.processes:
            process = self.processes[bot_id]
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            del self.processes[bot_id]
            logger.info(f"Removed bot {bot_id}")
            
        if bot_id in self.tokens:
            del self.tokens[bot_id]
            self.save_bots()
            
    def stop_all_bots(self):
        """Остановка всех ботов"""
        for bot_id in list(self.processes.keys()):
            self.remove_bot(bot_id)
        logger.info("All bots stopped")
            
    def get_bot_count(self) -> int:
        """Получение количества запущенных ботов"""
        return len(self.processes)
    
    def is_token_used(self, token: str) -> bool:
        """Проверка, используется ли токен"""
        return token in self.tokens.values()


# Глобальный менеджер ботов
bot_manager = BotManager()


async def validate_token(token: str) -> bool:
    """Проверка валидности токена через API Telegram"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{token}/getMe"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('ok', False)
                return False
    except Exception as e:
        logger.error(f"Error validating token: {e}")
        return False


def get_main_keyboard() -> InlineKeyboardMarkup:
    """Создание основной клавиатуры"""
    keyboard = [
        [InlineKeyboardButton(text="🤖 Захостить бота", callback_data="host_bot")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🛑 Остановить бота", callback_data="stop_bot")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def send_welcome(message: Message, bot_name: str = "Главный бот"):
    """Отправка приветственного сообщения"""
    welcome_text = (
        f"👋 Добро пожаловать в {bot_name}!\n\n"
        f"Я могу создавать копии самого себя. "
        f"Нажми кнопку 'Захостить бота', чтобы создать нового бота.\n\n"
        f"Активных ботов: {bot_manager.get_bot_count()}/{MAX_BOTS}"
    )
    await message.answer(welcome_text, reply_markup=get_main_keyboard())


async def run_child_bot(token: str, bot_id: str):
    """Запуск дочернего бота"""
    logger.info(f"Starting child bot {bot_id}")
    
    # Создаем экземпляр бота и диспетчера
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    # Регистрируем обработчики для дочернего бота
    @dp.message(Command("start"))
    async def start_command(message: Message):
        await send_welcome(message, f"Бот #{bot_id[:8]}")
    
    @dp.callback_query(F.data == "host_bot")
    async def host_bot_callback(callback: CallbackQuery):
        await callback.message.edit_text(
            "📝 Пожалуйста, отправьте токен бота, которого нужно запустить:"
        )
        await callback.answer()
    
    @dp.message(F.text & ~F.text.startswith('/'))
    async def handle_token(message: Message):
        token = message.text.strip()
        
        # Проверяем валидность токена
        if not await validate_token(token):
            await message.answer(
                "❌ Неверный токен. Пожалуйста, проверьте и попробуйте снова.",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Проверяем, не используется ли уже этот токен
        if bot_manager.is_token_used(token):
            await message.answer(
                "❌ Этот токен уже используется другим ботом.",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Проверяем лимит ботов
        if bot_manager.get_bot_count() >= MAX_BOTS:
            await message.answer(
                f"❌ Достигнут максимальный лимит ботов ({MAX_BOTS}).",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Создаем и запускаем нового бота
        new_bot_id = f"bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        process = BotProcess(token, new_bot_id)
        process.start()
        
        bot_manager.add_bot(new_bot_id, token, process)
        
        await message.answer(
            f"✅ Новый бот успешно запущен!\n"
            f"ID: {new_bot_id}\n"
            f"Всего активных ботов: {bot_manager.get_bot_count()}/{MAX_BOTS}",
            reply_markup=get_main_keyboard()
        )
    
    @dp.callback_query(F.data == "stats")
    async def stats_callback(callback: CallbackQuery):
        stats_text = (
            f"📊 Статистика бота:\n\n"
            f"Активных ботов: {bot_manager.get_bot_count()}/{MAX_BOTS}\n"
        )
        
        if bot_manager.processes:
            stats_text += "\nЗапущенные боты:\n"
            for bot_id in bot_manager.processes.keys():
                stats_text += f"• {bot_id[:8]}\n"
        
        await callback.message.edit_text(stats_text, reply_markup=get_main_keyboard())
        await callback.answer()
    
    @dp.callback_query(F.data == "stop_bot")
    async def stop_bot_callback(callback: CallbackQuery):
        await callback.message.edit_text(
            "⚠️ Для остановки дочерних ботов используйте главный бот.",
            reply_markup=get_main_keyboard()
        )
        await callback.answer()
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in child bot {bot_id}: {e}")
    finally:
        await bot.session.close()


async def main():
    """Главная функция для запуска основного бота"""
    # Получаем токен основного бота из переменных окружения
    MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
    
    if not MAIN_BOT_TOKEN:
        logger.error("=" * 50)
        logger.error("ОШИБКА: Токен главного бота не найден в переменных окружения!")
        logger.error("=" * 50)
        logger.error("\nКак установить токен:")
        logger.error("1. В командной строке (Linux/Mac):")
        logger.error("   export MAIN_BOT_TOKEN='ваш_токен_здесь'")
        logger.error("\n2. В командной строке (Windows):")
        logger.error("   set MAIN_BOT_TOKEN=ваш_токен_здесь")
        logger.error("\n3. Или создайте файл .env с содержимым:")
        logger.error("   MAIN_BOT_TOKEN=ваш_токен_здесь")
        logger.error("=" * 50)
        sys.exit(1)
    
    logger.info(f"Main bot token found: {MAIN_BOT_TOKEN[:10]}...")
    
    # Создаем экземпляры бота и диспетчера
    bot = Bot(token=MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    # Регистрируем обработчики для основного бота
    @dp.message(Command("start"))
    async def start_command(message: Message):
        await send_welcome(message)
    
    @dp.message(Command("stop_all"))
    async def stop_all_command(message: Message):
        """Команда для остановки всех ботов"""
        bot_manager.stop_all_bots()
        await message.answer("🛑 Все боты остановлены.")
    
    @dp.callback_query(F.data == "host_bot")
    async def host_bot_callback(callback: CallbackQuery):
        await callback.message.edit_text(
            "📝 Пожалуйста, отправьте токен бота, которого нужно запустить:"
        )
        await callback.answer()
    
    @dp.callback_query(F.data == "stats")
    async def stats_callback(callback: CallbackQuery):
        stats_text = (
            f"📊 Статистика главного бота:\n\n"
            f"Активных ботов: {bot_manager.get_bot_count()}/{MAX_BOTS}\n"
        )
        
        if bot_manager.processes:
            stats_text += "\nЗапущенные боты:\n"
            for bot_id, process in bot_manager.processes.items():
                status = "🟢 Работает" if process.is_alive() else "🔴 Остановлен"
                stats_text += f"• {bot_id[:8]}: {status}\n"
        
        await callback.message.edit_text(stats_text, reply_markup=get_main_keyboard())
        await callback.answer()
    
    @dp.callback_query(F.data == "stop_bot")
    async def stop_bot_callback(callback: CallbackQuery):
        if not bot_manager.processes:
            await callback.message.edit_text(
                "❌ Нет запущенных ботов для остановки.",
                reply_markup=get_main_keyboard()
            )
            await callback.answer()
            return
        
        # Создаем клавиатуру для выбора бота для остановки
        keyboard = []
        for bot_id in bot_manager.processes.keys():
            keyboard.append([InlineKeyboardButton(
                text=f"🛑 Остановить {bot_id[:8]}",
                callback_data=f"stop_{bot_id}"
            )])
        
        keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back")])
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        
        await callback.message.edit_text(
            "Выберите бота для остановки:",
            reply_markup=markup
        )
        await callback.answer()
    
    @dp.callback_query(F.data.startswith("stop_"))
    async def stop_specific_bot(callback: CallbackQuery):
        bot_id = callback.data.replace("stop_", "")
        
        if bot_id in bot_manager.processes:
            bot_manager.remove_bot(bot_id)
            await callback.message.edit_text(
                f"✅ Бот {bot_id[:8]} успешно остановлен.",
                reply_markup=get_main_keyboard()
            )
        else:
            await callback.message.edit_text(
                "❌ Бот не найден.",
                reply_markup=get_main_keyboard()
            )
        await callback.answer()
    
    @dp.callback_query(F.data == "back")
    async def back_callback(callback: CallbackQuery):
        await callback.message.edit_text(
            "Главное меню:",
            reply_markup=get_main_keyboard()
        )
        await callback.answer()
    
    @dp.message(F.text & ~F.text.startswith('/'))
    async def handle_token(message: Message):
        token = message.text.strip()
        
        # Проверяем валидность токена
        if not await validate_token(token):
            await message.answer(
                "❌ Неверный токен. Пожалуйста, проверьте и попробуйте снова.",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Проверяем, не используется ли уже этот токен
        if bot_manager.is_token_used(token):
            await message.answer(
                "❌ Этот токен уже используется другим ботом.",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Проверяем лимит ботов
        if bot_manager.get_bot_count() >= MAX_BOTS:
            await message.answer(
                f"❌ Достигнут максимальный лимит ботов ({MAX_BOTS}).",
                reply_markup=get_main_keyboard()
            )
            return
        
        # Создаем и запускаем нового бота
        new_bot_id = f"bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        process = BotProcess(token, new_bot_id)
        process.start()
        
        bot_manager.add_bot(new_bot_id, token, process)
        
        await message.answer(
            f"✅ Новый бот успешно запущен!\n"
            f"ID: {new_bot_id}\n"
            f"Всего активных ботов: {bot_manager.get_bot_count()}/{MAX_BOTS}",
            reply_markup=get_main_keyboard()
        )
    
    # Настройка обработки сигналов для graceful shutdown
    async def shutdown_handler():
        logger.info("Shutting down...")
        bot_manager.stop_all_bots()
        await dp.stop_polling()
        await bot.session.close()
    
    # Регистрируем обработчики сигналов
    for sig in [signal.SIGINT, signal.SIGTERM]:
        asyncio.get_event_loop().add_signal_handler(
            sig, lambda: asyncio.create_task(shutdown_handler())
        )
    
    try:
        logger.info("Starting main bot polling...")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    # Важно для Windows: устанавливаем метод запуска процессов
    if sys.platform == "win32":
        multiprocessing.set_start_method('spawn', force=True)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
