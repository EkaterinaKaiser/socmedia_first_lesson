from telethon import TelegramClient  # Импортируем клиент для работы с Telegram API
import pandas as pd  # Импортируем библиотеку pandas для работы с данными
import asyncio  # Импортируем библиотеку для асинхронного программирования
from datetime import datetime  # Импортируем класс datetime для работы с датами

# Настройки Telegram API
api_id =   # Укажите ваш API ID
api_hash = ''  # Укажите ваш API Hash
channel_username = 'piterach'  # Укажите имя канала в Telegram

# Создание клиента для подключения к Telegram
client = TelegramClient('session_name', api_id, api_hash)

async def fetch_all_messages():  # Определяем асинхронную функцию для получения всех сообщений
    await client.start()  # Запускаем клиента
    channel_entity = await client.get_entity(channel_username)  # Получаем объект канала по его имени

    # Создаем CSV файл и записываем заголовки
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # Получаем текущую дату и время в формате YYYYMMDD_HHMMSS
    csv_filename = f'telegram_messages_{timestamp}.csv'  # Формируем имя файла с сообщениями
    pd.DataFrame(columns=['id', 'date', 'text', 'views', 'forwards', 'likes']).to_csv(csv_filename, index=False, encoding='utf-8-sig')  # Создаем CSV файл с заголовками

    offset_id = 0  # Начальный идентификатор сообщения
    limit = 100  # Количество сообщений за один запрос
    total_messages = 0  # Счетчик общего количества сообщений

    while True:  # Бесконечный цикл для получения сообщений
        # Получаем сообщения из канала
        batch_messages = await client.get_messages(channel_entity, limit=limit, offset_id=offset_id)
        if not batch_messages:  # Если сообщений больше нет, выходим из цикла
            break

        messages = []  # Список для хранения сообщений
        for message in batch_messages:  # Обрабатываем каждое сообщение
            if message.text:  # Проверяем, есть ли текст в сообщении
                message_data = {  # Собираем данные о сообщении
                    'id': message.id,  # ID сообщения
                    'date': message.date.strftime('%Y-%m-%d %H:%M:%S'),  # Форматируем дату сообщения
                    'text': message.text,  # Текст сообщения
                    'views': getattr(message, 'views', 0),  # Получаем количество просмотров
                    'forwards': getattr(message, 'forwards', 0),  # Получаем количество репостов
                    'likes': sum(reaction.count for reaction in message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0  # Получаем количество лайков
                }
                messages.append(message_data)  # Добавляем данные сообщения в список

        if messages:  # Если есть сообщения для сохранения
            df = pd.DataFrame(messages)  # Создаем DataFrame из списка сообщений
            df.to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')  # Дописываем данные в CSV файл
            total_messages += len(messages)  # Увеличиваем счетчик общего количества сообщений

        offset_id = batch_messages[-1].id  # Обновляем offset_id для следующего запроса
        await asyncio.sleep(1)  # Задержка в 1 секунду между запросами

    await client.disconnect()  # Отключаем клиента
    return csv_filename, total_messages  # Возвращаем имя файла и общее количество сообщений

if __name__ == '__main__':  # Проверяем, что скрипт запущен напрямую
    loop = asyncio.get_event_loop()  # Создаем цикл событий
    csv_filename, total_messages = loop.run_until_complete(fetch_all_messages())  # Запускаем асинхронную функцию и получаем результаты

    # Выводим информацию о сохраненных сообщениях
    print(f"Сообщения сохранены в {csv_filename}, всего сообщений: {total_messages}")  # Выводим имя файла и общее количество сообщений