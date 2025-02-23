from telethon import TelegramClient
from telethon.errors import ChannelPrivateError
import pandas as pd
import asyncio
from datetime import datetime

# Настройки Telegram API
api_id = # ваш id
api_hash = '' # ваш hash
channel_username = '' # канал в телеграме

# Создание клиента
client = TelegramClient('session_name', api_id, api_hash)

async def analyze_messages(messages):
    if not messages:
        return
    
    # Подсчет средних показателей
    total_views = sum(msg['views'] for msg in messages)
    total_likes = sum(msg['likes'] for msg in messages)
    total_forwards = sum(msg['forwards'] for msg in messages)
    
    avg_views = total_views / len(messages)
    avg_likes = total_likes / len(messages)
    avg_forwards = total_forwards / len(messages)
    
    print("\nСтатистика канала:")
    print(f"Среднее количество просмотров: {avg_views:.2f}")
    print(f"Среднее количество лайков: {avg_likes:.2f}")
    print(f"Среднее количество репостов: {avg_forwards:.2f}")

async def fetch_all_messages():
    try:
        await client.start()
        channel_entity = await client.get_entity(channel_username)

        print(f"Получение всех сообщений из канала {channel_username}...")

        # Создаем CSV файл и записываем заголовки
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f'telegram_messages_{timestamp}.csv'
        pd.DataFrame(columns=['id', 'date', 'text', 'views', 'forwards', 'likes']).to_csv(csv_filename, index=False, encoding='utf-8-sig')

        offset_id = 0
        limit = 100
        total_messages = 0
        all_messages = []
        last_milestone = 0  # Для отслеживания последней отметки в 10000 сообщений

        while True:
            batch_messages = await client.get_messages(channel_entity, limit=limit, offset_id=offset_id)
            if not batch_messages:
                break

            messages = []
            for message in batch_messages:
                if message.text:
                    try:
                        message_data = {
                            'id': message.id,
                            'date': message.date.strftime('%Y-%m-%d %H:%M:%S'),
                            'text': message.text,
                            'views': getattr(message, 'views', 0),
                            'forwards': getattr(message, 'forwards', 0),
                            'likes': sum(reaction.count for reaction in message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0
                        }
                        messages.append(message_data)
                        all_messages.append(message_data)
                    except Exception as e:
                        print(f"Ошибка при обработке сообщения {message.id}: {e}")
                        continue

            if messages:
                # Сохраняем текущую партию сообщений в CSV
                df = pd.DataFrame(messages)
                df.to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
                total_messages += len(messages)
                
                # Проверяем, достигли ли мы следующей отметки в 10000 сообщений
                current_milestone = (total_messages // 10000) * 10000
                if current_milestone > last_milestone:
                    print(f"\n=== Достигнута отметка {current_milestone} сообщений ===")
                    print(f"Последнее сообщение от: {messages[-1]['date']}")
                    print(f"ID последнего сообщения: {messages[-1]['id']}")
                    last_milestone = current_milestone

                print(f"Сохранено {len(messages)} сообщений. Всего: {total_messages}")

            if len(batch_messages) < limit:
                break

            offset_id = batch_messages[-1].id
            await asyncio.sleep(1)  # Задержка между запросами

        print(f"\nВсего собрано и сохранено {total_messages} сообщений.")
        await client.disconnect()
        return csv_filename, all_messages

    except ChannelPrivateError:
        print(f"Канал {channel_username} является приватным или недоступным.")
        return None, None
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return None, None
    finally:
        if client.is_connected():
            await client.disconnect()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    csv_filename, messages = loop.run_until_complete(fetch_all_messages())

    if csv_filename and messages:
        print(f"Сообщения сохранены в {csv_filename}")
        # Анализируем сообщения
        loop.run_until_complete(analyze_messages(messages))