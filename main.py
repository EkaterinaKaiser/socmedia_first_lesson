import pandas as pd
import numpy as np
import asyncio
from parser import fetch_all_messages  # Убедитесь, что вы импортируете нужную функцию

comment = ["Эта идея хорошо подходит для нашего проекта, молодцы!",
           "Хорошо, что вы подняли эту тему, она очень важна!",
           "Мне нравится, как вы изложили свои мысли, очень хорошо!",
           "К сожалению, ситуация здесь выглядит плохо, есть над чем поработать.",
           "Плохо, что такие мнения появляются, это не конструктивно.",
           "Увы, реализация идеи вышла плохо, нужно больше усилий!"]
np.random.seed(3)
like = np.random.randint(100, 600, (6, ))
repost = np.random.randint(10, 60, (6, ))
view = np.random.randint(500, 2500, (6, ))

# 1. Анализ списка comment: количество символов и слов в каждом комментарии
for c in comment:
    num_chars = len(c)
    num_words = len(c.split())
    print(f"Комментарий: '{c}' - Символов: {num_chars}, Слов: {num_words}")

# 2. Анализ тональности комментариев
for index, c in enumerate(comment):
    if "хорошо" in c:
        sentiment = "положительная"
    elif "плохо" in c:
        sentiment = "отрицательная"
    else:
        sentiment = "нейтральная"
    print(f"Индекс: {index}, Тональность: {sentiment}")

# 3. Создание DataFrame
data = {
    "comment": comment,
    "like": like,
    "repost": repost,
    "view": view
}
df = pd.DataFrame(data)

# 4. Вывод комментария с наибольшим количеством лайков
max_likes_index = df['like'].idxmax()
print(f"Комментарий с наибольшими лайками: '{df.loc[max_likes_index, 'comment']}'")

# 5. Оценка вовлеченности
df['engagement'] = (df['like'] + df['repost']) / df['view']
print(df[['comment', 'engagement']])