#!/usr/bin/env python
# coding: utf-8

# In[26]:


import requests
import pandas as pd
import os
from time import sleep
from datetime import date, timedelta
import gspread
import time
today = date.today()
thirty_days_ago = today - timedelta(days=30)
seven_days_ago = today - timedelta(days=7)
dateTo =  date.today().strftime('%Y-%m-%d')
dateFrom30 =thirty_days_ago.strftime('%Y-%m-%d')


# In[27]:


apiKey = '501052d0-08f2-4033-bcba-2fd91ab58939'
clientId = '132938'
headers = {
        'Client-Id': clientId,
        'Api-Key': apiKey
      }


# In[28]:


# Список товаров
url ='https://api-seller.ozon.ru/v3/product/list'
params ={'limit':1000,
          'filter':{'visibility':'ALL'}}
res = requests.post(url,headers = headers,json = params)
table = res.json()
last_id = table['result']['last_id']
table1 = pd.DataFrame(table['result']['items'])
table1.columns = ['ID','Артикул','fbo','fbs','a','o','i']
tovari = table1[['Артикул']]

tovari_all = tovari


# In[29]:


# Остатки товаров
url = 'https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses'
limit = 1000
offset = 0
tables = []

while True:
    params = {'limit': limit, 'offset': offset}
    res = requests.post(url, headers=headers, json=params)
    table = res.json()

    if 'result' in table and 'rows' in table['result'] and table['result']['rows']:
        tables.append(table)
        offset += limit
    else:
        break

        
ost_sait = pd.concat([pd.DataFrame(table['result']['rows']) for table in tables])
ost_sait.columns = ['ID','Склад','Артикул','Название','В пути','Ост_сайт','Резерв']
ost_sait = ost_sait[['ID','Склад','Артикул','В пути','Ост_сайт']]


# In[30]:


# МЕТРИКИ
url = 'https://api-seller.ozon.ru/v1/analytics/data'
offset = 0
limit = 1000  # Ограничение запроса
data = []

while True:
    params = {
        "date_from": dateFrom30,
        "date_to": dateTo,
        "limit": limit,
        "offset": offset,
        "dimension": ["sku", "day"],
        "metrics": ["revenue", "ordered_units"]
    }
    res = requests.post(url, headers=headers, json=params)
    
    # Проверка на ошибку превышения лимита запросов
    if res.status_code == 429:
        time.sleep(1)  # Пауза в 1 секунду, если превышен лимит
        continue
    
    table = res.json()

    # Проверка на наличие данных в ответе
    if not table['result']['data']:
        break

    for row in table['result']['data']:
        dimensions = row['dimensions']
        metrics = row['metrics']
        data.append({
            "sku": dimensions[0]["id"],
            "day": dimensions[1]["id"],
            "revenue": metrics[0],
            "ordered_units": metrics[1]
        })

    # Увеличиваем offset
    offset += limit
    time.sleep(0.5)  # Пауза 0.5 секунды между запросами

metrics = pd.DataFrame(data)

metrics['Дата'] = pd.to_datetime(metrics['day']).dt.strftime('%d.%m')
metrics.sort_values(by='Дата', ascending=False, inplace=True)


# In[31]:


# Распределение по датам ЗАКАЗЫ

grouped = metrics.groupby('sku')

# Создание нового DataFrame
result1 = pd.DataFrame()

# Итерация по группам
for sku, group in grouped:
    # Суммирование заказов по датам
    orders_by_date = group.groupby('Дата')['ordered_units'].sum()

    # Преобразование в DataFrame
    orders_by_date = pd.DataFrame(orders_by_date).T

    # Добавление артикула в качестве первого столбца
    orders_by_date.insert(0, 'sku', sku)

    # Объединение с результатом
    result1 = pd.concat([result1, orders_by_date], ignore_index=True)
    result1 = result1.fillna(0)
    
for col in result1.columns[1:]: # Начиная с 3-го столбца (индекс 2)
    result1[col] = result1[col].astype(int)
    
result1.set_index('sku', inplace=True)


# Сортируем колонки по дате, исключая 'sku' из списка
result1 = result1.reindex(sorted(result1.columns, key=lambda x: pd.to_datetime(x, format='%d.%m')), axis=1)   


# In[32]:


# def get_item_info(offer_id): # Определение функции get_item_info
#     url = 'https://api-seller.ozon.ru/v3/product/info/list'
#     params = {'offer_id': [offer_id]} # Передаем offer_id как список с одним элементом
#     try:
#         res = requests.post(url, headers=headers, json=params)
#         if res.status_code == 200:
#             data = res.json()
#             if 'items' in data and data['items']:
#                 return data['items'][0]
#             else:
#                 return None # Обработка пустого списка items
#         else:
#             print(f"Ошибка при запросе к API для offer_id {offer_id}: Код статуса {res.status_code}")
#             return None # Возвращаем None при ошибке API
#     except requests.exceptions.RequestException as e:
#         print(f"Ошибка сети для offer_id {offer_id}: {e}")
#         return None # Возвращаем None при ошибке сети

# spisok_art = list(tovari_all['Артикул'])
# data = []

# for offer_id in spisok_art:
#     item_info = get_item_info(offer_id)
#     if item_info: # Проверяем, что item_info не None
#         try: # Обрабатываем возможные ошибки KeyError
#             data.append({
#                 'offer_id': item_info['offer_id'],
#                 'barcode': item_info['barcodes'][0] if item_info['barcodes'] else None, # Безопасное извлечение barcode
#                 'primary_image': item_info['primary_image'][0] if item_info['primary_image'] else None, # Безопасное извлечение primary_image
#                 'id': item_info['sources'][0]['sku'],
#                 'old_price': item_info['old_price'],
#                 'marketing_price': item_info['marketing_price'],
#                 'price': item_info['price']
#             })
#         except (KeyError, IndexError, TypeError) as e:
#             print(f"Ошибка обработки данных для offer_id {offer_id}: {e}")
# tovari_all = pd.DataFrame(data)

# tovari_all.columns = ['Артикул','Баркод','Фото','ID','Цена_розничная','Цена_СПП','Цена_без_СПП']
# tovari_all[['Цена_розничная', 'Цена_СПП', 'Цена_без_СПП']] = tovari_all[['Цена_розничная', 'Цена_СПП', 'Цена_без_СПП']].astype(float).astype(int)
# tovari_all = tovari_all[['Фото','Артикул', 'ID', 'Баркод', 'Цена_розничная', 'Цена_СПП', 'Цена_без_СПП']]


# In[33]:


def get_item_info(offer_id):
    url = 'https://api-seller.ozon.ru/v3/product/info/list'
    params = {'offer_id': [offer_id]}
    try:
        res = requests.post(url, headers=headers, json=params)
        if res.status_code == 200:
            data = res.json()
            if 'items' in data and data['items']:
                return data['items'][0]
            else:
                print(f"Пустой ответ для offer_id {offer_id}")
                return None
        else:
            print(f"Ошибка API для offer_id {offer_id}: Код статуса {res.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сети для offer_id {offer_id}: {e}")
        return None

spisok_art = list(tovari_all['Артикул'])
data = []

for offer_id in spisok_art:
    item_info = get_item_info(offer_id)
    if item_info:
        try:
            data.append({
                'offer_id': item_info['offer_id'],
                'barcode': item_info['barcodes'][0] if item_info.get('barcodes') else None,
                'primary_image': item_info['primary_image'][0] if item_info.get('primary_image') else None,
                'id': item_info['sources'][0]['sku'] if item_info.get('sources') and item_info['sources'] else None,
                'old_price': item_info.get('old_price'),
                'marketing_price': item_info.get('marketing_price'),
                'price': item_info.get('price')
            })
        except (KeyError, IndexError, TypeError) as e:
            print(f"Ошибка обработки данных для offer_id {offer_id}: {e}")

# Создаем DataFrame только если есть данные
if data:
    tovari_all = pd.DataFrame(data)
    
    # Переименовываем столбцы
    tovari_all.columns = ['Артикул','Баркод','Фото','ID','Цена_розничная','Цена_СПП','Цена_без_СПП']
    
    # Преобразуем цены в числа, обрабатывая возможные ошибки
    price_columns = ['Цена_розничная', 'Цена_СПП', 'Цена_без_СПП']
    for col in price_columns:
        tovari_all[col] = pd.to_numeric(tovari_all[col], errors='coerce').fillna(0).astype(int)
    
    # Изменяем порядок столбцов
    tovari_all = tovari_all[['Фото','Артикул', 'ID', 'Баркод', 'Цена_розничная', 'Цена_СПП', 'Цена_без_СПП']]
    
    print(f"Успешно обработано {len(tovari_all)} товаров")
else:
    print("Нет данных для создания DataFrame")
    tovari_all = pd.DataFrame(columns=['Фото','Артикул', 'ID', 'Баркод', 'Цена_розничная', 'Цена_СПП', 'Цена_без_СПП'])


# In[34]:


gc = gspread.service_account(filename ='key_json.json')
sh1 = gc.open("ОЗОН АКСЕССОРИКА ИА БИЖУ API Python").worksheet("Товары и цены")
range_to_clear = sh1.range('A:G')
for cell in range_to_clear:
    cell.value = '' 
sh1.update_cells(range_to_clear)
sh1.update([tovari_all.columns.values.tolist()]+tovari_all.values.tolist())

sh2 = gc.open("ОЗОН АКСЕССОРИКА ИА БИЖУ API Python").worksheet("Метрики")
range_to_clear = sh2.range('A:E')
for cell in range_to_clear:
    cell.value = '' 
sh2.update_cells(range_to_clear)
sh2.update([metrics.columns.values.tolist()]+metrics.values.tolist())

sh3 = gc.open("ОЗОН АКСЕССОРИКА ИА БИЖУ API Python").worksheet("Остатки")
range_to_clear = sh3.range('A:E')
for cell in range_to_clear:
    cell.value = '' 
sh3.update_cells(range_to_clear)
sh3.update([ost_sait.columns.values.tolist()]+ost_sait.values.tolist())
# result1.reset_index()
# sh3 = gc.open("ОЗОН РАЗВАЛОВА О.Ю. API Python").worksheet("По дням")
# sh3.update([result1.columns.values.tolist()]+result1.values.tolist())


# In[ ]:





# In[ ]:





# In[ ]:




