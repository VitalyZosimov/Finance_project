from hooks.mongo_hook import MongoHook

companies = [
    {'ticker': 'AAPL', 'name': 'Apple Inc', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'MSFT', 'name': 'Microsoft Corp', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'GOOGL', 'name': 'Alphabet Inc', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'AMZN', 'name': 'Amazon Inc', 'country': 'USA', 'sector': 'Consumer'},
    {'ticker': 'TSLA', 'name': 'Tesla Inc', 'country': 'USA', 'sector': 'Automotive'},
    {'ticker': 'META', 'name': 'Meta Platforms', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'NFLX', 'name': 'Netflix Inc', 'country': 'USA', 'sector': 'Entertainment'},
    {'ticker': 'NVDA', 'name': 'Nvidia Corp', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'AMD', 'name': 'AMD Corp', 'country': 'USA', 'sector': 'Technology'},
    {'ticker': 'IBM', 'name': 'IBM Corp', 'country': 'USA', 'sector': 'Technology'},
]

print('Подключение к MongoDB...')
hook = MongoHook()
db = hook.get_db('stockviz')

print('Удаление старой коллекции...')
db.companies_universe.drop()

print('Вставка данных...')
result = db.companies_universe.insert_many(companies)
print(f'Вставлено {len(result.inserted_ids)} компаний')

print(f'Всего в коллекции: {db.companies_universe.count_documents({})}')