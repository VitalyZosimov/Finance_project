from hooks.mongo_hook import MongoHook
import requests
from datetime import datetime, timedelta

hook = MongoHook()
db = hook.get_db('stockviz')

currency_ids = {'USD':431, 'EUR':451, 'RUB':298, 'CNY':462, 'JPY':290, 'GBP':143, 'PLN':293, 'UAH':285}
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

all_rates = []

for abbr, cur_id in currency_ids.items():
    url = f'https://api.nbrb.by/exrates/rates/dynamics/{cur_id}'
    params = {'startdate': start_date.strftime('%Y-%m-%d'), 'enddate': end_date.strftime('%Y-%m-%d')}
    try:
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        print(f'{abbr}: loaded {len(data)} records')
        for item in data:
            rate = item['Cur_OfficialRate'] / item['Cur_Scale']
            all_rates.append({
                'date': item['Date'][:10],
                'currency': abbr,
                'rate_to_byn': round(rate, 4),
                'scale': item['Cur_Scale']
            })
    except Exception as e:
        print(f'ERROR {abbr}: {e}')

for d in range(31):
    date = (start_date + timedelta(days=d)).strftime('%Y-%m-%d')
    all_rates.append({'date': date, 'currency': 'BYN', 'rate_to_byn': 1.0, 'scale': 1})

db.currency_rates.drop()
db.currency_rates.insert_many(all_rates)
print(f'OK: {len(all_rates)} records saved')