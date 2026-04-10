from flask import Flask, render_template, jsonify
import requests
import csv
import io
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

app = Flask(__name__)

URL_VENTAS = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1351036806&single=true&output=csv'
URL_STOCK  = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1599060189&single=true&output=csv'

# Cache en memoria
_cache = {'ventas': [], 'stock': [], 'ultima_actualizacion': None}

def fetch_csv(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(r.text))
        return list(reader)
    except Exception as e:
        print(f'Error fetching CSV: {e}')
        return []

def actualizar_cache():
    print('Actualizando caché desde Google Sheets...')
    ventas = fetch_csv(URL_VENTAS)
    stock  = fetch_csv(URL_STOCK)
    if ventas or stock:
        _cache['ventas'] = ventas
        _cache['stock']  = stock
        from datetime import datetime
        tz_lima = pytz.timezone('America/Lima')
        _cache['ultima_actualizacion'] = datetime.now(tz_lima).strftime('%d/%m/%Y %H:%M')
        print(f'Caché actualizado: {_cache["ultima_actualizacion"]}')
    else:
        print('Error: no se pudo actualizar el caché')

# Scheduler: 8am, 1pm, 7pm hora Lima
tz_lima = pytz.timezone('America/Lima')
scheduler = BackgroundScheduler(timezone=tz_lima)
scheduler.add_job(actualizar_cache, CronTrigger(hour=8,  minute=0, timezone=tz_lima))
scheduler.add_job(actualizar_cache, CronTrigger(hour=13, minute=0, timezone=tz_lima))
scheduler.add_job(actualizar_cache, CronTrigger(hour=19, minute=0, timezone=tz_lima))
scheduler.start()

# Carga inicial al arrancar
actualizar_cache()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    return jsonify({
        'ventas': _cache['ventas'],
        'stock':  _cache['stock'],
        'ultima_actualizacion': _cache['ultima_actualizacion']
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
