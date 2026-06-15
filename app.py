from flask import Flask, render_template, jsonify, request
import requests
import csv
import io
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import pytz

app = Flask(__name__)

URL_VENTAS     = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1351036806&single=true&output=csv'
URL_STOCK      = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1599060189&single=true&output=csv'
URL_MINUTAS_PF = 'https://docs.google.com/spreadsheets/d/15slphQ1xs7pkG4zH5mVwsnc1XKgGtLKah8-FHm7HMEM/export?format=csv&gid=1268984404'
URL_METAS      = 'https://docs.google.com/spreadsheets/d/15slphQ1xs7pkG4zH5mVwsnc1XKgGtLKah8-FHm7HMEM/export?format=csv&gid=1346196384'

# Cache en memoria
_cache = {'ventas': [], 'stock': [], 'minutas_pf': [], 'metas': [], 'ultima_actualizacion': None}

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
    ventas     = fetch_csv(URL_VENTAS)
    stock      = fetch_csv(URL_STOCK)
    minutas_pf = fetch_csv(URL_MINUTAS_PF)
    metas      = fetch_csv(URL_METAS)
    if ventas or stock:
        _cache['ventas']     = ventas
        _cache['stock']      = stock
        _cache['minutas_pf'] = minutas_pf
        _cache['metas']      = metas
        from datetime import datetime
        tz_lima = pytz.timezone('America/Lima')
        _cache['ultima_actualizacion'] = datetime.now(tz_lima).strftime('%d/%m/%Y %H:%M')
        print(f'Caché actualizado: {_cache["ultima_actualizacion"]}')
    else:
        print('Error: no se pudo actualizar el caché')

# Scheduler: cada 1 hora
tz_lima = pytz.timezone('America/Lima')
scheduler = BackgroundScheduler(timezone=tz_lima)
scheduler.add_job(actualizar_cache, IntervalTrigger(hours=1))
scheduler.start()

# Carga inicial al arrancar
actualizar_cache()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    return jsonify({
        'ventas':     _cache['ventas'],
        'stock':      _cache['stock'],
        'minutas_pf': _cache['minutas_pf'],
        'metas':      _cache['metas'],
        'ultima_actualizacion': _cache['ultima_actualizacion']
    })

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    actualizar_cache()
    return jsonify({
        'ok': True,
        'ultima_actualizacion': _cache['ultima_actualizacion']
    })

@app.route('/api/ai', methods=['POST'])
def api_ai():
    try:
        import anthropic
        data = request.get_json()
        question = data.get('question', '')
        sales_data = data.get('data', [])
        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            return jsonify({'error': 'ANTHROPIC_API_KEY no configurada. Agrégala como variable de entorno en Railway.'})
        client = anthropic.Anthropic(api_key=api_key)
        proyectos = list({p for yr in sales_data for p in yr.get('proyectos', {})})
        context = (
            f"Eres un asistente de análisis de ventas inmobiliarias para Padova SAC. "
            f"Proyectos: {', '.join(proyectos)}.\n"
            f"Datos de ventas:\n{json.dumps(sales_data, ensure_ascii=False, indent=2)}\n"
            f"Responde en español, de forma concisa. Menciona números concretos cuando aplique."
        )
        msg = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=1024,
            messages=[{'role': 'user', 'content': context + '\n\nPregunta: ' + question}]
        )
        return jsonify({'response': msg.content[0].text})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
