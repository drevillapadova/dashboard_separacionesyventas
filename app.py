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
        sales_data = data.get('data', {})
        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            return jsonify({'error': 'ANTHROPIC_API_KEY no configurada. Agrégala como variable de entorno en Railway.'})
        client = anthropic.Anthropic(api_key=api_key)
        resumen_anual = sales_data.get('resumen_anual', []) if isinstance(sales_data, dict) else sales_data
        ventas_por_mes = sales_data.get('ventas_por_mes', []) if isinstance(sales_data, dict) else []
        top_asesores = sales_data.get('top_asesores', []) if isinstance(sales_data, dict) else []
        separaciones_activas = sales_data.get('separaciones_activas', 0) if isinstance(sales_data, dict) else 0
        proyectos = list({p for yr in resumen_anual for p in yr.get('proyectos', {})})
        system = (
            "Eres un asistente de análisis de ventas inmobiliarias para Padova SAC. "
            "Respondes SIEMPRE en JSON con este formato:\n"
            '{"type":"text","content":"..."} para respuestas de texto\n'
            '{"type":"chart","title":"...","chart":{...config Chart.js...}} para gráficos\n\n'
            "Para gráficos usa Chart.js v4: incluye type (bar/line/pie/doughnut), "
            "data (labels + datasets con backgroundColor, borderColor, data), "
            "y options básicas (responsive:true, maintainAspectRatio:false). "
            "Colores sugeridos por proyecto: Helio=#3b82f6, Litoral=#10b981, "
            "Carabayllo 4=#ef4444, Carabayllo 5=#f97316, Sunny=#f59e0b, D.Orue=#8b5cf6. "
            "NO incluyas texto fuera del JSON. Responde en español."
        )
        context = (
            f"Proyectos: {', '.join(proyectos)}.\n"
            f"Resumen anual:\n{json.dumps(resumen_anual, ensure_ascii=False, indent=2)}\n\n"
            f"Ventas por mes:\n{json.dumps(ventas_por_mes, ensure_ascii=False, indent=2)}\n\n"
            f"Top asesores:\n{json.dumps(top_asesores, ensure_ascii=False, indent=2)}\n\n"
            f"Separaciones activas: {separaciones_activas}"
        )
        msg = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=2048,
            system=system,
            messages=[{'role': 'user', 'content': context + '\n\nPregunta: ' + question}]
        )
        raw = msg.content[0].text.strip()
        # Intentar parsear como JSON; si falla devolver como texto
        try:
            parsed = json.loads(raw)
            return jsonify(parsed)
        except Exception:
            return jsonify({'type': 'text', 'content': raw})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
