from flask import Flask, render_template, jsonify
import requests
import csv
import io
import os # Importante para leer el puerto de Railway

app = Flask(__name__)

URL_VENTAS = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1351036806&single=true&output=csv'
URL_STOCK  = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR3Vnd6iNuxVhaxVIvvoD9AW4s_sgzqXillGeWiqL8CV0ha9L8WdX1D7KEBbcHYDTF7T9PCmOTCoC68/pub?gid=1599060189&single=true&output=csv'

def fetch_csv(url):
    try:
        # Añadimos un user-agent para que Google no bloquee la petición
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(r.text))
        return list(reader)
    except Exception as e:
        print(f'Error: {e}')
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    ventas = fetch_csv(URL_VENTAS)
    stock  = fetch_csv(URL_STOCK)
    return jsonify({'ventas': ventas, 'stock': stock})

if __name__ == '__main__':
    # Esto permite que Railway asigne el puerto automáticamente
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
