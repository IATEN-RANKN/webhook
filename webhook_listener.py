# webhook_listener.py

import os
from flask import Flask, request, abort, Response
from process_data import process_webhook_data
import json
import logging
from dotenv import load_dotenv

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Carrega as variáveis do arquivo .env
load_dotenv()

# Exemplo de validação
REQUIRED_ENV_VARS = ["MONGO_URI", "DATABASE_NAME", "COLLECTION_NAME"]
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        raise EnvironmentError(f"A variável de ambiente {var} não está definida.")

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = request.json  # Obtém o payload JSON
        # Chama a função no outro arquivo, passando o JSON
        output = process_webhook_data(data)
        if output:
            # Serializa o output usando json.dumps, garantindo a ordem das chaves
            response_json = json.dumps(output, ensure_ascii=False)
            return Response(response_json, mimetype='application/json'), 200
        else:
            error_message = {"error": "Erro ao processar os dados ou salvar no MongoDB."}
            logging.error(error_message["error"])
            error_response = json.dumps(error_message, ensure_ascii=False)
            return Response(error_response, mimetype='application/json'), 500
    else:
        abort(400)

if __name__ == '__main__':
    app.run(port=5000)
