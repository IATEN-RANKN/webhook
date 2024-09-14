# mongo_utils.py

import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Exemplo de validação
REQUIRED_ENV_VARS = ["MONGO_URI", "DATABASE_NAME", "COLLECTION_NAME"]
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        raise EnvironmentError(f"A variável de ambiente {var} não está definida.")

# Configura o logging para este módulo
logging.basicConfig(level=logging.INFO)

# Detalhes de conexão do MongoDB obtidos das variáveis de ambiente
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def get_document_from_mongodb(best_number: str) -> Optional[Dict[str, Any]]:
    """
    Verifica se o best_number existe no MongoDB.
    Retorna o documento completo se encontrado, caso contrário, retorna None.
    """
    if not all([MONGO_URI, DATABASE_NAME, COLLECTION_NAME]):
        logging.error("As configurações do MongoDB não foram definidas nas variáveis de ambiente.")
        return None

    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Procura um documento onde o 'id' é igual ao best_number
        result = collection.find_one({"id": best_number})
        if result:
            # Converte o '_id' para string
            result['_id'] = str(result['_id'])
            return result  # Retorna o documento completo
        else:
            return None
    except ConnectionFailure as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None
    finally:
        client.close()
