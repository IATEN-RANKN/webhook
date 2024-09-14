# mongo_save.py

import os
import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, PyMongoError
from typing import Dict, Any
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configura o logging para este módulo
logging.basicConfig(level=logging.INFO)

# Detalhes de conexão do MongoDB obtidos das variáveis de ambiente
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def save_to_mongodb(output: Dict[str, Any]) -> bool:
    """
    Atualiza ou cria um documento no MongoDB com base no 'output' fornecido.
    Retorna True se a operação for bem-sucedida, False caso contrário.
    """
    if not all([MONGO_URI, DATABASE_NAME, COLLECTION_NAME]):
        logging.error("As configurações do MongoDB não foram definidas nas variáveis de ambiente.")
        return False

    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Identifica o documento pelo campo 'id' (que é igual a 'best_number')
        filter_query = {'id': output.get('id')}

        # Atualiza o documento ou insere se não existir
        update_doc = {'$set': output}
        result = collection.update_one(filter_query, update_doc, upsert=True)

        if result.matched_count > 0:
            logging.info(f"Documento com id {output.get('id')} atualizado com sucesso.")
        elif result.upserted_id:
            logging.info(f"Novo documento com id {output.get('id')} inserido com sucesso.")
        else:
            logging.warning("Nenhuma operação foi realizada no MongoDB.")

        return True
    except (ConnectionFailure, PyMongoError) as e:
        logging.error(f"Erro ao conectar ou operar no MongoDB: {e}")
        return False
    finally:
        client.close()
