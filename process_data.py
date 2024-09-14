# process_data.py

from datetime import datetime
from typing import Dict, Any, Optional
from collections import OrderedDict
import pytz  # Biblioteca para lidar com fusos horários

from process_numbers import score_phone
from mongo_utils import get_document_from_mongodb
from mongo_save import save_to_mongodb  # Importa a função para salvar no MongoDB
import logging

# Configurar o logging
logging.basicConfig(level=logging.INFO)

def process_webhook_data(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Processa os dados recebidos pelo webhook e retorna um dicionário com a estrutura especificada.
    Se ocorrer um erro ao salvar no MongoDB, retorna None.
    """
    # Extrair 'primary_data'
    primary_data = data.get('primary_data', {})

    # Extrair dados pessoais
    full_name = primary_data.get('customer_name', '').strip()
    full_name_parts = full_name.split()
    first_name = full_name_parts[0] if full_name_parts else ''
    last_name = ' '.join(full_name_parts[1:]) if len(full_name_parts) > 1 else ''

    # Inicializar dados de saída com valores do webhook
    output = OrderedDict()
    output['full_name'] = full_name
    output['first_name'] = first_name
    output['last_name'] = last_name
    output['phone_number'] = None  # Será definido posteriormente
    output['number_invalid'] = True  # Será definido posteriormente
    # 'created_at' e 'updated_at' serão definidos posteriormente
    output['id_in_platforms'] = []
    output['tags'] = []
    output['conversation_history'] = []

    # Extrair e processar números de telefone
    phone_fields = ['mobile_1', 'mobile_2', 'mobile_3', 'mobile_4']
    phone_candidates = []

    for field in phone_fields:
        phone_number = primary_data.get(field)
        if phone_number:
            try:
                score, formatted_phone = score_phone(phone_number)
                phone_candidates.append((score, formatted_phone))
            except Exception as e:
                logging.error(f"Erro ao processar o número {phone_number}: {e}")

    if phone_candidates:
        # Selecionar o número de telefone com a maior pontuação
        best_score, best_number = max(phone_candidates, key=lambda x: x[0])
        output['phone_number'] = best_number
        output['number_invalid'] = False
    else:
        logging.warning("Nenhum número de telefone válido encontrado no JSON.")
        best_number = None
        output['phone_number'] = None
        output['number_invalid'] = True

    # Definir 'id' como 'best_number'
    output['id'] = best_number if best_number else 'N/A'

    # Verifica se o best_number existe no MongoDB
    existing_document = get_document_from_mongodb(best_number) if best_number else None

    # Data e hora atual no fuso horário do Brasil
    br_timezone = pytz.timezone('America/Sao_Paulo')
    current_time = datetime.now(br_timezone).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Definir 'created_at' e 'updated_at'
    output['updated_at'] = current_time
    if existing_document and 'created_at' in existing_document:
        output['created_at'] = existing_document['created_at']
    else:
        output['created_at'] = current_time

    if existing_document:
        # Usar dados do documento existente onde disponível
        output['full_name'] = existing_document.get('full_name', output['full_name'])
        output['first_name'] = existing_document.get('first_name', output['first_name'])
        output['last_name'] = existing_document.get('last_name', output['last_name'])
        output['phone_number'] = existing_document.get('phone_number', output['phone_number'])
        output['number_invalid'] = existing_document.get('number_invalid', output['number_invalid'])
        existing_id_in_platforms = existing_document.get('id_in_platforms', [])
    else:
        existing_id_in_platforms = []

    # Processar 'id_in_platforms'
    # Plataformas existentes
    existing_platforms_set = set(
        (platform.get('platform', ''), platform.get('id_in_platform', '')) for platform in existing_id_in_platforms
    )
    # Novas plataformas do webhook
    platforms_to_add = data.get('platforms_to_add', [])
    new_id_in_platforms = [
        OrderedDict([
            ("platform", platform.get('platform', '')),
            ("id_in_platform", platform.get('platform_id', ''))
        ])
        for platform in platforms_to_add
    ]

    # Combinar plataformas, evitando duplicatas
    combined_id_in_platforms = existing_id_in_platforms.copy()
    for platform in new_id_in_platforms:
        platform_tuple = (platform['platform'], platform['id_in_platform'])
        if platform_tuple not in existing_platforms_set:
            combined_id_in_platforms.append(platform)
            existing_platforms_set.add(platform_tuple)

    output['id_in_platforms'] = combined_id_in_platforms

    # Processar 'tags'
    # Tags existentes
    existing_tags = existing_document.get('tags', []) if existing_document else []
    # Novas tags do webhook
    new_tags = data.get('tags_to_add', [])
    processed_new_tags = []
    for tag in new_tags:
        if 'tag_added_at' in tag:
            tag['tag_add_at'] = tag.pop('tag_added_at')
        processed_new_tags.append(OrderedDict([
            ("tag_name", tag.get('tag_name', '')),
            ("tag_add_at", tag.get('tag_add_at', ''))
        ]))

    # Combinar tags (duplicatas são permitidas)
    output['tags'] = existing_tags + processed_new_tags

    # Processar 'conversation_history'
    # Conversas existentes
    existing_conversations = existing_document.get('conversation_history', []) if existing_document else []
    # Construir um dicionário de conversas existentes com base em 'message_id'
    conversation_dict = {
        conv['message_id']: conv for conv in existing_conversations if 'message_id' in conv
    }

    # Novas conversas do webhook
    new_conversations = data.get('conversations_to_add', [])
    for conv in new_conversations:
        message_id = conv.get('message_id')
        if message_id and message_id not in conversation_dict:
            conv_ordered = OrderedDict([
                ("message_id", conv.get('message_id', '')),
                ("phone_sender", conv.get('phone_sender', '')),
                ("phone_receiver", conv.get('phone_receiver', '')),
                ("message_user", conv.get('message_user', '')),
                ("message_date", conv.get('message_date', '')),
                ("message_content", conv.get('message_content', ''))
            ])
            conversation_dict[message_id] = conv_ordered

    # Lista final de conversas sem duplicatas
    output['conversation_history'] = list(conversation_dict.values())

    # Verificar se 'best_number' é válido antes de salvar
    if output['id'] != 'N/A':
        # Chamar a função para salvar no MongoDB
        success = save_to_mongodb(output)
        if not success:
            logging.error("Falha ao salvar os dados no MongoDB.")
            return None  # Indica falha para o webhook_listener
    else:
        logging.error("Não foi possível determinar 'best_number'; não salvando no MongoDB.")
        return None  # Indica falha para o webhook_listener

    return output
