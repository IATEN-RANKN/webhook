# process_numbers.py

import re
from typing import Tuple

# Compilar padrões regex no nível do módulo
VALID_PHONE_PATTERN = re.compile(r'^55\d{2}9\d{8}$')
DUPLICATE_NINE_PATTERN = re.compile(r'^55\d{2}99\d{8}$')

def validate_phone(phone: str) -> bool:
    """
    Valida se o número de telefone corresponde ao padrão de celular brasileiro:
    '55' + código de área de 2 dígitos + '9' + número de 8 dígitos.
    """
    return bool(VALID_PHONE_PATTERN.fullmatch(phone))

def has_duplicate_nine(phone: str) -> bool:
    """
    Verifica se o número de telefone tem '99' após o código do país e código de área,
    indicando um '9' extra que precisa ser removido.
    """
    return bool(DUPLICATE_NINE_PATTERN.fullmatch(phone))

def verificar_digitos_iguais(numero: str) -> bool:
    """
    Verifica se todos os dígitos no número de telefone são iguais,
    o que indicaria um número inválido.
    """
    digits = ''.join(filter(str.isdigit, numero))
    # Remove o código do país '55' se presente
    if digits.startswith('55'):
        digits = digits[2:]
    # Verifica se todos os dígitos são iguais
    return digits and digits == digits[0] * len(digits)

def format_phone(phone: str) -> str:
    """
    Formata o número de telefone para o padrão brasileiro:
    - Remove caracteres não numéricos
    - Adiciona o código do país '55' se estiver ausente
    - Remove '9' extra após o código de área, se presente
    - Garante que haja um '9' após o código de área
    """
    digits = ''.join(filter(str.isdigit, phone))
    if not digits.startswith('55'):
        digits = '55' + digits

    # Remove '9' duplicado se houver dois '9's após o código de área
    if has_duplicate_nine(digits):
        # Remove o segundo '9' (na posição 4)
        digits = digits[:4] + digits[5:]

    # Garante que haja um '9' após o código de área
    if not validate_phone(digits):
        # Insere '9' após o código de área (na posição 4)
        if len(digits) >= 4:
            digits = digits[:4] + '9' + digits[4:]
        else:
            # Se não houver dígitos suficientes, adiciona '9' no final
            digits += '9'

    return digits

def score_phone(phone: str) -> Tuple[int, str]:
    """
    Atribui uma pontuação ao número de telefone com base em sua validade.
    Pontuações mais altas indicam maior chance de ser um número de celular válido.
    """
    if not phone:
        return 0, ''

    # Remove caracteres não numéricos e formata
    formatted_phone = format_phone(phone)
    score = 0

    # Verifica se é válido após a formatação
    if validate_phone(formatted_phone):
        score += 2
    else:
        score += 1  # Número formatado, mas ainda não válido

    # Verifica se não possui todos os dígitos iguais
    if not verificar_digitos_iguais(formatted_phone):
        score += 1

    # Critérios adicionais podem ser adicionados aqui

    return score, formatted_phone
