import websocket
import json
import numpy as np
import pandas as pd
# import credentials as cd
# import binance_functions
import time
import hashlib
import hmac
from urllib.parse import urlencode
import requests
import math
from binance.client import Client, BinanceAPIException
from binance.enums import *

BINANCE_SECRET_KEY = 'S5gKdLlC1JlA73rzxyHvcocYib9DYdVPvVH0kriQ3kc1B2FWv5LEVMPtVybMHZ7N'
BINANCE_API_KEY = 'mjixn46yVrODJomGMyk0IrzwlXXSxDYs61SpgyMQyeiOrsxKxCfgMp7TKGj89ZIe'



def get_timestamp_signature(params):
    timestamp = int(time.time() * 1000)  # Timestamp em milissegundos
    params['timestamp'] = timestamp
    
    query_string = urlencode(params)
    secret = BINANCE_SECRET_KEY
    signature = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    
    return timestamp, signature

def get_balance(asset):
    url = 'https://api.binance.com/api/v3/account'
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    
    params = {
        "recvWindow": 100  # Janela de tempo para aceitar a requisição
    }
    
    timestamp, signature = get_timestamp_signature(params)
    params['signature'] = signature
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        try:
            asset_info = response.json()
            for item in asset_info['balances']:
                if item['asset'] == asset:
                    return float(item['free'])
            return 0.0
        except ValueError:
            raise Exception("Erro ao converter a resposta da API para JSON.")
    else:
        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")

# Exemplo de uso:
# btc_balance = get_balance('BRL')
# print(f"Saldo BTC: {btc_balance}")


def get_symbol_info(symbol):
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)
    
    if response.status_code == 200:
        try:
            data = response.json()
            for s in data['symbols']:
                if s['symbol'] == symbol:
                    return s
            raise Exception("Símbolo não encontrado.")
        except ValueError:
            raise Exception("Erro ao converter a resposta da API para JSON.")
    else:
        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")

def round_to_step_size(quantity, step_size):
    return math.floor(quantity / step_size) * step_size

def sell_asset(symbol, quantity):
    # Obtenha o passo do símbolo
    symbol_info = get_symbol_info(symbol)
    filters = symbol_info['filters']
    lot_size_filter = next(f for f in filters if f['filterType'] == 'LOT_SIZE')
    step_size = float(lot_size_filter['stepSize'])
    
    # Ajuste a quantidade
    quantity = round_to_step_size(quantity, step_size)
    
    url = 'https://api.binance.com/api/v3/order'
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    
    params = {
        'symbol': symbol,
        'side': 'SELL',
        'type': 'MARKET',
        'quantity': quantity,
        'recvWindow': 1000
    }
    
    timestamp, signature = get_timestamp_signature(params)
    params['signature'] = signature
    
    response = requests.post(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")

# # Exemplo de uso
symbol_info = get_symbol_info('BNBBRL')
filters = symbol_info['filters']
print(symbol_info)

# Obtendo o stepSize do filtro correto
step_size = next(f['stepSize'] for f in filters if f['filterType'] == 'LOT_SIZE')
print(step_size)
MIN_OP = float(next(f['minNotional'] for f in filters if f['filterType'] == 'NOTIONAL'))
print(MIN_OP)

    
# balance_brl = get_balance('USDT')
# print(balance_brl)


# if btc_balance > 0:
#     result = sell_asset('BTCBRL', btc_balance)
#     print("Venda realizada com sucesso:", result)
# else:

#     print("Nenhum saldo BTC disponível para venda.")


# import socket


# # Obter o IP interno (local)
# hostname = socket.gethostname()
# ip_interno = socket.gethostbyname(hostname)

# print(f"IP Interno: {ip_interno}")

# import requests

# # Obter o IP externo (público)
# response = requests.get('https://api.ipify.org?format=json')
# ip_externo = response.json()['ip']

# print(f"IP Externo: {ip_externo}")


def get_server_time():
    """Obter o timestamp do servidor da Binance."""
    response = requests.get("https://api3.binance.com/api/v3/time")
    if response.status_code == 200:
        server_time = response.json()['serverTime']
        return server_time
    else:
        raise Exception(f"Erro ao obter o tempo do servidor: {response.status_code} - {response.text}")

def is_time_synchronized():
    """Verifica se o tempo local está sincronizado com o tempo do servidor da Binance."""

    server_time = get_server_time()  # Tempo do servidor Binance em milissegundos
    local_time = int(time.time() * 1000)  # Tempo local em milissegundos
    print('server_time: ', server_time)
    print('local_time: ', local_time)

    # Diferença permitida: recvWindow padrão é de 5000ms (5 segundos)
    recv_window = 2000  
    time_diff = server_time - local_time  # Diferença absoluta entre o tempo do servidor e o local
    print('time_diff: ', time_diff)

    if time_diff > recv_window:
        print(f"Erro: O tempo local está fora da janela de recebimento ({recv_window} ms). Diferença atual: {time_diff} ms.")
        return False
    else:
        print("Tempo sincronizado corretamente!")
        return True

# Testa se o tempo está sincronizado
is_time_synchronized()