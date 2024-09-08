import websocket
import json
import numpy as np
import pandas as pd
import credentials as cd
import binance_functions
import time
import hashlib
import hmac
from urllib.parse import urlencode
import requests
import math

from binance.client import Client, BinanceAPIException
from binance.enums import *

########### RULES ########### 

# is_candle_closed == True
# SIMULATE_FLG == False
# baseline
# compra ou venda baseada no lucro (atualmente compra ou vende todo baseline)

########### RULES ########### 


BINANCE_API_KEY = cd.API_KEY
BINANCE_SECRET_KEY = cd.SECRET_KEY

price_list = []
df_prices = pd.DataFrame(columns =['close', 'var', 'positivo'
                                       , 'negativo', 'media_positivos', 'media_negativo' 
                                       ,'position','quantity','symbol'
                                       ,'coin_balance','coin_wltcoin_balance'
                                       ,'profit_value' ,'in_position_after'
                                       ,'hold','venda','compra'
                                       ,'wltcoin_balance_before','wltcoin_balance_after'
                                    ])
SIMULATE_FLG = False

def get_symbol_info(symbol):
    url = 'https://api3.binance.com/api/v3/exchangeInfo'
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

def get_decimal_places(step_size):
    """Calculate the number of decimal places based on the step size."""
    return max(0, len(str(step_size).split('.')[1]))

def round_to_step_size(quantity, step_size):
    # Certifique-se de que quantity e step_size são floats
    quantity = float(quantity)
    step_size = float(step_size)
    return math.floor(quantity / step_size) * step_size

def get_balance(asset):
    url = 'https://api.binance.com/api/v3/account'
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    
    params = {
        "recvWindow": 20000  # Janela de tempo para aceitar a requisição
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
        raise Exception(f"Erro na requisição GET_BALANCE: {response.status_code} - {response.text}")


def get_timestamp_signature(params):
    timestamp = int(time.time() * 1000)  # Timestamp em milissegundos
    
    # Adiciona o timestamp aos parâmetros
    params['timestamp'] = timestamp
    
    query_string = urlencode(params)
    secret = BINANCE_SECRET_KEY
    signature = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    
    return timestamp, signature

def place_order(symbol, side, quantity, price=None, order_type="MARKET"):
    url = 'https://api3.binance.com/api/v3/order'
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    
    # Formata a quantidade para garantir que está no formato correto
    quantity = f"{quantity:.{QTY_PRECISION}f}".rstrip('0').rstrip('.')
    print('quantity before: ', quantity)

    quantity = round_to_step_size(quantity, STEP_SIZE)
    quantity = f"{float(quantity):.{QTY_PRECISION}f}"
    print('quantity after: ', quantity)


    # Parâmetros da ordem
    params = {
        "symbol": symbol,
        "side": side,  # BUY ou SELL
        "type": order_type,  # MARKET para ordem a mercado
        # "quantity": f"{quantity:.8f}".rstrip('0').rstrip('.'),
        "quantity": quantity,
        "recvWindow": 10000
    }
    
    if order_type == "LIMIT" and price is not None:
        params["price"] = f"{price:.{QTY_PRECISION}f}".rstrip('0').rstrip('.')
    
    # Obtenção do timestamp e assinatura
    timestamp, signature = get_timestamp_signature(params)
    
    # Adiciona a assinatura aos parâmetros
    params['signature'] = signature
    
    # Executa a requisição POST para colocar a ordem
    response = requests.post(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro na requisição: {response.text}")


def order(side, quantity, symbol,order_type=ORDER_TYPE_MARKET):
    if not SIMULATE_FLG:
        try:
            print("sending order SIDE: ",side, ', QUANTITY: ',quantity, ', SYMBOL: ',symbol)
            # order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
            order = place_order(symbol=symbol, side=side, quantity=quantity)
            print('ORDER: ',order)
            
        except Exception as e:
            print("an exception occured - {}".format(e))
            return False
        
    return True



## PARA TESTAR A LÓGICA SEM CHAMAR WEBSOCKET
def random_test(df_prices, closes, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, in_position):
    
    close_lst = np.random.randint(240000, 250000, size=(1000))
    
    for close in close_lst:
        
        in_position = process_new_price(close, df_prices, close_lst, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, in_position)


def calculate_rsi(close_prices, rsi_period):
    """
    Calcula o Índice de Força Relativa (RSI) com base nos preços de fechamento e no período especificado.
    """
    # Calcula as variações dos preços de fechamento
    price_diff = close_prices.diff()

    # Separa as variações em positivas e negativas
    gain = price_diff.where(price_diff > 0, 0)
    loss = -price_diff.where(price_diff < 0, 0)

    # Calcula as médias móveis dos ganhos e das perdas
    avg_gain = gain.rolling(window=rsi_period).mean()
    avg_loss = loss.rolling(window=rsi_period).mean()

    # Calcula o RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]  # Retorna apenas o último valor do RSI

def process_new_price(close, df_prices, price_list, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, in_position):
    """
    Processa um novo preço recebido, atualiza o DataFrame de preços e verifica sinais de negociação com base no RSI.
    """
    try:
        if not SIMULATE_FLG:
            price_list.append(float(close))

        # Adiciona o preço de fechamento ao DataFrame
        df_prices.loc[len(df_prices)] = {'close': float(close), 'RSI': None, 'compra': False, 'venda': False}

        if len(price_list) >= RSI_PERIOD:
            # Calcula o RSI para a última linha
            rsi = calculate_rsi(df_prices['close'], RSI_PERIOD)
            print('RSI: ', rsi)
            # Atualiza o DataFrame com o valor de RSI calculado
            df_prices.at[len(df_prices) - 1, 'RSI'] = rsi

            # Calcula outras métricas
            df_prices['var'] = df_prices['close'].pct_change()
            df_prices['positivo'] = df_prices['var'].apply(lambda x: x if x > 0 else 0)
            df_prices['negativo'] = df_prices['var'].apply(lambda x: abs(x) if x < 0 else 0)
            df_prices['media_positivos'] = df_prices['positivo'].rolling(window=RSI_PERIOD).mean()
            df_prices['media_negativo'] = df_prices['negativo'].rolling(window=RSI_PERIOD).mean()
            
            # get BTC free balance
            coin_balance = get_balance(COIN_CODE)
            # print('coin_balance: ', coin_balance)

            # # convert BTC balance to WALLET_COIN
            coin_wltcoin_balance = float(close) * coin_balance
            # print('coin_wltcoin_balance: ', coin_wltcoin_balance)

            # # get profit compared to baseline
            profit_value = coin_wltcoin_balance - BASELINE
            # print('profit_value: ', profit_value)

            # Get WALLET_COIN balance before order
            wltcoin_balance_before = get_balance(WLT_COIN)

            profit_value_perc = profit_value/BASELINE
            
            print('profit_value: ', profit_value, ' || ', 'profit_valu_perc: ', profit_value_perc*100, '%', ' || ', 'wltcoin_balance_before: ', wltcoin_balance_before)
            
            # convert profit in WALLET_COIN to BTC
            # TRADE_QUANTITY = BASELINE / float(close)
            # TRADE_QUANTITY = coin_balance #profit_value / float(close)
            TRADE_QUANTITY = (MIN_OP + profit_value) / float(close)
            
            # Verifica os sinais de negociação com base no RSI e na variável in_position
            if (profit_value/BASELINE) > MIN_PROFIT and in_position:

                print(f'RSI: {rsi} > RSI_OVERBOUGHT: {RSI_OVERBOUGHT}. Vendendo...')

                if TRADE_QUANTITY <= coin_balance:    
                    df_prices['in_position_before'] = in_position
                    # create sell order
                    order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)                    
                    if order_succeeded:
                        print('VENDEU!')
                        in_position = False
                        

                    # Get WALLET_COIN balance after order
                    wltcoin_balance_after = get_balance(WLT_COIN)
                    print('lucro calculado: ', profit_value)
                    print('SALDO R$: ', wltcoin_balance_after)
                    
                    # print(f'RSI: {rsi} > RSI_OVERBOUGHT: {RSI_OVERBOUGHT}. Vendendo...')
                    df_prices.at[len(df_prices) - 1, 'hold'] = False
                    df_prices.at[len(df_prices) - 1, 'venda'] = True
                    df_prices['position'] = 'SELL'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['wltcoin_balance_before'] = wltcoin_balance_before
                    df_prices['wltcoin_balance_after'] = wltcoin_balance_after
                    df_prices['coin_balance'] = coin_balance
                    df_prices['coin_wltcoin_balance'] = coin_wltcoin_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
                else:

                    # print('In position to sell, but there''s no profit over the BASELINE')
                    df_prices['position'] = 'SIDE_SELL_NO_PROFITS'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['wltcoin_balance_before'] = wltcoin_balance_before
                    df_prices['coin_balance'] = coin_balance
                    df_prices['coin_wltcoin_balance'] = coin_wltcoin_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
                
            elif rsi > RSI_OVERBOUGHT and not in_position:
                # print(f'RSI: {rsi} > RSI_OVERBOUGHT: MAS NÃO ESTAMOS EM POSIÇÃO. HOLD...')
                df_prices.at[len(df_prices) - 1, 'hold'] = True
                df_prices['position'] = 'SIDE_SELL_DONT_OWN_YET'
            
            elif rsi < RSI_OVERSOLD and not in_position:
                # get BTC free balance
                coin_balance = get_balance(COIN_CODE)
                
                # convert BTC balance to WALLET_COIN
                coin_wltcoin_balance = float(close) * coin_balance

                # get profit compared to BASELINE
                profit_value = coin_wltcoin_balance - BASELINE

                # get WALLET_COIN free balance
                wltcoin_balance_before = get_balance(WLT_COIN)
                # print('WALLET_COIN_balance: ', wltcoin_balance_before)

                # convert WALLET_COIN balance to BTC
                # TRADE_QUANTITY = wltcoin_balance_before / float(close)
                # TRADE_QUANTITY = BASELINE / float(close)
                # Define se é a primeira compra naquela moeda, caso sim compra o baseline, senão somente o valor mínimo de operação.
                if coin_balance > 0:
                    TRADE_QUANTITY = MIN_OP / float(close)
                else:
                    TRADE_QUANTITY = BASELINE / float(close)

                if (wltcoin_balance_before / float(close)) > TRADE_QUANTITY: # Verifica se tem mais saldo do que está tentando comprar
                    
                    #df_prices['position'] = 'SIDE_BUY_ALREADY_OWN'
                    # print(f'Compra: {type(TRADE_QUANTITY)} {TRADE_QUANTITY}')
                    # create buy order
                    order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)

                    df_prices['in_position_before'] = in_position

                    if order_succeeded:
                        print('COMPROU')
                        in_position = True

                    # Get WALLET_COIN balance after order
                    wltcoin_balance_after = get_balance(WLT_COIN)
                    print('SALDO R$: ', wltcoin_balance_after)
                    
                    # print(f'RSI: {rsi} < RSI_OVERSOLD: {RSI_OVERSOLD}. Comprando...')
                    df_prices.at[len(df_prices) - 1, 'hold'] = False
                    df_prices.at[len(df_prices) - 1, 'compra'] = True
                    df_prices['position'] = 'BUY'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['wltcoin_balance_before'] = wltcoin_balance_before
                    df_prices['wltcoin_balance_after'] = wltcoin_balance_after
                    df_prices['coin_balance'] = coin_balance
                    df_prices['coin_wltcoin_balance'] = coin_wltcoin_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position

                else:
                    
                    # print(f'RSI: {rsi} < RSI_OVERSOLD: MAS NÃO TEMOS SALDO SUFICIENTE. HOLD...')
                    df_prices['position'] = 'SIDE_BUY_NO_BALANCE'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['wltcoin_balance_before'] = wltcoin_balance_before
                    df_prices['coin_balance'] = coin_balance
                    df_prices['coin_wltcoin_balance'] = coin_wltcoin_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
            
            elif rsi < RSI_OVERSOLD and in_position:
                # print(f'RSI: {rsi} < RSI_OVERSOLD: MAS JÁ ESTAMOS EM POSIÇÃO. HOLD...')
                df_prices['position'] = 'SIDE_BUY_IN_POSITION'
                df_prices.at[len(df_prices) - 1, 'hold'] = True

            else:
                # print(f'RSI: {rsi} entre os indicadores: HOLD... ')
                df_prices.at[len(df_prices) - 1, 'hold'] = True
            
            # WRITES LAST LINE ON CSV
            last_close = df_prices.iloc[-1:]
            if len(closes) == RSI_PERIOD:
                last_close.to_csv('positions-VITE.csv', header=True, index=False)
                print('--- CRIOU O CSV ---')
            else:
                last_close.to_csv('positions-VITE.csv', mode='a', header=False, index=False)
        
        return in_position
    except Exception as e:
        print(e)
    
def on_open(ws):
    print('opened connection')

def on_close(ws):
    print('closed connection')

def on_message(ws, message):
    """
    Função de callback chamada quando uma nova mensagem é recebida pelo WebSocket.
    Processa a mensagem JSON para obter o preço de fechamento e chama a função de processamento de preço.
    """
    try:
        global in_position
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_closed = candle['x']
        close = candle['c']
        
        if is_candle_closed:
            print('CLOSE:: ', close)
            in_position = process_new_price(close, df_prices, price_list, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, in_position)

    except Exception as e:
        print(e)

if __name__ == '__main__':

    COIN_CODE = 'BNB'
    WLT_COIN = 'BRL'
    SOCKET = f"wss://stream.binance.com:9443/ws/{COIN_CODE.lower()}{WLT_COIN.lower()}@kline_1m"

    RSI_PERIOD = 22
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    TRADE_SYMBOL = f'{COIN_CODE}{WLT_COIN}'
    MIN_PROFIT = 0.02

    symbol_info = get_symbol_info(TRADE_SYMBOL)
    filters = symbol_info['filters']

    QTY_PRECISION = symbol_info['baseAssetPrecision']
    STEP_SIZE = next(f['stepSize'] for f in filters if f['filterType'] == 'LOT_SIZE')
    MIN_OP = float(next(f['minNotional'] for f in filters if f['filterType'] == 'NOTIONAL'))
    STEP_SIZE_PRECISION = get_decimal_places(STEP_SIZE)
    STEP_SIZE = f"{float(STEP_SIZE):.{STEP_SIZE_PRECISION}f}".rstrip('0').rstrip('.')

    closes = []
    in_position = True    

    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY, tld='us')

    # set profit BASELINE to drive sell/buy operations
    BASELINE = 90

    # start app
    print(' --- Hello World! We are starting! ---')
    if not SIMULATE_FLG:
        try:
            print('Declare WebSocket')
            ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
            print(f'Run WebSocket - Start - {ws}')
            ws.run_forever()
            print('Run WebSocket - End')
        except BinanceAPIException as e:
            print(f'Error: {e}')
    else:
        random_test(df_prices, closes, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD)

    print('End script')