import websocket
import json
import numpy as np
import pandas as pd
import credentials as cd
import binance_functions

from binance.client import Client, BinanceAPIException
from binance.enums import *

price_list = []
df_prices = pd.DataFrame(columns =['close', 'var', 'positivo'
                                       , 'negativo', 'media_positivos', 'media_negativo' 
                                       ,'position','quantity','symbol'
                                       ,'btc_balance','btcbrl_balance'
                                       ,'profit_value' ,'in_position_after'])
SIMULATE_FLG = True

def order(side, quantity, symbol,order_type=ORDER_TYPE_MARKET):
    if not SIMULATE_FLG:
        try:
            print("sending order")
            order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
            print(order)
            
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

            # Atualiza o DataFrame com o valor de RSI calculado
            df_prices.at[len(df_prices) - 1, 'RSI'] = rsi

            # Calcula outras métricas
            df_prices['var'] = df_prices['close'].pct_change()
            df_prices['positivo'] = df_prices['var'].apply(lambda x: x if x > 0 else 0)
            df_prices['negativo'] = df_prices['var'].apply(lambda x: abs(x) if x < 0 else 0)
            df_prices['media_positivos'] = df_prices['positivo'].rolling(window=RSI_PERIOD).mean()
            df_prices['media_negativo'] = df_prices['negativo'].rolling(window=RSI_PERIOD).mean()

            # Verifica os sinais de negociação com base no RSI e na variável in_position
            if rsi > RSI_OVERBOUGHT and in_position:
                print(f'RSI: {rsi} > RSI_OVERBOUGHT: {RSI_OVERBOUGHT}. Vendendo...')
                
                # get BTC free balance
                btc_balance = binance_functions.get_balance('BTC')
                print('btc_balance: ', btc_balance)

                # convert BTC balance to BRL
                btcbrl_balance = close * btc_balance
                print('btcbrl_balance: ', btcbrl_balance)

                # get profit compared to baseline
                profit_value = btcbrl_balance - baseline
                print('profit_value: ', profit_value)

                # Get BRL balance before order
                brl_balance_before = binance_functions.get_balance('BRL')
                
                # convert profit in BRL to BTC
                TRADE_QUANTITY = btc_balance #profit_value / close

                if profit_value > 0:
                    
                    df_prices['in_position_before'] = in_position
                    # create sell order
                    order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)                    
                    if order_succeeded:
                        print('VENDEU!')
                        total_vendas += 1
                        in_position = False 
                        
                        if total_vendas == 4:
                            ws.close()

                    # Get BRL balance after order
                    brl_balance_after = binance_functions.get_balance('BRL')
                    
                    print(f'RSI: {rsi} > RSI_OVERBOUGHT: {RSI_OVERBOUGHT}. Vendendo...')
                    df_prices.at[len(df_prices) - 1, 'hold'] = False
                    df_prices.at[len(df_prices) - 1, 'venda'] = True
                    df_prices['position'] = 'SELL'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['brl_balance_before'] = brl_balance_before
                    df_prices['brl_balance_after'] = brl_balance_after
                    df_prices['btc_balance'] = btc_balance
                    df_prices['btcbrl_balance'] = btcbrl_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
                else:

                    print('In position to sell, but there''s no profit over the baseline')
                    df_prices['position'] = 'SIDE_SELL_NO_PROFITS'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['brl_balance_before'] = brl_balance_before
                    df_prices['btc_balance'] = btc_balance
                    df_prices['btcbrl_balance'] = btcbrl_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
            
            elif rsi > RSI_OVERBOUGHT and not in_position:
                print(f'RSI: {rsi} > RSI_OVERBOUGHT: MAS NÃO ESTAMOS EM POSIÇÃO. HOLD...')
                df_prices.at[len(df_prices) - 1, 'hold'] = True
                df_prices['position'] = 'SIDE_SELL_DONT_OWN_YET'
            
            elif rsi < RSI_OVERSOLD and not in_position:
                # get BTC free balance
                btc_balance = binance_functions.get_balance('BTC')
                
                # convert BTC balance to BRL
                btcbrl_balance = close * btc_balance

                # get profit compared to baseline
                profit_value = btcbrl_balance - baseline

                # get BRL free balance
                brl_balance_before = binance_functions.get_balance('BRL')
                print('brl_balance: ', brl_balance_before)

                # convert BRL balance to BTC
                TRADE_QUANTITY = brl_balance_before / close

                if brl_balance_before > 0:
                    
                    #df_prices['position'] = 'SIDE_BUY_ALREADY_OWN'
                    print(f'Compra: {type(TRADE_QAUNTITY)} {TRADE_QAUNTITY}')
                    # create buy order
                    order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)

                    df_prices['in_position_before'] = in_position

                    if order_succeeded:
                        in_position = True

                    # Get BRL balance after order
                    brl_balance_after = binance_functions.get_balance('BRL')
                    
                    print(f'RSI: {rsi} < RSI_OVERSOLD: {RSI_OVERSOLD}. Comprando...')
                    df_prices.at[len(df_prices) - 1, 'hold'] = False
                    df_prices.at[len(df_prices) - 1, 'compra'] = True
                    df_prices['position'] = 'BUY'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['brl_balance_before'] = brl_balance_before
                    df_prices['brl_balance_after'] = brl_balance_after
                    df_prices['btc_balance'] = btc_balance
                    df_prices['btcbrl_balance'] = btcbrl_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position

                else:
                    
                    print(f'RSI: {rsi} < RSI_OVERSOLD: MAS NÃO TEMOS SALDO SUFICIENTE. HOLD...')
                    df_prices['position'] = 'SIDE_BUY_NO_BALANCE'
                    df_prices['quantity'] = TRADE_QUANTITY
                    df_prices['symbol'] = TRADE_SYMBOL
                    df_prices['brl_balance_before'] = brl_balance_before
                    df_prices['btc_balance'] = btc_balance
                    df_prices['btcbrl_balance'] = btcbrl_balance
                    df_prices['profit_value'] = profit_value
                    df_prices['in_position_after'] = in_position
            
            elif rsi < RSI_OVERSOLD and in_position:
                print(f'RSI: {rsi} < RSI_OVERSOLD: MAS JÁ ESTAMOS EM POSIÇÃO. HOLD...')
                df_prices.at[len(df_prices) - 1, 'hold'] = True

            else:
                print(f'RSI: {rsi} entre os indicadores: HOLD... ')
                df_prices.at[len(df_prices) - 1, 'hold'] = True
            
            # WRITES LAST LINE ON CSV
            last_close = df_prices.iloc[-1:]
            if len(closes) == (RSI_PERIOD + 1):
                last_close.to_csv('positions.csv', header=True, index=False)
                print('--- CRIOU O CSV ---')
            else:
                last_close.to_csv('positions.csv', mode='a', header=False, index=False)
        
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
        
        if not is_candle_closed:
            in_position = process_new_price(close, df_prices, price_list, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, in_position)

    except Exception as e:
        print(e)

if __name__ == '__main__':

    SOCKET = "wss://stream.binance.com:9443/ws/btcbrl@kline_1m"

    RSI_PERIOD = 22
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 50
    TRADE_SYMBOL = 'BTCBRL'
    TRADE_QUANTITY = 0.05

    closes = []
    in_position = False    

    client = Client(cd.API_KEY, cd.API_SECRET, tld='us')

    # set profit baseline to drive sell/buy operations
    baseline = 300

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