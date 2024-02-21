import websocket
import json
import pprint
#import talib
import numpy
import pandas as pd
import credentials as cd
import binance_functions
import datetime

# import config
from binance.client import Client, BinanceAPIException
from binance.enums import *

def order(side, quantity, symbol,order_type=ORDER_TYPE_MARKET):
    try:
        print("sending order")
        order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
        print(order)
        
    except Exception as e:
        print("an exception occured - {}".format(e))
        return False

    return True

    
def on_open(ws):
    print('opened connection')

def on_close(ws):
    print('closed connection')

def on_message(ws, message):
    global closes, in_position
    
    print('received message')
    json_message = json.loads(message)
    
    candle = json_message['k']

    is_candle_closed = candle['x']
    close = candle['c']
    
    if not is_candle_closed:
        print("candle closed at {} ' --- closes: ', {}".format(close, len(closes)))

        closes.append(float(close))
        # print(len(closes))

        # print("closes")
        # print(' --- closes: ', len(closes))

        if len(closes) > RSI_PERIOD:
            print('started df')

            df_closes.loc[len(df_closes)] = {'close': float(close)}

            df_closes['var'] = df_closes['close'].pct_change()
            df_closes['positivo'] = df_closes['var'].apply(lambda x : x if x > 0 else 0)
            df_closes['negativo'] = df_closes['var'].apply(lambda x : abs(x) if x < 0 else 0)

            #create RSI Column
            df_closes['media_positivos'] = df_closes['positivo'].rolling(window=RSI_PERIOD).mean()
            df_closes['media_negativo'] = df_closes['negativo'].rolling(window=RSI_PERIOD).mean()

            # dropping first 22 lines wich are NaN
            rsi = (100 -  100 /
                        (1+df_closes['media_positivos']/df_closes['media_negativo'])
                        )

            df_closes['RSI'] = rsi
            
            print("the current rsi is {}".format(rsi))
            print("the RSI_OVERBOUGHT is {}".format(RSI_OVERBOUGHT))
            print("the RSI_OVERSOLD is {}".format(RSI_OVERSOLD))


            if rsi > RSI_OVERBOUGHT and in_position:
                
                if in_position:
                    print('QUER VENDER E ESTÁ EM POSIÇÃO!')
                    
                    # get BTC free balance
                    btc_balance = binance_functions.get_balance('BTC')
                    print('btc_balance: ', btc_balance)
                    # convert BTC balance to BRL
                    btcbrl_balance = close * btc_balance
                    print('btcbrl_balance: ', btcbrl_balance)

                    # get profit compared to baseline
                    profit_value = btcbrl_balance - baseline
                    print('profit_value: ', profit_value)
                    
                    if profit_value > 0:

                        # convert profit in BRL to BTC
                        TRADE_QUANTITY = profit_value / close
                        
                        df_closes['in_position_before'] = in_position
                        # create sell order
                        order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)                    
                        if order_succeeded:
                            print('VENDEU!')
                            in_position = False 
                        
                        df_closes['position'] = 'SELL'
                        df_closes['quantity'] = TRADE_QUANTITY
                        df_closes['symbol'] = TRADE_SYMBOL
                        df_closes['btc_balance'] = btc_balance
                        df_closes['btcbrl_balance'] = btcbrl_balance
                        df_closes['profit_value'] = profit_value
                        df_closes['in_position_after'] = in_position

                    else:

                        print('In position to sell, but there''s no profit over the baseline')
                        df_closes['position'] = 'SIDE_SELL_NO_PROFITS'
                        df_closes['quantity'] = TRADE_QUANTITY
                        df_closes['symbol'] = TRADE_SYMBOL
                        df_closes['btc_balance'] = btc_balance
                        df_closes['btcbrl_balance'] = btcbrl_balance
                        df_closes['profit_value'] = profit_value
                        df_closes['in_position_after'] = in_position
                else:
                    
                        print('In position to sell, but don''t on it yet')
                        df_closes['position'] = 'SIDE_SELL_DONT_OWN_YET'
                        df_closes['quantity'] = TRADE_QUANTITY
                        df_closes['symbol'] = TRADE_SYMBOL
                        df_closes['btc_balance'] = btc_balance
                        df_closes['btcbrl_balance'] = btcbrl_balance
                        df_closes['profit_value'] = profit_value
                        df_closes['in_position_after'] = in_position
                    
            elif rsi < RSI_OVERSOLD:
                
                btc_balance = binance_functions.get_balance('BTC')

                btcbrl_balance = close * btc_balance

                # get profit compared to baseline
                profit_value = btcbrl_balance - baseline
                
                if in_position:
                    
                    print('In position to buy, but already have it')

                    df_closes['position'] = 'SIDE_BUY_ALREADY_OWN'
                    df_closes['quantity'] = TRADE_QUANTITY
                    df_closes['symbol'] = TRADE_SYMBOL
                    df_closes['btc_balance'] = btc_balance
                    df_closes['btcbrl_balance'] = btcbrl_balance
                    df_closes['profit_value'] = profit_value
                    df_closes['in_position_after'] = in_position
                else:
                    
                    # get BRL free balance
                    brl_balance = binance_functions.get_balance('BRL')
                    print('brl_balance: ', brl_balance)
                    if brl_balance > 0:
                
                        # convert BRL balance to BTC
                        TRADE_QUANTITY = brl_balance / close
                        
                        df_closes['position'] = 'SIDE_BUY_ALREADY_OWN'

                        # create buy order
                        order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)

                        df_closes['in_position_before'] = in_position

                        if order_succeeded:
                            in_position = True
                        
                        df_closes['position'] = 'BUY'
                        df_closes['quantity'] = TRADE_QUANTITY
                        df_closes['symbol'] = TRADE_SYMBOL
                        df_closes['btc_balance'] = btc_balance
                        df_closes['btcbrl_balance'] = btcbrl_balance
                        df_closes['profit_value'] = profit_value
                        df_closes['in_position_after'] = in_position

                    else:
                        
                        print('In position to buy, but don''t have enough balance')
                        df_closes['position'] = 'SIDE_BUY_NO_BALANCE'
                        df_closes['quantity'] = TRADE_QUANTITY
                        df_closes['symbol'] = TRADE_SYMBOL
                        df_closes['btc_balance'] = btc_balance
                        df_closes['btcbrl_balance'] = btcbrl_balance
                        df_closes['profit_value'] = profit_value
                        df_closes['in_position_after'] = in_position
            else:
                #RSI entre overbought e oversold
                print('No position to take')
                df_closes['position'] = 'HOLD'
                df_closes['quantity'] = TRADE_QUANTITY
                df_closes['symbol'] = TRADE_SYMBOL
                df_closes['btc_balance'] = btc_balance
                df_closes['btcbrl_balance'] = btcbrl_balance
                df_closes['profit_value'] = profit_value
                df_closes['in_position_after'] = in_position
                
                if len(closes) == (RSI_PERIOD + 1):
                    last_close.to_csv('positions.csv', index=False)
                    print('--- CRIOU O CSV ---')
                else:
                    last_close = df_closes.iloc[-1:]
                    # WRITES LAST LINE ON CSV
                    last_close.to_csv('positions.csv', mode='a', header=True, index=False)

            # WRITES LAST LINE ON CSV
            last_close.to_csv('positions.csv', mode='a', header=True, index=False)

            
            #CLEAR CLOSES LIST TO MANTAIN LAST 22 LINES ONLY
            closes.pop(0)
            
            if len(closes) == (RSI_PERIOD + 1):
                last_close.to_csv('positions.csv', index=False)
                print('--- CRIOU O CSV ---')

            

            last_close = df_closes.iloc[-1:]

            # WRITES LAST LINE ON CSV
            last_close.to_csv('positions.csv', mode='a', header=True, index=False)


if __name__ == '__main__':
        
    SOCKET = "wss://stream.binance.com:9443/ws/btcbrl@kline_1m"

    RSI_PERIOD = 10
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 65
    TRADE_SYMBOL = 'BTCBRL'
    TRADE_QUANTITY = 0.05

    closes = []
    in_position = False
    
    df_closes = pd.DataFrame(columns =['close', 'var', 'positivo'
                                       , 'negativo', 'media_positivos', 'media_negativo' 
                                       ,'position','quantity','symbol'
                                       ,'btc_balance','btcbrl_balance'
                                       ,'profit_value' ,'in_position_after'])
    

    client = Client(cd.API_KEY, cd.API_SECRET, tld='us')

    # set profit baseline to drive sell/buy operations
    baseline = 50

    # start app
    print(' --- Hello World! We are starting! ---')

    try:
        ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
        ws.run_forever()
    except BinanceAPIException as e:
        print(f'Error: {e}')
        
    print('End script')