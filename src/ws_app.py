import websocket
import json
#import pprint
#import talib
import numpy
import pandas as pd
import credentials
import binance_functions

# import config
from binance.client import Client
from binance.enums import *

def order(side, quantity, symbol,order_type=ORDER_TYPE_MARKET):
    try:
        print("sending order")
        # order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
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
    
    # print('received message')
    json_message = json.loads(message)
    # pprint.pprint(json_message)

    candle = json_message['k']

    is_candle_closed = candle['x']
    close = candle['c']

    if is_candle_closed:
        print("candle closed at {} ' --- closes: ', {}".format(close, len(closes)))
        closes.append(float(close))
        
        print("closes")
        print(' --- closes: ', len(closes))

        if len(closes) > RSI_PERIOD:
            
            # np_closes = numpy.array(closes)
            df_closes = pd.DataFrame(data=closes)


            df_closes['var'] = df_closes['close'].pct_change().dropna()
            df_closes['positivo'] = df_closes['var'].apply(lambda x : x if x > 0 else 0)
            df_closes['negativo'] = df_closes['var'].apply(lambda x : abs(x) if x < 0 else 0)

            #create RSI Column
            df_closes['media_positivos'] = df_closes['positivo'].rolling(window=22).mean()
            df_closes['media_negativo'] = df_closes['negativo'].rolling(window=22).mean()

            # dropping first 22 lines wich are NaN
            df_closes = df_closes.dropna()
            rsi = (100 -  100 /
                        (1+df_closes['media_positivos']/df_closes['media_negativo'])
                        )

            df_closes['RSI'] = rsi

            # RSI BY TA-LIB            
            #rsi = talib.RSI(np_closes, RSI_PERIOD)
            # LAST CALCULATED RSI (just in case)
            # last_rsi = rsi[-1]
            
            print("the current rsi is {}".format(rsi))

            if rsi > RSI_OVERBOUGHT and in_position:
                
                if in_position:
                
                    # get BTC free balance
                    btc_balance = binance_functions.get_balance('BTC')
                    
                    # convert BTC balance to BRL
                    btcbrl_balance = close * btc_balance

                    # get profit compared to baseline
                    profit_value = btcbrl_balance - baseline
                    
                    if profit_value > 0:

                        # convert profit in BRL to BTC
                        TRADE_QUANTITY = profit_value / close
                        
                        # create sell order
                        # order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)                    
                        # if order_succeeded:
                        #     in_position = False 
                        in_position = False
                    else:

                        print('In position to sell, but there''s no profit over the baseline')
                else:
                    # df.at[i, 'position'] = 'DOI'
                    print('In position to sell, but don''t on it yet')
                    
            elif rsi < RSI_OVERSOLD:
                
                if in_position:
                    print('In position to buy, but already have it')
                    # df.at[i, 'position'] = 'HOLD'
                else:
                    
                    # get BRL free balance
                    brl_balance = binance_functions.get_balance('BRL')

                    if brl_balance > 0:
                
                        # convert BRL balance to BTC
                        TRADE_QUANTITY = brl_balance / close
                        
                        # create buy order
                        # order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)
                        # if order_succeeded:
                        #     in_position = True
                        in_position = True

                        # df.at[i, 'position'] = 'BOUGHT'

                    else:
                        
                        print('In position to buy, but don''t have enough balance')
            else:
                #RSI entre overbought e oversold
                print('No position to take')
                # df.at[i, 'position'] = 'HOLD'
            
            #CLEAR CLOSES LIST TO MANTAIN LAST 22 LINES ONLY
            closes.pop(0)

            df_closes.to_csv('operations_log.csv', mode='a', header=False, index=False)

if _name_ == '_main_':
        
    SOCKET = "wss://stream.binance.com:9443/ws/btcbrl@kline_1m"

    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 65
    TRADE_SYMBOL = 'BTCBRL'
    TRADE_QUANTITY = 0.05

    closes = []
    in_position = False
    df_closes = pd.DataFrame(columns =['close'])
    
    client = Client(credentials.API_KEY, credentials.API_SECRET, tld='us')

    # set profit baseline to drive sell/buy operations
    baseline = 50

    # start app
    ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
    ws.run_forever()
