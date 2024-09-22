import hmac
import time
import hashlib
import requests
import pandas as pd
import credentials
from google.cloud import bigquery
from urllib.parse import urlencode

"""

Because USER_DATA endpoints require signature:
- call `send_signed_request` for USER_DATA endpoints
- call `send_public_request` for public endpoints

"""

KEY = credentials.API_KEY
SECRET = credentials.SECRET_KEY
BASE_URL = "https://binance.com"  # production base url
# BASE_URL = "https://bpay.binanceapi.com"  # production base url
# BASE_URL = "https://api.binance.com"  # production base url
# BASE_URL = 'https://testnet.binance.vision' # testnet base url

def hashing(query_string):
    return hmac.new(
        SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def get_timestamp():
    return int(time.time() * 1000)


def dispatch_request(http_method):
    session = requests.Session()
    session.headers.update(
        {"Content-Type": "application/json;charset=utf-8", "X-MBX-APIKEY": KEY}
    )
    return {
        "GET": session.get,
        "DELETE": session.delete,
        "PUT": session.put,
        "POST": session.post,
    }.get(http_method, "GET")


# used for sending request requires the signature
def send_signed_request(http_method, url_path, payload={}):
    query_string = urlencode(payload, True)
    if query_string:
        query_string = "{}&timestamp={}".format(query_string, get_timestamp())
    else:
        query_string = "timestamp={}".format(get_timestamp())

    url = (
        BASE_URL + url_path + "?" + query_string + "&signature=" + hashing(query_string)
    )
    print("{} {}".format(http_method, url))
    params = {"url": url, "params": {}}
    response = dispatch_request(http_method)(**params)
    return response.json()


# used for sending public data request
def send_public_request(url_path, payload={}):
    query_string = urlencode(payload, True)
    url = BASE_URL + url_path
    if query_string:
        url = url + "?" + query_string
    print("{}".format(url))
    response = dispatch_request("GET")(url=url)
    return response.json()

def get_balance(symbol,type='free'):

    # validate type param
    assert type in ['free','locked'], 'Invalid balance type'

    # get account balances list
    response = send_signed_request("GET", "/api/v3/account")

    # treat and filter balances greater than 0
    df_balances = pd.DataFrame(response['balances'])
    df_balances['free'] = df_balances['free'].astype(float)
    df_balances['locked'] = df_balances['locked'].astype(float)
    df_balances[(df_balances['free'] + df_balances['locked']) > 0]

    # return balance from the symbol and type selected
    return int(df_balances[df_balances['asset'] == symbol][type])

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


""" ======  end of functions ====== 

### public data endpoint, call send_public_request #####
# get klines
response = send_public_request(
    "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1d"}
)
print(response)


### USER_DATA endpoints, call send_signed_request #####
# get account informtion
# if you can see the account details, then the API key/secret is correct
response = send_signed_request("GET", "/api/v3/account")
print(response)


# # place an order
# if you see order response, then the parameters setting is correct
params = {
    "symbol": "BNBUSDT",
    "side": "BUY",
    "type": "LIMIT",
    "timeInForce": "GTC",
    "quantity": 1,
    "price": "20",
}
response = send_signed_request("POST", "/api/v3/order", params)
print(response)


# User Universal Transfer
params = {"type": "MAIN_MARGIN", "asset": "USDT", "amount": "0.1"}
response = send_signed_request("POST", " /sapi/v1/asset/transfer", params)
print(response)


# New Future Account Transfer (FUTURES)
params = {"asset": "USDT", "amount": 0.01, "type": 2}
response = send_signed_request("POST", "/sapi/v1/futures/transfer", params)
print(response)

"""