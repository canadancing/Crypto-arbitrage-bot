import asyncio
import os
from gateio_funding_arb.config import load_config
import ccxt.async_support as ccxt
import pprint

async def main():
    config = load_config("config.yaml")
    gate_config = next(c for c in config.exchanges if c.name == "gateio")
    
    exchange = ccxt.gateio({
        "apiKey": gate_config.api_key,
        "secret": gate_config.api_secret,
        "options": {'defaultType': 'spot'}
    })
    
    try:
        res = await exchange.fetch_balance()
        if 'USDT' in res:
            print("USDT Balance:")
            pprint.pprint(res['USDT'])
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
