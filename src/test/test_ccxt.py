import ccxt.pro as ccxt_pro
import asyncio
import polars as pl

async def main():
    symbol = "BTC/USD"
    # exchange = ccxt_pro.krakenfutures({
    #     "enableRateLimit":True,
    #     "newUpdates":True,
    # })
    exchange = ccxt_pro.kraken({
            "enableRateLimit":True,
            "newUpdates":True,
        })
    await exchange.load_markets()

    try:
        # data = await exchange.watch_ticker(symbol)
        # for key,value in data.items():
        #     print(f"[{key}]:{value}")

        data = await exchange.watch_trades(symbol)
        print(data[0])
    except Exception as e:
        print(f"error: {e}")
    finally:
        await exchange.close()
    
    # orderbook = await exchange.watch_order_book(symbol)
    # print(orderbook.keys())

    # mark_price = await exchange.watch_mark_price(symbol)
    # print("mark_price")
    # print(mark_price)

    # oi = await exchange.fetch_open_interest(symbol)
    # print("oi")
    # print(oi)

    # trades = await exchange.watch_trades(symbol)
    # print("trades")
    # print(trades)
    # while True:
    #     funding_rate = await asyncio.wait_for(exchange.watch_funding_rate(symbol),timeout=60)
    #     print("funding rate")
    #     print(funding_rate)

if __name__ == '__main__':
    asyncio.run(main())