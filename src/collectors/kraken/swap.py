from src.collectors.base.stream_base import StreamBase
from src.models.schema import TradeDataForSwap,MarkPriceData,OpenInterestData,FundingRateData,OrderbookForSwap,LiquidationsData

import ccxt.pro as ccxt_pro
import asyncio
import time

class KrakenSwapManager(StreamBase):
    def __init__(self, exchange_id, mkt_type):
        super().__init__(exchange_id, mkt_type)

    async def connect(self):
        async with self._reconnect_lock:
            if not self._reconnect_lock and self.ws:
                return

            try:
                if self.ws:
                    self.logger.info(f"🔄 [CLOSE] Close old CCXT Pro client for {self.exchange_id}")
                    await self.ws.close()
                    # Clear the old client before creating the replacement. If
                    # construction fails, background loops can retry from ws=None.
                    self.ws = None

                self.logger.info(f"🔄 [RECONNECT] Initializing new CCXT Pro client for {self.exchange_id}...")
                self.ws = ccxt_pro.krakenfutures({
                    "enableRateLimit":True,
                    "newUpdates":True,
                    "options":{
                        "adjustForTimeDifference":True,
                        "ws": { 
                            "heartbeat": 20000 
                        }
                    }
                })

                self.ws.load_markets()

                await asyncio.sleep(0.01)
                self.logger.info("✅ [SUCCESS] Connection established.")

            except Exception as e:
                self.ws = None
                self.logger.error(f"❌ [RECONNECT-FAILED] {e}")
                raise e
            finally:
                self._is_reconnecting = False

    async def _handle_orderbook(self,symbol:str,data):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace("/","-")}:orderbook"
        registry_key = f"registry:streams:orderbook"
        await self.register_stream_once(registry_key,stream_key)
        raw_ts = data.get("timestamp")
        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
        raw_nonce = data.get("nonce")
        nonce = raw_nonce if raw_nonce is not None else 0

        try:
            tick = OrderbookForSwap(
                exchange_id=self.exchange_id,
                symbol=symbol,
                mkt_type=self.mkt_type,
                bid_prices=[row[0] for row in data["bids"][:20]],
                bid_volumes=[row[1] for row in data["bids"][:20]],
                ask_prices=[row[0] for row in data["asks"][:20]],
                ask_volumes=[row[1] for row in data["asks"][:20]],
                nonce=nonce,
                timestamp=ts
            )
            await self.redis.xadd(stream_key,{"data":tick.model_dump_json()},maxlen=1000,approximate=True)

        except Exception as e:
            self.logger.error(f"orderbook add redis error: {e}")

    async def _handle_trades(self,symbol:str,trades):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace("/","-")}:trades"
        registry = "registry:streams:trades"
        await self.register_stream_once(registry,stream_key)

        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                for trade_dict in trades:
                    if trade_dict["price"] > 0 and trade_dict["amount"] > 0:
                        raw_ts = trade_dict.get("timestamp")
                        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                        trade_sequence = trade_dict.get("info",{}).get("seq")
                        trade = TradeDataForSwap(
                            exchange_id=self.exchange_id,
                            symbol=symbol,
                            mkt_type=self.mkt_type,
                            trade_id=str(trade_dict["id"]),
                            trade_sequence=trade_sequence,
                            timestamp=ts,
                            side=trade_dict["side"],
                            price=trade_dict["price"],
                            amount=trade_dict["amount"]
                        )
                        await self.redis.xadd(stream_key,{"data":trade.model_dump_json()},maxlen=10000,approximate=True)

                await pipe.execute()

        except Exception as e:
            self.logger.error(f"swap trades add redis error: {e}")

    async def _handle_ticker(self,symbol:str,data):
        stream_key_mp = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:mark_price"
        registry_mp = f"registry:streams:mark_price"
        await self.register_stream_once(registry_mp,stream_key_mp)

        stream_key_fr = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:funding_rate"
        registry_fr = f"registry:streams:funding_rate"
        await self.register_stream_once(registry_fr,stream_key_fr)

        stream_key_oi = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:open_interest"
        registry_oi = f"registry:streams:open_interest"
        await self.register_stream_once(registry_oi,stream_key_oi)

        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                info = data["info"]

                raw_ts = data.get("timestamp")
                ts = raw_ts if raw_ts is not None else int(time.time() * 1000)

                markPriceData = MarkPriceData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    mark_price=data['markPrice'],
                    timestamp=ts,
                    index_price=data['indexPrice']
                )

                fundingRateData = FundingRateData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    timestamp=ts,
                    funding_rate=info['funding_rate'],
                    next_funding_rate_timestamp=info['next_funding_rate_time']
                )

                oiData = OpenInterestData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    timestamp=ts,
                    base_volume=data['baseVolume'],
                    open_interest_amount=info['openInterest']
                )

                await self.redis.xadd(stream_key_mp,{"data":markPriceData.model_dump_json()},maxlen=500,approximate=True)
                await self.redis.xadd(stream_key_fr,{"data":fundingRateData.model_dump_json()},maxlen=100,approximate=True)
                await self.redis.xadd(stream_key_oi,{"data":oiData.model_dump_json()},maxlen=300,approximate=True)
                await pipe.execute()

        except Exception as e:
            self.logger.error(f"tick add redis error: {e}")

