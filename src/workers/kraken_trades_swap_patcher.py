from src.workers.base_patcher import BasePatcher

import ccxt
import polars as pl
import os
import time
from datetime import datetime,timezone,timedelta

class KrakenTradesSwapPatcher(BasePatcher):
    def __init__(self, exchange_id, symbol, target_date, logger):
        super().__init__(exchange_id, symbol, target_date, logger)
        self.exchange = None
        self.mkt_type = "swap"
        split_symbols = self.symbol.split("/")
        self.swap_symbol = f"{split_symbols[0]}/{split_symbols[1]}:{split_symbols[1]}"

    def connection(self):
        try:
            self.exchange = ccxt.krakenfutures({
                "enableRateLimit":True,
                "newUpdates":True,
            })

            self.exchange.load_markets()

            self.logger.info("✅ [SUCCESS] Connection established.")

        except Exception as e:
            self.exchange = None
            self.logger.error(f"❌ [RECONNECT-FAILED] {e}")
            raise e

    def read_rest_data(self,symbol:str,target_date:str):
        star_dt = datetime.strptime(target_date,"%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ms = int(star_dt.timestamp() * 1000)
        end_ms = int((star_dt + timedelta(days=1)).timestamp() * 1000)

        market = self.exchange.market(symbol)

        base_request = {
            "symbol":market["id"],
            "since":start_ms,
            "before":end_ms,
            "sort":"asc",
            "count":500,
        }

        token = None
        seen_tokens = set()
        all_trades = []
        page_no = 0

        while True:
            page_no += 1

            request = dict(base_request)

            if token:
                request["continuation_token"] = token

            response = self.exchange.historyGetMarketSymbolExecutions(request)

            elements = response.get("elements",[])

            for element in elements:
                execution = element.get("event",{}).get("Execution",{}).get("execution")

                if not execution:
                    continue

                trade = self.exchange.parse_trade(execution,market)

                if start_ms <= trade["timestamp"] < end_ms:
                    all_trades.append(trade)

            next_token = response.get("continuationToken")

            self.logger.info(
                f"Kraken page={page_no}, "
                f"rows={len(elements)}, "
                f"has_next={bool(next_token)}"
            )

            if not next_token:
                break

            if next_token == token or next_token in seen_tokens:
                raise RuntimeError(
                    f"Kraken returned repeated continuationToken at page={page_no}"
                )

            seen_tokens.add(next_token)
            token = next_token

        return all_trades

    def _clear_data(self,exchange_id:str,mkt_type:str,symbol:str,trades:list) -> pl.LazyFrame:
        lf = pl.LazyFrame(trades)

        return lf.with_columns([
            pl.lit(exchange_id).alias("exchange_id"),
            pl.lit(symbol).alias("symbol"),
            pl.lit(mkt_type).alias("mkt_type"),
            pl.col("id").cast(pl.String).alias("trade_id"),
            pl.col("price").cast(pl.Float64),
            pl.col("amount").cast(pl.Float64),
            pl.col("timestamp").cast(pl.Int64),
            pl.lit(time.time() * 1000).cast(pl.Int64).alias("local_timestamp"),
        ]).select(["trade_id","exchange_id","symbol","mkt_type","price","amount","timestamp","side","local_timestamp"]).sort("timestamp")

    def _get_ch_data(self,exchange_id:str,symbol:str,target_date:str) -> pl.LazyFrame:
        star_dt = datetime.strptime(target_date,"%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ms = int(star_dt.timestamp() * 1000)
        end_ms = int((star_dt + timedelta(days=1)).timestamp() * 1000)

        sql = f"""
            SELECT trade_id FROM market_data.trades_swap
            WHERE timestamp>={start_ms} AND timestamp<{end_ms}
                AND exchange_id='{exchange_id}'
                AND symbol='{symbol}'
            ORDER BY trade_id ASC
        """
        arrow = self.ch.query_arrow(sql)
        if arrow.num_rows == 0:
            return pl.LazyFrame()
        else:
            return pl.from_arrow(arrow).with_columns([
                pl.col("trade_id").cast(pl.String)
            ]).lazy()

    def main(self):
        self.connection()
        trades = self.read_rest_data(self.swap_symbol,self.target_date)

        print(f"Total: {len(trades)}")

        if len(trades) > 0:
            official_lf = self._clear_data(self.exchange_id,self.mkt_type,self.symbol,trades).unique(subset=["trade_id"],keep="first")
            ch_lf = self._get_ch_data(self.exchange_id,self.symbol,self.target_date).collect()
            gaps_df = pl.DataFrame()

            if ch_lf.is_empty():
                gaps_df = official_lf.collect()
            else:
                gaps_df = official_lf.join(ch_lf.lazy(),on="trade_id",how="anti").collect()

            if not gaps_df.is_empty():
                try:
                    # self.sync_to_clickhouse(gaps_df,'trades_swap')
                    self.export_parquet(gaps_df,"trades_swap")
                    self.logger.info(f"✅ [PATCHED] Injected {len(gaps_df)} missing records into {self.exchange_id} {self.symbol}.")
                except Exception as e:
                    self.logger.error(f"❌ [GAP-ERROR] Patch failed: {e}")
