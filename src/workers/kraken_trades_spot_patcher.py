from datetime import datetime, timedelta, timezone
import time

import ccxt
import polars as pl

from src.workers.base_patcher import BasePatcher


class KrakenTradesSpotPatcher(BasePatcher):
    def __init__(self, exchange_id: str, symbol: str, target_date: str, logger):
        super().__init__(exchange_id, symbol, target_date, logger)
        self.exchange = None
        self.mkt_type = "spot"

    def connection(self):
        try:
            self.exchange = ccxt.kraken({
                "enableRateLimit": True,
            })
            self.exchange.load_markets()
            self.logger.info("✅ [SUCCESS] Kraken spot connection established.")
        except Exception as e:
            self.exchange = None
            self.logger.error(f"❌ [RECONNECT-FAILED] {e}")
            raise

    def _fetch_page(self, request: dict, max_attempts: int = 5) -> dict:
        for attempt in range(1, max_attempts + 1):
            try:
                return self.exchange.publicGetTrades(request)
            except ccxt.NetworkError as e:
                if attempt == max_attempts:
                    raise

                delay = min(2 ** (attempt - 1), 8)
                self.logger.warning(
                    f"Kraken spot REST retry={attempt}/{max_attempts} "
                    f"delay={delay}s error={e}"
                )
                time.sleep(delay)

        raise RuntimeError("Kraken spot REST request exhausted all retries")

    def read_rest_data(self, symbol: str, target_date: str) -> list:
        start_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)

        market = self.exchange.market(symbol)

        # Kraken Spot's `since` cursor uses nanoseconds and is exclusive. Start
        # one nanosecond before the target day so a boundary trade is retained.
        cursor = str(start_ms * 1_000_000 - 1)
        seen_cursors = set()
        all_trades = []
        page_no = 0

        while True:
            page_no += 1
            response = self._fetch_page({
                "pair": market["id"],
                "since": cursor,
                "count": 1000,
            })

            result = response.get("result")
            if not isinstance(result, dict):
                raise RuntimeError(
                    f"Kraken spot returned an invalid result at page={page_no}"
                )

            elements = result.get(market["id"])
            if elements is None:
                # Kraken can occasionally use an alternate pair name as the
                # response key. `last` is the only non-market key.
                market_key = next((key for key in result if key != "last"), None)
                if market_key is None:
                    raise RuntimeError(
                        f"Kraken spot response has no market data at page={page_no}"
                    )
                elements = result.get(market_key, [])

            reached_end = False
            for raw_trade in elements:
                trade = self.exchange.parse_trade(raw_trade, market)
                timestamp = trade.get("timestamp")
                trade_id = trade.get("id")

                if timestamp is None:
                    raise RuntimeError(
                        f"Kraken spot trade has no timestamp at page={page_no}"
                    )

                # Kraken returns trades in ascending timestamp order.
                if timestamp >= end_ms:
                    reached_end = True
                    break

                if timestamp < start_ms:
                    continue

                if trade_id is None:
                    raise RuntimeError(
                        f"Kraken spot trade has no trade_id at page={page_no}"
                    )

                # Keep only the fields needed by trades_spot. This avoids
                # retaining CCXT's nested `info`, fee and order metadata.
                all_trades.append({
                    "id": trade_id,
                    "price": trade.get("price"),
                    "amount": trade.get("amount"),
                    "timestamp": timestamp,
                    "side": trade.get("side"),
                })

            next_cursor = result.get("last")
            self.logger.info(
                f"Kraken spot page={page_no}, "
                f"rows={len(elements)}, "
                f"collected={len(all_trades)}, "
                f"has_next={bool(next_cursor) and not reached_end}"
            )

            if reached_end:
                break

            if not elements and not next_cursor:
                break

            if not next_cursor:
                raise RuntimeError(
                    f"Kraken spot response has no pagination cursor at page={page_no}"
                )

            next_cursor = str(next_cursor)
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError(
                    f"Kraken spot returned repeated cursor at page={page_no}"
                )

            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return all_trades

    def _clear_data(
        self,
        exchange_id: str,
        mkt_type: str,
        symbol: str,
        trades: list,
    ) -> pl.LazyFrame:
        local_timestamp = int(time.time() * 1000)

        return (
            pl.LazyFrame(trades)
            .with_columns([
                pl.col("id").cast(pl.Int64).alias("trade_id"),
                pl.lit(exchange_id).alias("exchange_id"),
                pl.lit(symbol).alias("symbol"),
                pl.lit(mkt_type).alias("mkt_type"),
                pl.col("price").cast(pl.Float64),
                pl.col("amount").cast(pl.Float64),
                pl.col("timestamp").cast(pl.Int64),
                pl.col("side").cast(pl.String),
                pl.lit(local_timestamp, dtype=pl.Int64).alias("local_timestamp"),
            ])
            .select([
                "trade_id",
                "exchange_id",
                "symbol",
                "mkt_type",
                "price",
                "amount",
                "timestamp",
                "side",
                "local_timestamp",
            ])
            .filter(
                (pl.col("price") > 0)
                & (pl.col("amount") > 0)
                & pl.col("side").is_in(["buy", "sell"])
            )
            .unique(subset=["trade_id"], keep="first")
            .sort(["timestamp", "trade_id"])
        )

    def _get_ch_data(
        self,
        exchange_id: str,
        symbol: str,
        target_date: str,
    ) -> pl.LazyFrame:
        start_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)

        sql = f"""
            SELECT trade_id
            FROM market_data.trades_spot
            WHERE timestamp >= {start_ms}
                AND timestamp < {end_ms}
                AND exchange_id = '{exchange_id}'
                AND symbol = '{symbol}'
        """
        arrow = self.ch.query_arrow(sql)
        return (
            pl.from_arrow(arrow)
            .with_columns(pl.col("trade_id").cast(pl.Int64))
            .lazy()
        )

    def main(self):
        self.connection()
        trades = self.read_rest_data(self.symbol, self.target_date)
        self.logger.info(
            f"Kraken spot official rows={len(trades)} "
            f"symbol={self.symbol} date={self.target_date}"
        )

        if not trades:
            self.logger.warning(
                f"Kraken spot returned no trades for {self.symbol} {self.target_date}"
            )
            return

        official_lf = self._clear_data(
            self.exchange_id,
            self.mkt_type,
            self.symbol,
            trades,
        )
        ch_lf = self._get_ch_data(
            self.exchange_id,
            self.symbol,
            self.target_date,
        )
        gaps_df = official_lf.join(
            ch_lf,
            on="trade_id",
            how="anti",
        ).collect()

        if gaps_df.is_empty():
            self.logger.info(
                f"✅ [NO-GAPS] Kraken spot {self.symbol} {self.target_date}"
            )
            return

        try:
            # self.sync_to_clickhouse(gaps_df, "trades_spot")
            self.export_parquet(gaps_df, "trades_spot")
            self.logger.info(
                f"✅ [PATCHED] Exported {len(gaps_df)} missing records "
                f"for {self.exchange_id} {self.symbol}."
            )
        except Exception as e:
            self.logger.error(f"❌ [GAP-ERROR] Patch failed: {e}")
            raise
