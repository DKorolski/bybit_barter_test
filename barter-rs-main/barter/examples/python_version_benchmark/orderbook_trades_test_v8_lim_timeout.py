import json
import zipfile
from dataclasses import dataclass
from typing import Optional, List

import numpy as np
import pandas as pd
from tqdm import tqdm

# ===== CONFIG V8 =====
OB_ZIP_PATH   = "2025-11-16_BTCUSDT_ob200_2.data.zip"
TRADES_PATH   = "BTCUSDT2025-11-16.csv.gz"

ROWS_TO_PROCESS: Optional[int] = 800_000
MAX_LEVELS     = 20

TICK_SIZE      = 0.1
ORDER_SIZE     = 1.0

# Биржевые комиссии (доля от нотиона на сторону)
FEE_RATE_MAKER = 0.0
FEE_RATE_TAKER = 0.0066/100  # сейчас не используется, но оставим

# Фикс брокера за каждую сторону (entry и exit) в котируемой валюте (USDT)
BROKER_FEE_ABS = 1.0

# "Идеальный" TP вокруг которого ищем кластер
TP_TICKS       = 200

# Кластеры для входа
ENTRY_CLUSTER_Q         = 0.895
MAX_CLUSTER_DEPTH_ENTRY = 12

# Кластеры для выхода
EXIT_CLUSTER_Q          = 0.3
MAX_CLUSTER_DEPTH_EXIT  = 12

# Когда после входа начинаем охотиться за выходным китом
EXIT_CLUSTER_START_MS   = 800

# Таймаут лимитки выхода по кластеру (и входной лимитки)
EXIT_ORDER_TIMEOUT_MS   = 2_500

# Насколько кластер может отличаться от tp_plain (в тиках)
EXIT_CLUSTER_MAX_DIFF_TICKS = 197

# Через сколько начинаем "time-stop" режим
MAX_HOLD_MS = 60_000  # 30 секунд

# Сколько живёт лимитка "time-stop" перед перевыставлением
TIME_STOP_ORDER_TIMEOUT_MS = 800  # 2 секунды

# ===== РЕЗУЛЬТАТ СДЕЛКИ =====

@dataclass
class TradeResult:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: int               # +1 long / -1 short
    entry_price: float
    exit_price: float
    reason: str             # cluster_exit / time_stop_limit / eod_force / limit_exit
    gross_ticks: float
    net_ticks: float
    gross_ret: float
    net_ret: float
    waited_ms_for_fill: int
    entry_fee: float
    exit_fee: float
    total_fee: float
    exit_liquidity: str     # maker / taker
    tp_plain: float
    exit_from_cluster: bool


# ===== ЗАГРУЗКА ДАННЫХ =====

def load_orderbook_features(
    zip_path: str,
    rows_to_process: Optional[int] = None,
    max_levels: int = 20,
) -> pd.DataFrame:
    records: List[dict] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("В zip нет файлов")
        data_name = names[0]
        print(f"[OB] Читаю {data_name} из {zip_path}")

        with zf.open(data_name, "r") as f:
            it = enumerate(f)
            if rows_to_process is not None:
                it = tqdm(it, total=rows_to_process, desc="Чтение L2")

            for i, raw_line in it:
                if rows_to_process is not None and i >= rows_to_process:
                    break

                line = raw_line.decode("utf-8").strip().strip("'")
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                ts = obj.get("ts")
                data = obj.get("data", {})
                bids = data.get("b", [])
                asks = data.get("a", [])
                if ts is None or not bids or not asks:
                    continue

                # best bid / ask
                bbp, bbv = float(bids[0][0]), float(bids[0][1])
                bap, bav = float(asks[0][0]), float(asks[0][1])

                # крупнейший bid-кластер
                max_b = min(max_levels, len(bids))
                cbp, cbv, cbd = None, 0.0, None
                for lvl in range(max_b):
                    p, v = float(bids[lvl][0]), float(bids[lvl][1])
                    if v > cbv:
                        cbp, cbv, cbd = p, v, lvl

                # крупнейший ask-кластер
                max_a = min(max_levels, len(asks))
                cap, cav, cad = None, 0.0, None
                for lvl in range(max_a):
                    p, v = float(asks[lvl][0]), float(asks[lvl][1])
                    if v > cav:
                        cap, cav, cad = p, v, lvl

                records.append(
                    {
                        "ts": int(ts),
                        "bbp": bbp, "bbv": bbv,
                        "bap": bap, "bav": bav,
                        "cbp": cbp, "cbv": cbv, "cbd": cbd,
                        "cap": cap, "cav": cav, "cad": cad,
                    }
                )

    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    print(f"[OB] Снэпшотов прочитано: {len(df)}")
    print("OB ts range:", df["ts"].min(), "->", df["ts"].max())
    return df


def load_trades(trades_path: str) -> pd.DataFrame:
    trades = pd.read_csv(trades_path, compression="gzip")
    print("[TR] Колонки:", trades.columns.tolist())

    if "ts" not in trades.columns:
        if "timestamp" in trades.columns:
            trades = trades.rename(columns={"timestamp": "ts"})
        elif "time" in trades.columns:
            trades = trades.rename(columns={"time": "ts"})
        else:
            raise ValueError("Не найден ts/timestamp/time в трейдах")

    if "price" not in trades.columns:
        raise ValueError("Нет колонки 'price' в трейдах")

    if "size" not in trades.columns:
        if "qty" in trades.columns:
            trades = trades.rename(columns={"qty": "size"})
        elif "volume" in trades.columns:
            trades = trades.rename(columns={"volume": "size"})
        else:
            raise ValueError("Нет size/qty/volume в трейдах")

    # нормируем ts к миллисекундам
    if not np.issubdtype(trades["ts"].dtype, np.number):
        dt = pd.to_datetime(trades["ts"], utc=True)
        trades["ts"] = (dt.view("int64") // 1_000_000).astype("int64")
    else:
        ts_max = trades["ts"].max()
        if ts_max < 1e11:      # секунды
            trades["ts"] = trades["ts"].astype("int64") * 1000
        elif ts_max > 1e14:    # наносекунды
            trades["ts"] = trades["ts"].astype("int64") // 1_000_000

    trades["time"] = pd.to_datetime(trades["ts"], unit="ms", utc=True)
    trades = trades.sort_values("ts").reset_index(drop=True)

    print(f"[TR] Записей: {len(trades)}")
    print("TR ts range:", trades["ts"].min(), "->", trades["ts"].max())
    return trades[["ts", "time", "price", "size"]]


# ===== БЭКТЕСТ V8 =====

def backtest_tick_v8(ob: pd.DataFrame, tr: pd.DataFrame) -> pd.DataFrame:
    # пороги кластеров
    bid_entry_thr = ob["cbv"].quantile(ENTRY_CLUSTER_Q)
    ask_entry_thr = ob["cav"].quantile(ENTRY_CLUSTER_Q)
    bid_exit_thr  = ob["cbv"].quantile(EXIT_CLUSTER_Q)
    ask_exit_thr  = ob["cav"].quantile(EXIT_CLUSTER_Q)

    print(f"[BT] Entry bid thr: {bid_entry_thr:.4f}")
    print(f"[BT] Entry ask thr: {ask_entry_thr:.4f}")
    print(f"[BT] Exit bid thr:  {bid_exit_thr:.4f}")
    print(f"[BT] Exit ask thr:  {ask_exit_thr:.4f}")

    ob = ob.sort_values("ts").reset_index(drop=True)
    ob_ts   = ob["ts"].values
    ob_time = ob["time"].values
    ob_bbp  = ob["bbp"].values
    ob_bbv  = ob["bbv"].values
    ob_bap  = ob["bap"].values
    ob_bav  = ob["bav"].values
    ob_cbp  = ob["cbp"].values
    ob_cbv  = ob["cbv"].values
    ob_cbd  = ob["cbd"].values
    ob_cap  = ob["cap"].values
    ob_cav  = ob["cav"].values
    ob_cad  = ob["cad"].values

    tr = tr.sort_values("ts").reset_index(drop=True)
    tr_ts    = tr["ts"].values
    tr_time  = tr["time"].values
    tr_price = tr["price"].values
    tr_size  = tr["size"].values

    n_ob = len(ob_ts)
    n_tr = len(tr_ts)

    i_ob = 0
    i_tr = 0

    state = "flat"  # flat / waiting_entry / in_position
    side = 0

    # входная лимитка
    order_price = None
    order_ts = None
    queue_ahead = 0.0
    entry_order_ts = None

    # позиция
    entry_price = None
    entry_ts = None
    entry_time = None
    tp_plain = None

    # выходная лимитка
    exit_order_price = None
    exit_order_ts = None
    exit_queue_ahead = 0.0
    exit_reason_target = None  # cluster_exit / time_stop_limit

    trades_res: List[TradeResult] = []

    n_signals = 0
    n_orders_placed = 0
    n_filled = 0
    n_order_timeouts = 0
    n_exit_orders_placed = 0
    n_exit_orders_timeout = 0

    last_trade_price = None
    last_mid = None
    last_time = None

    while i_ob < n_ob or i_tr < n_tr:
        next_ob_ts = ob_ts[i_ob] if i_ob < n_ob else None
        next_tr_ts = tr_ts[i_tr] if i_tr < n_tr else None

        if next_tr_ts is None or (next_ob_ts is not None and next_ob_ts <= next_tr_ts):
            # ===== событие стакана =====
            ts = int(next_ob_ts)

            bbp = float(ob_bbp[i_ob])
            bbv = float(ob_bbv[i_ob])
            bap = float(ob_bap[i_ob])
            bav = float(ob_bav[i_ob])

            cbp = ob_cbp[i_ob]
            cbv = float(ob_cbv[i_ob]) if ob_cbv[i_ob] is not None else 0.0
            cbd = ob_cbd[i_ob]

            cap = ob_cap[i_ob]
            cav = float(ob_cav[i_ob]) if ob_cav[i_ob] is not None else 0.0
            cad = ob_cad[i_ob]

            cur_time = pd.Timestamp(ob_time[i_ob])

            last_mid = (bbp + bap) / 2.0
            last_time = cur_time

            # 1) таймаут входной лимитки
            if state == "waiting_entry" and order_ts is not None:
                if ts - order_ts >= EXIT_ORDER_TIMEOUT_MS:
                    state = "flat"
                    side = 0
                    order_price = None
                    order_ts = None
                    queue_ahead = 0.0
                    entry_order_ts = None
                    n_order_timeouts += 1

            # 2) таймаут выходной лимитки (кластер или time-stop-limit)
            if state == "in_position" and exit_order_price is not None and exit_order_ts is not None:
                timeout_ms = EXIT_ORDER_TIMEOUT_MS
                if exit_reason_target == "time_stop_limit":
                    timeout_ms = TIME_STOP_ORDER_TIMEOUT_MS
                if ts - exit_order_ts >= timeout_ms:
                    exit_order_price = None
                    exit_order_ts = None
                    exit_queue_ahead = 0.0
                    exit_reason_target = None
                    n_exit_orders_timeout += 1

            # 3) кластерный выход (только пока hold < MAX_HOLD_MS)
            if (
                state == "in_position"
                and entry_ts is not None
                and exit_order_price is None
                and ts - entry_ts < MAX_HOLD_MS
                and tp_plain is not None
            ):
                if side == +1:
                    is_big_ask = (
                        cav >= ask_exit_thr
                        and cad is not None
                        and cad <= MAX_CLUSTER_DEPTH_EXIT
                        and cap is not None
                    )
                    if is_big_ask:
                        cap_f = float(cap)
                        candidate = cap_f - TICK_SIZE
                        if abs(candidate - tp_plain) <= EXIT_CLUSTER_MAX_DIFF_TICKS * TICK_SIZE:
                            exit_order_price = candidate
                            exit_order_ts = ts
                            exit_queue_ahead = 0.0  # перед стеной
                            exit_reason_target = "cluster_exit"
                            n_exit_orders_placed += 1
                elif side == -1:
                    is_big_bid = (
                        cbv >= bid_exit_thr
                        and cbd is not None
                        and cbd <= MAX_CLUSTER_DEPTH_EXIT
                        and cbp is not None
                    )
                    if is_big_bid:
                        cbp_f = float(cbp)
                        candidate = cbp_f + TICK_SIZE
                        if abs(candidate - tp_plain) <= EXIT_CLUSTER_MAX_DIFF_TICKS * TICK_SIZE:
                            exit_order_price = candidate
                            exit_order_ts = ts
                            exit_queue_ahead = 0.0
                            exit_reason_target = "cluster_exit"
                            n_exit_orders_placed += 1

            # 4) time-stop режим: держим только maker лимитки на best bid/ask и перевыставляем
            if state == "in_position" and entry_ts is not None and ts - entry_ts >= MAX_HOLD_MS:
                # если осталась кластерная лимитка — отменяем и переходим в time-stop режим
                if exit_order_price is not None and exit_reason_target == "cluster_exit":
                    exit_order_price = None
                    exit_order_ts = None
                    exit_queue_ahead = 0.0
                    exit_reason_target = None
                    n_exit_orders_timeout += 1

                # если сейчас нет активной лимитки — ставим time-stop лимитку по лучшей цене
                if exit_order_price is None:
                    if side == +1:
                        limit_price = bap
                        queue = max(bav - ORDER_SIZE, 0.0)
                    else:
                        limit_price = bbp
                        queue = max(bbv - ORDER_SIZE, 0.0)

                    exit_order_price = limit_price
                    exit_order_ts = ts
                    exit_queue_ahead = queue
                    exit_reason_target = "time_stop_limit"
                    n_exit_orders_placed += 1

            # 5) входные сигналы
            if state == "flat":
                is_bid_entry = (
                    cbv >= bid_entry_thr
                    and cbd is not None
                    and cbd <= MAX_CLUSTER_DEPTH_ENTRY
                )
                is_ask_entry = (
                    cav >= ask_entry_thr
                    and cad is not None
                    and cad <= MAX_CLUSTER_DEPTH_ENTRY
                )

                if is_bid_entry:
                    state = "waiting_entry"
                    side = +1
                    order_price = bbp
                    order_ts = ts
                    entry_order_ts = ts
                    queue_ahead = max(bbv - ORDER_SIZE, 0.0)
                    n_signals += 1
                    n_orders_placed += 1
                elif is_ask_entry:
                    state = "waiting_entry"
                    side = -1
                    order_price = bap
                    order_ts = ts
                    entry_order_ts = ts
                    queue_ahead = max(bav - ORDER_SIZE, 0.0)
                    n_signals += 1
                    n_orders_placed += 1

            i_ob += 1

        else:
            # ===== событие трейда =====
            ts = int(next_tr_ts)
            price = float(tr_price[i_tr])
            size = float(tr_size[i_tr])
            cur_time = pd.Timestamp(tr_time[i_tr])

            last_trade_price = price
            last_time = cur_time

            # 1) исполнение входной лимитки
            if state == "waiting_entry" and order_price is not None:
                if side == +1:
                    if price <= order_price + 1e-9:
                        queue_ahead -= size
                else:
                    if price >= order_price - 1e-9:
                        queue_ahead -= size

                if queue_ahead <= 0.0:
                    state = "in_position"
                    entry_price = order_price
                    entry_ts = ts
                    entry_time = cur_time

                    if side == +1:
                        tp_plain = entry_price + TP_TICKS * TICK_SIZE
                    else:
                        tp_plain = entry_price - TP_TICKS * TICK_SIZE

                    n_filled += 1
                    order_price = None
                    order_ts = None
                    queue_ahead = 0.0

                    exit_order_price = None
                    exit_order_ts = None
                    exit_queue_ahead = 0.0
                    exit_reason_target = None

            # 2) исполнение выходной лимитки (cluster_exit или time_stop_limit)
            if state == "in_position" and entry_price is not None and exit_order_price is not None:
                if side == +1:
                    if price >= exit_order_price - 1e-9:
                        exit_queue_ahead -= size
                else:
                    if price <= exit_order_price + 1e-9:
                        exit_queue_ahead -= size

                if exit_queue_ahead <= 0.0:
                    exit_price = exit_order_price
                    exit_time = cur_time

                    pnl_px = side * (exit_price - entry_price) * ORDER_SIZE

                    # обе стороны как maker + фикс брокера
                    entry_fee = FEE_RATE_MAKER * entry_price * ORDER_SIZE + BROKER_FEE_ABS
                    exit_fee  = FEE_RATE_MAKER * exit_price * ORDER_SIZE + BROKER_FEE_ABS
                    total_fee = entry_fee + exit_fee

                    notional_entry = entry_price * ORDER_SIZE
                    gross_ret = pnl_px / notional_entry
                    net_ret   = (pnl_px - total_fee) / notional_entry

                    gross_ticks = side * (exit_price - entry_price) / TICK_SIZE
                    rel_tick = TICK_SIZE / entry_price
                    net_ticks = net_ret / rel_tick

                    waited_ms = int(entry_ts - entry_order_ts) if (entry_ts is not None and entry_order_ts is not None) else 0

                    reason = exit_reason_target if exit_reason_target is not None else "limit_exit"
                    exit_liq = "maker"
                    exit_from_cluster = (reason == "cluster_exit")

                    trades_res.append(
                        TradeResult(
                            entry_time=entry_time,
                            exit_time=exit_time,
                            side=side,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            reason=reason,
                            gross_ticks=gross_ticks,
                            net_ticks=net_ticks,
                            gross_ret=gross_ret,
                            net_ret=net_ret,
                            waited_ms_for_fill=waited_ms,
                            entry_fee=entry_fee,
                            exit_fee=exit_fee,
                            total_fee=total_fee,
                            exit_liquidity=exit_liq,
                            tp_plain=tp_plain if tp_plain is not None else np.nan,
                            exit_from_cluster=exit_from_cluster,
                        )
                    )

                    # сброс
                    state = "flat"
                    side = 0
                    order_price = None
                    order_ts = None
                    queue_ahead = 0.0
                    entry_price = None
                    entry_ts = None
                    entry_time = None
                    entry_order_ts = None
                    tp_plain = None
                    exit_order_price = None
                    exit_order_ts = None
                    exit_queue_ahead = 0.0
                    exit_reason_target = None

            i_tr += 1

    # ===== EOD: форсированный выход, если что-то осталось =====
    if state == "in_position" and entry_price is not None:
        if last_trade_price is not None:
            exit_price = last_trade_price
        elif last_mid is not None:
            exit_price = last_mid
        else:
            exit_price = entry_price

        exit_time = last_time if last_time is not None else entry_time

        pnl_px = side * (exit_price - entry_price) * ORDER_SIZE

        entry_fee = FEE_RATE_MAKER * entry_price * ORDER_SIZE + BROKER_FEE_ABS
        exit_fee  = FEE_RATE_MAKER * exit_price * ORDER_SIZE + BROKER_FEE_ABS
        total_fee = entry_fee + exit_fee

        notional_entry = entry_price * ORDER_SIZE
        gross_ret = pnl_px / notional_entry
        net_ret   = (pnl_px - total_fee) / notional_entry

        gross_ticks = side * (exit_price - entry_price) / TICK_SIZE
        rel_tick = TICK_SIZE / entry_price
        net_ticks = net_ret / rel_tick

        waited_ms = int(entry_ts - entry_order_ts) if (entry_ts is not None and entry_order_ts is not None) else 0

        trades_res.append(
            TradeResult(
                entry_time=entry_time,
                exit_time=exit_time,
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                reason="eod_force",
                gross_ticks=gross_ticks,
                net_ticks=net_ticks,
                gross_ret=gross_ret,
                net_ret=net_ret,
                waited_ms_for_fill=waited_ms,
                entry_fee=entry_fee,
                exit_fee=exit_fee,
                total_fee=total_fee,
                exit_liquidity="maker",
                tp_plain=tp_plain if tp_plain is not None else np.nan,
                exit_from_cluster=False,
            )
        )

    print(
        f"[BT] signals={n_signals}, orders_placed={n_orders_placed}, "
        f"filled={n_filled}, order_timeouts={n_order_timeouts}"
    )
    print(
        f"[BT] exit_orders_placed={n_exit_orders_placed}, "
        f"exit_orders_timeout={n_exit_orders_timeout}"
    )

    return pd.DataFrame([t.__dict__ for t in trades_res])


# ===== СТАТИСТИКА И ЛОГ =====

def summarize_trades(trades_df: pd.DataFrame):
    if trades_df.empty:
        print("Сделок нет.")
        return

    n = len(trades_df)
    wins = (trades_df["net_ticks"] > 0).sum()
    winrate = wins / n if n > 0 else 0.0

    print("\n===== BACKTEST V8 (cluster + time-stop maker limit + broker fee) =====")
    print(f"Сделок: {n}")
    print(f"Winrate: {winrate:.2%}")
    print(f"Средний PnL (net_ticks): {trades_df['net_ticks'].mean():.3f}")
    print(f"Суммарный PnL (net_ticks): {trades_df['net_ticks'].sum():.3f}")
    total_ret = trades_df["net_ret"].sum()
    print(f"Суммарный net_ret: {total_ret:.5f} (~{total_ret*100:.3f}%)")

    print("\nПо причинам выхода:")
    print(trades_df["reason"].value_counts())

    print("\nТип ликвидности выхода:")
    print(trades_df["exit_liquidity"].value_counts())

    print("\nВремя удержания позиции (сек):")
    hold_sec = (trades_df["exit_time"] - trades_df["entry_time"]).dt.total_seconds()
    print(hold_sec.describe())

    print("\nКомиссии:")
    print("Средний entry_fee:", trades_df["entry_fee"].mean())
    print("Средний exit_fee:", trades_df["exit_fee"].mean())
    print("Суммарный total_fee:", trades_df["total_fee"].sum())


def save_trade_log(trades_df: pd.DataFrame, path: str = "scalp_trades_v8_tradelog.csv"):
    if trades_df.empty:
        print("Журнал не сохранён: нет сделок.")
        return

    log_cols = [
        "entry_time", "exit_time", "side",
        "entry_price", "exit_price",
        "gross_ticks", "net_ticks",
        "gross_ret", "net_ret",
        "reason", "exit_liquidity",
        "tp_plain", "exit_from_cluster",
        "entry_fee", "exit_fee", "total_fee",
    ]
    log_cols = [c for c in log_cols if c in trades_df.columns]
    log_df = trades_df[log_cols].copy()
    log_df["direction"] = log_df["side"].map({+1: "long", -1: "short"}).fillna("")

    ordered = [
        "entry_time", "exit_time",
        "direction", "side",
        "entry_price", "exit_price",
        "gross_ticks", "net_ticks",
        "gross_ret", "net_ret",
        "reason", "exit_liquidity",
        "tp_plain", "exit_from_cluster",
        "entry_fee", "exit_fee", "total_fee",
    ]
    ordered = [c for c in ordered if c in log_df.columns]
    log_df = log_df[ordered]

    log_df.to_csv(path, index=False)
    print(f"\nЖурнал сделок сохранён в {path}")


def main():
    ob = load_orderbook_features(OB_ZIP_PATH, ROWS_TO_PROCESS, MAX_LEVELS)
    tr = load_trades(TRADES_PATH)

    trades_df = backtest_tick_v8(ob, tr)
    summarize_trades(trades_df)

    trades_df.to_csv("scalp_trades_v8_full.csv", index=False)
    print("\nПолный DataFrame сделок сохранён в scalp_trades_v8_full.csv")

    save_trade_log(trades_df, "scalp_trades_v8_tradelog.csv")


if __name__ == "__main__":
    main()
