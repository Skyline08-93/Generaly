# triangle_bybit_protected_bot.py
import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import time
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application

# === Конфигурация сети ===
TESTNET_MODE = os.getenv("TESTNET_MODE", "true").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === Параметры торговли ===
COMMISSION_RATE = 0.001
MAX_PROFIT = 5.0
START_COINS = ['USDT']  # Упрощено до USDT для упрощения логики
LOG_FILE = "trades_log.csv"
TRIANGLE_CACHE = {}
TRIANGLE_HOLD_TIME = 5  # Минимальный интервал для одного треугольника

# === Лимиты запросов ===
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "15"))  # Пауза между циклами сканирования
MAX_TRADES_PER_MINUTE = int(os.getenv("MAX_TRADES_PER_MINUTE", "5"))
MAX_TRADES_PER_HOUR = int(os.getenv("MAX_TRADES_PER_HOUR", "30"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "100"))

# Глобальный счетчик сделок
TRADE_COUNTER = {
    "minute": {"count": 0, "reset_time": 0},
    "hour": {"count": 0, "reset_time": 0},
    "day": {"count": 0, "reset_time": 0}
}

# Инициализация счетчиков
def init_counters():
    now = time.time()
    TRADE_COUNTER["minute"] = {"count": 0, "reset_time": now + 60}
    TRADE_COUNTER["hour"] = {"count": 0, "reset_time": now + 3600}
    TRADE_COUNTER["day"] = {"count": 0, "reset_time": now + 86400}

# === Инициализация биржи ===
exchange_config = {
    "enableRateLimit": True,
    "options": {"defaultType": "spot"},
    "rateLimit": 300  # Задержка 300 мс между запросами
}

if TESTNET_MODE:
    exchange_config.update({
        "apiKey": os.getenv("BYBIT_TESTNET_API_KEY"),
        "secret": os.getenv("BYBIT_TESTNET_API_SECRET"),
        "urls": {"api": {
            "public": "https://api-testnet.bybit.com",
            "private": "https://api-testnet.bybit.com"
        }},
    })
    TARGET_VOLUME_USDT = float(os.getenv("TESTNET_TARGET_VOLUME", "10"))
    MIN_PROFIT = float(os.getenv("TESTNET_MIN_PROFIT", "0.01"))
    NETWORK_NAME = "Bybit Testnet"
else:
    exchange_config.update({
        "apiKey": os.getenv("BYBIT_MAINNET_API_KEY"),
        "secret": os.getenv("BYBIT_MAINNET_API_SECRET"),
    })
    TARGET_VOLUME_USDT = float(os.getenv("MAINNET_TARGET_VOLUME", "100"))
    MIN_PROFIT = float(os.getenv("MAINNET_MIN_PROFIT", "0.1"))
    NETWORK_NAME = "Bybit Mainnet"

exchange = ccxt.bybit(exchange_config)

# Инициализация файла лога
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as f:
        f.write("timestamp,network,route,profit_percent,volume_usdt,status,details\n")

# Инициализация счетчиков сделок
init_counters()

# Инициализация бота Telegram
telegram_app = None
if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
    telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

async def log_debug(message):
    """Логирование отладочной информации"""
    if DEBUG_MODE:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[DEBUG {timestamp}] {message}")
        if telegram_app:
            try:
                await telegram_app.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=f"<code>[DEBUG] {message}</code>",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass

async def check_rate_limits():
    """Проверяет и сбрасывает счетчики лимитов"""
    now = time.time()
    for period in ["minute", "hour", "day"]:
        if now > TRADE_COUNTER[period]["reset_time"]:
            TRADE_COUNTER[period]["count"] = 0
            TRADE_COUNTER[period]["reset_time"] = now + (
                60 if period == "minute" else 
                3600 if period == "hour" else 
                86400
            )

    if TRADE_COUNTER["minute"]["count"] >= MAX_TRADES_PER_MINUTE:
        return False, "minute"
    if TRADE_COUNTER["hour"]["count"] >= MAX_TRADES_PER_HOUR:
        return False, "hour"
    if TRADE_COUNTER["day"]["count"] >= MAX_TRADES_PER_DAY:
        return False, "day"
    
    return True, None

async def check_volume_limits(symbol, volume):
    """Проверяет объем сделки относительно дневного объема"""
    try:
        if TESTNET_MODE:
            return True
            
        ticker = await exchange.fetch_ticker(symbol)
        daily_volume = ticker['quoteVolume']  # Объем в USDT
        
        # Не более 1% от дневного объема
        if volume > daily_volume * 0.01:
            return False
        return True
    except Exception as e:
        await log_debug(f"Volume limit check failed: {str(e)}")
        return True  # В случае ошибки пропускаем проверку

async def load_symbols():
    try:
        await log_debug("Loading markets...")
        markets = await exchange.load_markets()
        symbols = [symbol for symbol, market in markets.items() if market['active']]
        await log_debug(f"Loaded {len(symbols)} active symbols")
        return symbols, markets
    except Exception as e:
        await log_debug(f"Market load error: {str(e)}")
        return [], {}

async def find_triangles(symbols):
    triangles = []
    symbol_set = set(symbols)
    
    for base in START_COINS:
        base_pairs = [s for s in symbols if base in s]
        await log_debug(f"Found {len(base_pairs)} pairs for {base}")
        
        for s1 in base_pairs:
            currencies = s1.split('/')
            if base == currencies[0]:
                mid1 = currencies[1]
            else:
                mid1 = currencies[0]
                
            for s2 in symbols:
                if s1 == s2:
                    continue
                    
                if mid1 in s2:
                    currencies2 = s2.split('/')
                    if mid1 == currencies2[0]:
                        mid2 = currencies2[1]
                    else:
                        mid2 = currencies2[0]
                        
                    s3 = f"{mid2}/{base}"
                    if s3 in symbol_set:
                        triangles.append((base, mid1, mid2))
                        await log_debug(f"Triangle found: {base}->{mid1}->{mid2}")
                    else:
                        s3 = f"{base}/{mid2}"
                        if s3 in symbol_set:
                            triangles.append((base, mid1, mid2))
                            await log_debug(f"Triangle found: {base}->{mid1}->{mid2}")
    
    await log_debug(f"Total triangles: {len(triangles)}")
    return list(set(triangles))

async def get_avg_price(orderbook_side, target_amount):
    total_base = 0
    total_quote = 0
    for price, volume in orderbook_side:
        price = float(price)
        volume = float(volume)
        quote_amount = price * volume
        
        if total_quote + quote_amount >= target_amount:
            remaining = target_amount - total_quote
            volume_used = remaining / price
            total_base += volume_used
            total_quote += remaining
            break
        else:
            total_base += volume
            total_quote += quote_amount
    
    if total_quote < target_amount:
        return None, total_quote, total_quote
    
    avg_price = total_quote / total_base
    return avg_price, total_quote, total_quote

async def get_execution_price(symbol, side, target_amount):
    try:
        await log_debug(f"Fetching orderbook for {symbol}")
        orderbook = await exchange.fetch_order_book(symbol, limit=20)
        
        if side == "buy":
            return await get_avg_price(orderbook['asks'], target_amount)
        else:
            return await get_avg_price(orderbook['bids'], target_amount)
    except Exception as e:
        await log_debug(f"Orderbook error for {symbol}: {str(e)}")
        return None, 0, 0

async def send_telegram_message(text):
    if not telegram_app or not TELEGRAM_CHAT_ID:
        return
        
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=text, 
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        await log_debug(f"Telegram send error: {str(e)}")

def log_trade(base, mid1, mid2, profit, volume, status, details=""):
    try:
        with open(LOG_FILE, "a") as f:
            route = f"{base}->{mid1}->{mid2}->{base}"
            f.write(f"{datetime.utcnow()},{NETWORK_NAME},{route},{profit:.4f},{volume},{status},{details}\n")
    except Exception as e:
        print(f"Log error: {str(e)}")

async def fetch_balances():
    try:
        await log_debug("Fetching balances...")
        balances = await exchange.fetch_balance()
        return {k: float(v) for k, v in balances["total"].items() if float(v) > 0.000001}
    except Exception as e:
        await log_debug(f"Balance error: {str(e)}")
        return {}

async def execute_real_trade(route_id, steps):
    """Выполняет торговые операции с защитой"""
    # Проверка лимитов
    limit_ok, period = await check_rate_limits()
    if not limit_ok:
        return False, f"Rate limit exceeded for {period}"

    # Проверка объемов для каждого шага
    for symbol, _, amount in steps:
        if not await check_volume_limits(symbol, amount):
            return False, f"Volume exceeds 1% daily limit for {symbol}"
    
    if TESTNET_MODE:
        # Симуляция для тестовой сети
        test_msg = [
            f"🧪 <b>{NETWORK_NAME}: TEST TRADE</b>",
            f"Route: {route_id}",
            "Steps:"
        ]
        
        for i, (symbol, side, amount) in enumerate(steps):
            test_msg.append(f"{i+1}. {symbol} {side.upper()} {amount:.6f}")
            
        test_msg.append("\n⚠️ <i>No real execution in test mode</i>")
        await send_telegram_message("\n".join(test_msg))
        
        # Обновляем счетчики даже в тестовом режиме
        for period in ["minute", "hour", "day"]:
            TRADE_COUNTER[period]["count"] += 1
            
        return True, "Test trade simulated"
    
    try:
        # Реальная сделка
        results = []
        for i, (symbol, side, amount) in enumerate(steps):
            market = exchange.market(symbol)
            min_amount = float(market['limits']['amount']['min'])
            
            if amount < min_amount:
                return False, f"Amount below min: {amount} < {min_amount} for {symbol}"
            
            formatted_amount = float(exchange.amount_to_precision(symbol, amount))
            await log_debug(f"Creating {side} order for {symbol}: {formatted_amount}")
            order = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=formatted_amount
            )
            results.append(order)
            await log_debug(f"Order executed: {order['id']}")
            
            # Пауза между шагами
            if i < len(steps) - 1:
                await asyncio.sleep(1)
        
        # Обновляем счетчики после успешной сделки
        for period in ["minute", "hour", "day"]:
            TRADE_COUNTER[period]["count"] += 1
            
        return True, results
    except Exception as e:
        await log_debug(f"Trade execution failed: {str(e)}")
        return False, str(e)

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"

        # Проверка существования пар
        if s1 not in markets or s2 not in markets or s3 not in markets:
            return

        # Шаг 1
        side1 = "buy" if f"{mid1}/{base}" in symbols else "sell"
        price1, vol1, liq1 = await get_execution_price(s1, side1, TARGET_VOLUME_USDT)
        if price1 is None:
            await log_debug(f"Price1 not available for {s1}")
            return
            
        step1 = (1 / price1 if f"{mid1}/{base}" in symbols else price1) * (1 - COMMISSION_RATE)
        amount_after_step1 = TARGET_VOLUME_USDT / price1 if side1 == "buy" else TARGET_VOLUME_USDT * price1
        amount_after_step1 *= (1 - COMMISSION_RATE)

        # Шаг 2
        side2 = "buy" if f"{mid2}/{mid1}" in symbols else "sell"
        price2, vol2, liq2 = await get_execution_price(s2, side2, amount_after_step1)
        if price2 is None:
            await log_debug(f"Price2 not available for {s2}")
            return
            
        step2 = (1 / price2 if f"{mid2}/{mid1}" in symbols else price2) * (1 - COMMISSION_RATE)
        amount_after_step2 = amount_after_step1 / price2 if side2 == "buy" else amount_after_step1 * price2
        amount_after_step2 *= (1 - COMMISSION_RATE)

        # Шаг 3
        side3 = "sell" if f"{mid2}/{base}" in symbols else "buy"
        price3, vol3, liq3 = await get_execution_price(s3, side3, amount_after_step2)
        if price3 is None:
            await log_debug(f"Price3 not available for {s3}")
            return
            
        step3 = (price3 if f"{mid2}/{base}" in symbols else 1 / price3) * (1 - COMMISSION_RATE)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100
        
        await log_debug(f"Triangle {base}-{mid1}-{mid2}: Profit={profit_percent:.2f}%")
        
        if not (MIN_PROFIT <= profit_percent <= MAX_PROFIT): 
            return

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.utcnow()
        prev_time = TRIANGLE_CACHE.get(route_hash)
        
        if prev_time and (now - prev_time).total_seconds() < TRIANGLE_HOLD_TIME:
            execute = False
        else:
            TRIANGLE_CACHE[route_hash] = now
            execute = True

        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = round((result - 1) * TARGET_VOLUME_USDT, 2)

        message_lines = [
            f"🔁 <b>{NETWORK_NAME}: Arbitrage Opportunity</b>",
            f"🔄 Route: {route_id}",
            f"1. {s1} {side1.upper()} @ {price1:.6f}",
            f"2. {s2} {side2.upper()} @ {price2:.6f}",
            f"3. {s3} {side3.upper()} @ {price3:.6f}",
            "",
            f"💰 <b>Profit:</b> {pure_profit_usdt:.2f} USDT",
            f"📈 <b>Spread:</b> {profit_percent:.2f}%",
            f"💧 <b>Min Liquidity:</b> ${min_liquidity:.2f}",
            f"⚙️ <b>Ready:</b> {'YES' if execute else 'NO'}"
        ]

        await send_telegram_message("\n".join(message_lines))
        log_trade(base, mid1, mid2, profit_percent, min_liquidity, "detected")

        if execute:
            # Проверка баланса в основной сети
            base_balance = 0
            if not TESTNET_MODE:
                balances = await fetch_balances()
                base_balance = balances.get(base, 0)
                
                if base_balance < TARGET_VOLUME_USDT:
                    msg = f"⛔ <b>Insufficient funds</b>\n{base} balance: {base_balance:.2f} < {TARGET_VOLUME_USDT}"
                    await send_telegram_message(msg)
                    log_trade(base, mid1, mid2, profit_percent, min_liquidity, "failed", "insufficient_balance")
                    return
            
            steps = []
            
            # Шаг 1: base -> mid1
            if f"{mid1}/{base}" in symbols:
                steps.append((s1, "buy", TARGET_VOLUME_USDT))
            else:
                steps.append((s1, "sell", TARGET_VOLUME_USDT))
            
            # Шаг 2: mid1 -> mid2
            amount_mid1 = TARGET_VOLUME_USDT / price1 * (1 - COMMISSION_RATE) if f"{mid1}/{base}" in symbols else TARGET_VOLUME_USDT * price1 * (1 - COMMISSION_RATE)
            if f"{mid2}/{mid1}" in symbols:
                steps.append((s2, "buy", amount_mid1))
            else:
                steps.append((s2, "sell", amount_mid1))
            
            # Шаг 3: mid2 -> base
            amount_mid2 = amount_mid1 / price2 * (1 - COMMISSION_RATE) if f"{mid2}/{mid1}" in symbols else amount_mid1 * price2 * (1 - COMMISSION_RATE)
            if f"{mid2}/{base}" in symbols:
                steps.append((s3, "sell", amount_mid2))
            else:
                steps.append((s3, "buy", amount_mid2))
            
            # Выполнение сделки
            trade_success, trade_result = await execute_real_trade(route_id, steps)
            
            if trade_success:
                status_msg = "simulated" if TESTNET_MODE else "executed"
                profit_msg = f"Expected profit: {pure_profit_usdt:.2f} USDT"
                
                if not TESTNET_MODE:
                    new_balances = await fetch_balances()
                    new_base_balance = new_balances.get(base, 0)
                    profit_usdt = new_base_balance - base_balance
                    profit_msg = f"Actual profit: {profit_usdt:.2f} USDT"
                
                msg = [
                    f"✅ <b>{NETWORK_NAME}: Trade {status_msg}</b>",
                    f"Route: {route_id}",
                    f"Spread: {profit_percent:.2f}%",
                    profit_msg
                ]
                
                await send_telegram_message("\n".join(msg))
                log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, status_msg)
            else:
                msg = f"❌ <b>Trade failed</b>\nRoute: {route_id}\nReason: {trade_result}"
                await send_telegram_message(msg)
                log_trade(base, mid1, mid2, profit_percent, min_liquidity, "failed", trade_result)
    except Exception as e:
        error_msg = f"⚠️ <b>Triangle processing error</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        await log_debug(f"Triangle error: {str(e)}")

async def send_balance_update():
    """Отправляет текущий баланс в Telegram"""
    try:
        balances = await fetch_balances()
        if not balances:
            return
            
        msg = [f"💰 <b>{NETWORK_NAME} BALANCE:</b>"]
        for coin, amount in balances.items():
            if amount > 0.0001:
                msg.append(f"{coin}: {amount:.6f}")
        
        if TESTNET_MODE:
            msg.append("\n⚙️ Use Bybit Testnet Faucet for funding")
        
        await send_telegram_message("\n".join(msg))
    except Exception as e:
        await log_debug(f"Balance update error: {str(e)}")

async def check_exchange_connection():
    """Проверяет подключение к бирже"""
    try:
        await log_debug("Checking exchange connection...")
        server_time = await exchange.fetch_time()
        await log_debug(f"Exchange time: {server_time}")
        return True
    except Exception as e:
        error_msg = f"❌ <b>Connection error to {NETWORK_NAME}</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        await log_debug(f"Connection error: {str(e)}")
        return False

async def main_loop():
    """Основной цикл работы бота"""
    await log_debug("Bot starting...")
    
    # Инициализация счетчиков
    init_counters()
    
    # Проверка подключения к бирже
    connected = await check_exchange_connection()
    if not connected:
        return
        
    await send_telegram_message(f"🤖 <b>Bot started ({NETWORK_NAME})</b>")
    
    symbols, markets = await load_symbols()
    if not symbols:
        await send_telegram_message("⚠️ <b>No trading symbols found!</b>")
        return
        
    triangles = await find_triangles(symbols)
    if not triangles:
        await send_telegram_message("⚠️ <b>No arbitrage triangles found!</b>")
    
    if telegram_app:
        await telegram_app.initialize()
        await telegram_app.start()

    last_balance_update = time.time()
    last_counter_reset = time.time()
    
    while True:
        try:
            await log_debug("Starting scan cycle...")
            start_time = time.time()
            
            # Сбрасываем счетчики при необходимости
            now = time.time()
            if now - last_counter_reset > 60:
                await check_rate_limits()
                last_counter_reset = now
            
            # Проверяем все треугольники
            for triangle in triangles:
                try:
                    await check_triangle(*triangle, symbols, markets)
                except Exception as e:
                    await log_debug(f"Triangle error: {str(e)}")
                await asyncio.sleep(0.5)  # Задержка между треугольниками
                
            # Отправляем баланс каждый час
            if now - last_balance_update > 3600:
                await send_balance_update()
                last_balance_update = now
                
            cycle_time = time.time() - start_time
            await log_debug(f"Scan cycle completed in {cycle_time:.2f}s")
            await asyncio.sleep(max(1, SCAN_INTERVAL - cycle_time))
            
        except Exception as e:
            await log_debug(f"Main loop error: {str(e)}")
            await asyncio.sleep(30)

async def shutdown():
    """Корректное завершение работы"""
    try:
        await exchange.close()
        if telegram_app:
            await telegram_app.stop()
            await telegram_app.shutdown()
    except Exception as e:
        print(f"Shutdown error: {str(e)}")

async def main():
    """Точка входа в приложение"""
    try:
        await main_loop()
    except KeyboardInterrupt:
        print("Stopping...")
    except Exception as e:
        error_msg = f"🚨 <b>Critical error</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        await log_debug(f"Critical error: {str(e)}")
    finally:
        await shutdown()

if __name__ == '__main__':
    asyncio.run(main())