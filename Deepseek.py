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
from dotenv import load_dotenv  # Добавлен импорт

# Загрузка переменных из .env файла
load_dotenv()

# === Конфигурация сети ===
TESTNET_MODE = os.getenv("TESTNET_MODE", "true").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === Параметры торговли ===
COMMISSION_RATE = 0.001
MAX_PROFIT = 5.0
START_COINS = ['USDT', 'BTC', 'ETH']
LOG_FILE = "trades_log.csv"
TRIANGLE_CACHE = {}
TRIANGLE_HOLD_TIME = 5

# === Лимиты запросов ===
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "15"))
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
    "rateLimit": 300
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

# Безопасная инициализация Telegram
telegram_app = None
if TELEGRAM_TOKEN and TELEGRAM_TOKEN.startswith("5") and TELEGRAM_CHAT_ID:
    try:
        telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()
    except Exception as e:
        print(f"⚠️ Ошибка инициализации Telegram: {str(e)}")
        telegram_app = None

# === Основные функции ===
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
        daily_volume = ticker['quoteVolume']
        
        if volume > daily_volume * 0.01:
            return False
        return True
    except:
        return True

async def load_symbols():
    markets = await exchange.load_markets()
    return list(markets.keys()), markets

async def find_triangles(symbols):
    triangles = []
    for base in START_COINS:
        for sym1 in symbols:
            if not sym1.endswith('/' + base): continue
            mid1 = sym1.split('/')[0]
            for sym2 in symbols:
                if not sym2.startswith(mid1 + '/'): continue
                mid2 = sym2.split('/')[1]
                third = f"{mid2}/{base}"
                if third in symbols or f"{base}/{mid2}" in symbols:
                    triangles.append((base, mid1, mid2))
    return triangles

async def get_avg_price(orderbook_side, target_usdt):
    total_base = 0
    total_usd = 0
    max_liquidity = 0
    for price, volume in orderbook_side:
        price = float(price)
        volume = float(volume)
        usd = price * volume
        max_liquidity += usd
        if total_usd + usd >= target_usdt:
            remain_usd = target_usdt - total_usd
            total_base += remain_usd / price
            total_usd += remain_usd
            break
        else:
            total_base += volume
            total_usd += usd
    if total_usd < target_usdt:
        return None, 0, max_liquidity
    avg_price = total_usd / total_base
    return avg_price, total_usd, max_liquidity

async def get_execution_price(symbol, side, target_usdt):
    try:
        orderbook = await exchange.fetch_order_book(symbol)
        if side == "buy":
            return await get_avg_price(orderbook['asks'], target_usdt)
        else:
            return await get_avg_price(orderbook['bids'], target_usdt)
    except Exception as e:
        if DEBUG_MODE:
            print(f"[Orderbook Error {symbol}]: {e}")
        return None, 0, 0

def format_line(index, pair, price, side, volume_usd, color, liquidity):
    emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(color, "")
    return f"{emoji} {index}. {pair} - {price:.6f} ({side}), исполнено ${volume_usd:.2f}, доступно ${liquidity:.2f}"

async def send_telegram_message(text):
    if not telegram_app:
        if DEBUG_MODE:
            print(f"[Telegram Skipped]: {text[:100]}...")
        return
    
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=text, 
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        if DEBUG_MODE:
            print(f"[Telegram Error]: {e}")

def log_trade(base, mid1, mid2, profit, volume, status, details=""):
    with open(LOG_FILE, "a") as f:
        route = f"{base}->{mid1}->{mid2}->{base}"
        f.write(f"{datetime.utcnow()},{NETWORK_NAME},{route},{profit:.4f},{volume},{status},{details}\n")

async def fetch_balances():
    try:
        balances = await exchange.fetch_balance()
        return {k: float(v) for k, v in balances["total"].items() if float(v) > 0}
    except Exception as e:
        if DEBUG_MODE:
            print(f"[Balance Error]: {e}")
        return {}

async def execute_real_trade(route_id, steps):
    limit_ok, period = await check_rate_limits()
    if not limit_ok:
        return False, f"Превышен лимит сделок за {period}"
    
    for symbol, _, amount in steps:
        if not await check_volume_limits(symbol, amount):
            return False, f"Объем превышает 1% дневного объема для {symbol}"
    
    if TESTNET_MODE:
        test_msg = [
            f"🧪 <b>{NETWORK_NAME}: ТЕСТОВАЯ СДЕЛКА</b>",
            f"Маршрут: {route_id}",
            "Действия:"
        ]
        
        for i, (symbol, side, amount) in enumerate(steps):
            test_msg.append(f"{i+1}. {symbol} {side.upper()} {amount:.6f}")
            
        test_msg.append("\n⚠️ <i>В тестовом режиме сделка не исполняется</i>")
        await send_telegram_message("\n".join(test_msg))
        
        for period in ["minute", "hour", "day"]:
            TRADE_COUNTER[period]["count"] += 1
            
        return True, "Test trade simulated"
    
    try:
        results = []
        for i, (symbol, side, amount) in enumerate(steps):
            market = exchange.market(symbol)
            min_amount = float(market['limits']['amount']['min'])
            
            if amount < min_amount:
                return False, f"Объем меньше минимального: {amount} < {min_amount} для {symbol}"
            
            formatted_amount = float(exchange.amount_to_precision(symbol, amount))
            order = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=formatted_amount
            )
            results.append(order)
            
            if i < len(steps) - 1:
                await asyncio.sleep(0.5)
        
        for period in ["minute", "hour", "day"]:
            TRADE_COUNTER[period]["count"] += 1
            
        return True, results
    except Exception as e:
        return False, str(e)

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"

        price1, vol1, liq1 = await get_execution_price(s1, "buy" if f"{mid1}/{base}" in symbols else "sell", TARGET_VOLUME_USDT)
        if not price1: return
        step1 = (1 / price1 if f"{mid1}/{base}" in symbols else price1) * (1 - COMMISSION_RATE)
        side1 = "ASK" if f"{mid1}/{base}" in symbols else "BID"

        price2, vol2, liq2 = await get_execution_price(s2, "buy" if f"{mid2}/{mid1}" in symbols else "sell", TARGET_VOLUME_USDT)
        if not price2: return
        step2 = (1 / price2 if f"{mid2}/{mid1}" in symbols else price2) * (1 - COMMISSION_RATE)
        side2 = "ASK" if f"{mid2}/{mid1}" in symbols else "BID"

        price3, vol3, liq3 = await get_execution_price(s3, "sell" if f"{mid2}/{base}" in symbols else "buy", TARGET_VOLUME_USDT)
        if not price3: return
        step3 = (price3 if f"{mid2}/{base}" in symbols else 1 / price3) * (1 - COMMISSION_RATE)
        side3 = "BID" if f"{mid2}/{base}" in symbols else "ASK"

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100
        if not (MIN_PROFIT <= profit_percent <= MAX_PROFIT): 
            return

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.utcnow()
        prev_time = TRIANGLE_CACHE.get(route_hash)
        
        if prev_time and (now - prev_time).total_seconds() >= TRIANGLE_HOLD_TIME:
            execute = True
        else:
            TRIANGLE_CACHE[route_hash] = now
            execute = False

        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = round((result - 1) * TARGET_VOLUME_USDT, 2)

        message_lines = [
            f"🔁 <b>{NETWORK_NAME}: Арбитражная возможность</b>",
            format_line(1, s1, price1, side1, vol1, "green", liq1),
            format_line(2, s2, price2, side2, vol2, "yellow", liq2),
            format_line(3, s3, price3, side3, vol3, "red", liq3),
            "",
            f"💰 <b>Чистая прибыль:</b> {pure_profit_usdt:.2f} USDT",
            f"📈 <b>Спред:</b> {profit_percent:.2f}%",
            f"💧 <b>Мин. ликвидность:</b> ${min_liquidity:.2f}",
            f"⚙️ <b>Готов к сделке:</b> {'ДА' if execute else 'НЕТ'}"
        ]

        if DEBUG_MODE:
            print("\n".join(message_lines))

        await send_telegram_message("\n".join(message_lines))
        log_trade(base, mid1, mid2, profit_percent, min_liquidity, "detected")

        if execute:
            base_balance = 0
            if not TESTNET_MODE:
                balances = await fetch_balances()
                base_balance = balances.get(base, 0)
                
                if base_balance < TARGET_VOLUME_USDT:
                    msg = f"⛔ <b>Недостаточно средств</b>\nБаланс {base}: {base_balance:.2f} < {TARGET_VOLUME_USDT}"
                    await send_telegram_message(msg)
                    log_trade(base, mid1, mid2, profit_percent, min_liquidity, "failed", "insufficient_balance")
                    return
            
            steps = []
            expected_prices = []
            
            if f"{mid1}/{base}" in symbols:
                steps.append((s1, "buy", TARGET_VOLUME_USDT))
            else:
                steps.append((s1, "sell", TARGET_VOLUME_USDT))
            expected_prices.append(price1)
            
            amount_mid1 = TARGET_VOLUME_USDT / price1 * (1 - COMMISSION_RATE) if f"{mid1}/{base}" in symbols else TARGET_VOLUME_USDT * price1 * (1 - COMMISSION_RATE)
            if f"{mid2}/{mid1}" in symbols:
                steps.append((s2, "buy", amount_mid1))
            else:
                steps.append((s2, "sell", amount_mid1))
            expected_prices.append(price2)
            
            amount_mid2 = amount_mid1 / price2 * (1 - COMMISSION_RATE) if f"{mid2}/{mid1}" in symbols else amount_mid1 * price2 * (1 - COMMISSION_RATE)
            if f"{mid2}/{base}" in symbols:
                steps.append((s3, "sell", amount_mid2))
            else:
                steps.append((s3, "buy", amount_mid2))
            expected_prices.append(price3)
            
            trade_success, trade_result = await execute_real_trade(route_id, steps)
            
            if trade_success:
                status_msg = "симулирована" if TESTNET_MODE else "выполнена"
                profit_msg = f"Ожидаемая прибыль: {pure_profit_usdt:.2f} USDT"
                
                if not TESTNET_MODE:
                    new_balances = await fetch_balances()
                    new_base_balance = new_balances.get(base, 0)
                    profit_usdt = new_base_balance - base_balance
                    profit_msg = f"Фактическая прибыль: {profit_usdt:.2f} USDT"
                
                msg = [
                    f"✅ <b>{NETWORK_NAME}: Сделка {status_msg}</b>",
                    f"Маршрут: {route_id}",
                    f"Спред: {profit_percent:.2f}%",
                    profit_msg
                ]
                
                if TESTNET_MODE:
                    msg.append("<i>В тестовом режиме реальные ордера не создаются</i>")
                
                await send_telegram_message("\n".join(msg))
                log_status = "simulated" if TESTNET_MODE else "executed"
                log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, log_status)
                
            else:
                msg = f"❌ <b>Ошибка сделки</b>\nМаршрут: {route_id}\nПричина: {trade_result}"
                await send_telegram_message(msg)
                log_trade(base, mid1, mid2, profit_percent, min_liquidity, "failed", trade_result)
    except Exception as e:
        error_msg = f"⚠️ <b>Ошибка при обработке треугольника</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        if DEBUG_MODE:
            import traceback
            traceback.print_exc()

async def send_balance_update():
    try:
        balances = await fetch_balances()
        if not balances:
            return
            
        msg = [f"💰 <b>{NETWORK_NAME} БАЛАНС:</b>"]
        for coin, amount in balances.items():
            if amount > 0.0001:
                msg.append(f"{coin}: {amount:.6f}")
        
        if TESTNET_MODE:
            msg.append("\n⚙️ Для пополнения используйте Bybit Testnet Faucet")
        
        await send_telegram_message("\n".join(msg))
    except Exception as e:
        if DEBUG_MODE:
            print(f"[Balance Update Error]: {e}")

async def check_exchange_connection():
    try:
        server_time = await exchange.fetch_time()
        if DEBUG_MODE:
            print(f"Серверное время: {server_time}")
        return True
    except Exception as e:
        error_msg = f"❌ <b>Ошибка подключения к {NETWORK_NAME}</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        if DEBUG_MODE:
            import traceback
            traceback.print_exc()
        return False

async def main():
    try:
        # Вывод информации о загруженных переменных
        print("="*50)
        print("Загруженные настройки:")
        print(f"Режим сети: {NETWORK_NAME}")
        print(f"Тестовый режим: {TESTNET_MODE}")
        print(f"Объем сделки: {TARGET_VOLUME_USDT} USDT")
        print(f"Минимальная прибыль: {MIN_PROFIT}%")
        print(f"Telegram: {'подключен' if telegram_app else 'отключен'}")
        print("="*50)
        
        if not telegram_app:
            print("⚠️ Telegram не инициализирован. Уведомления отключены")
        
        init_counters()
        
        connected = await check_exchange_connection()
        if not connected:
            return
        
        if telegram_app:
            await telegram_app.initialize()
            await telegram_app.start()
            await send_telegram_message(
                f"♻️ <b>Бот запущен ({NETWORK_NAME})</b>\n"
                f"⚙️ <b>Настройки:</b>\n"
                f"Объем: {TARGET_VOLUME_USDT} USDT\n"
                f"Мин. прибыль: {MIN_PROFIT}%\n"
                f"Лимиты: {MAX_TRADES_PER_MINUTE}/мин, {MAX_TRADES_PER_HOUR}/час, {MAX_TRADES_PER_DAY}/день"
            )
        
        symbols, markets = await load_symbols()
        triangles = await find_triangles(symbols)
        
        if DEBUG_MODE:
            print(f"🔁 Найдено маршрутов: {len(triangles)}")
            await send_telegram_message(f"🔍 Найдено треугольников: {len(triangles)}")

        last_balance_update = time.time()
        last_counter_reset = time.time()
        
        while True:
            tasks = [check_triangle(base, mid1, mid2, symbols, markets) 
                     for base, mid1, mid2 in triangles]
            await asyncio.gather(*tasks)
            
            now = time.time()
            if now - last_counter_reset > 60:
                await check_rate_limits()
                last_counter_reset = now
            
            if now - last_balance_update > 3600:
                await send_balance_update()
                last_balance_update = now
                
            await asyncio.sleep(SCAN_INTERVAL)
            
    except KeyboardInterrupt:
        print("Остановка...")
    except Exception as e:
        error_msg = f"🚨 <b>Критическая ошибка</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        if DEBUG_MODE:
            import traceback
            traceback.print_exc()
    finally:
        try:
            await exchange.close()
            if telegram_app:
                await telegram_app.stop()
                await telegram_app.shutdown()
        except:
            pass

if __name__ == '__main__':
    asyncio.run(main())