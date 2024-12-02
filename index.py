import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from urllib.parse import urlparse
import re
import threading
import os

# Налаштування Google Sheets API 
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
client = gspread.authorize(creds)

# Telegram Bot Token
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Глобальні змінні
is_parsing = False
sheet = None
user_sheets = {}  # Для збереження вибору документів користувачами

# Перевірка, чи URL є коректним
def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

# Парсинг eBay
def parse_ebay(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    price_div = soup.find("div", class_="x-price-primary")
    price = price_div.find("span", class_="ux-textspans").text.strip() if price_div else "Ціна не знайдена"
    quantity_div = soup.find("div", class_="x-quantity__availability")
    quantity = re.sub(r"\D+", "", quantity_div.find("span", class_="ux-textspans").text).strip() if quantity_div else "<10"
    
    # Видаляємо зайві символи з ціни і замінюємо крапку на кому
    price = re.sub(r"[^\d.]", "", price).replace(".", ",")
    return price, quantity


# Функція для оновлення Google Sheets
def update_sheet(urls):
    """
    Оновлює дані в таблиці Google Sheets, використовуючи масові оновлення пакетами по 20 посилань.
    """
    global is_parsing
    updates = []  # Масив для запису оновлень
    rows = sheet.get_all_values()  # Отримуємо всі поточні значення таблиці

    for i, url in enumerate(urls):
        if not is_valid_url(url):
            continue
        price, quantity = parse_ebay(url)
        print(price, quantity, i)

        # Пошук рядка, де знаходиться посилання
        for row_num, row in enumerate(rows[1:], start=2):  # Пропускаємо заголовки
            if len(row) > 8 and row[8] == url:  # Перевіряємо, чи є стовпець 9 (індекс 8)
                updates.append({
                    "range": f"J{row_num}",  # Ціна у стовпчику 10 (J)
                    "values": [[price]]
                })
                updates.append({
                    "range": f"K{row_num}",  # Кількість у стовпчику 11 (K)
                    "values": [[quantity]]
                })
                break

        # Виконуємо запис кожні 20 оновлень
        if len(updates) >= 40:  # 20 посилань * 2 оновлення (ціна + кількість)
            try:
                # Дебаг: перевіряємо вміст `updates`
                for update_item in updates:
                    if not isinstance(update_item, dict) or "range" not in update_item or "values" not in update_item:
                        print(f"Некоректний формат у {update_item}")

                # Виконання оновлення
                sheet.spreadsheet.values_batch_update({
                    "valueInputOption": "USER_ENTERED",
                    "data": updates
                })
                print(f"Оновлено {len(updates)} комірок у таблиці.")
                updates.clear()  # Очищаємо масив для наступного пакета
            except Exception as e:
                print(f"Помилка під час оновлення таблиці: {e}")

# Обробник для команди /listdocs
async def listdocs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Отримуємо всі доступні документи
        files = client.list_spreadsheet_files()  # gspread надає функцію для отримання списку документів
        if not files:
            await update.message.reply_text("У вас немає доступних документів.")
            return

        # Форматуємо список документів
        docs_list = "\n".join([f"{i+1}. {file['name']}" for i, file in enumerate(files)])
        await update.message.reply_text(f"Доступні документи:\n{docs_list}")
    except Exception as e:
        await update.message.reply_text(f"Не вдалося отримати список документів. Помилка: {e}")

# Обробник для команди /setdoc
async def setdoc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) == 0:
        await update.message.reply_text("Будь ласка, вкажіть назву документа: /setdoc <назва_документа>")
        return

    doc_name = " ".join(context.args)
    try:
        global sheet
        spreadsheet = client.open(doc_name)
        sheet = spreadsheet.sheet1
        user_sheets[update.effective_user.id] = doc_name
        await update.message.reply_text(f"Документ '{doc_name}' успішно вибрано! Використайте команду /parse для початку парсингу.")
    except Exception as e:
        await update.message.reply_text(f"Не вдалося знайти документ '{doc_name}'. Помилка: {e}")

# Обробник для команди /getdoc
async def getdoc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc_name = user_sheets.get(update.effective_user.id)
    if doc_name:
        await update.message.reply_text(f"Поточний документ: '{doc_name}'")
    else:
        await update.message.reply_text("Документ не вибрано. Використовуйте команду /setdoc <назва_документа>.")

# Функція для запуску парсингу
async def start_parsing(context: ContextTypes.DEFAULT_TYPE):
    global is_parsing
    if is_parsing:
        await context.bot.send_message(chat_id=context.job.chat_id, text="Парсинг уже запущено.")
        return

    if sheet is None:
        await context.bot.send_message(chat_id=context.job.chat_id, text="Документ не вибрано. Використовуйте /setdoc для вибору документа.")
        return

    is_parsing = True
    listings = sheet.col_values(9)
    threading.Thread(target=update_sheet, args=(listings,)).start()
    await context.bot.send_message(chat_id=context.job.chat_id, text="Парсинг запущено!")

# Обробник для команди /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вітаю! Я бот для управління парсером. Використовуйте /setdoc, щоб вибрати документ.")

# Обробник для команди /parse
async def parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    job_queue = context.application.job_queue
    job_queue.run_once(start_parsing, 0, chat_id=update.message.chat_id)

# Обробник для команди /status
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_message = "Парсинг виконується." if is_parsing else "Парсер готовий до роботи."
    await update.message.reply_text(status_message)

# Обробник для команди /stop
async def stop_parsing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_parsing
    if is_parsing:
        is_parsing = False
        await update.message.reply_text("Парсинг зупинено.")
    else:
        await update.message.reply_text("Парсер зараз не запущений.")

# Обробник для команди /getdoc
async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Бот має такі команди: \n/help - отримати це повідомлення \n/listdocs - подивитись список доступних таблиць \n/setdoc `назва документу` (без лапок) - встановити активний документ, куди вводяться дані \n/getdoc - отримати обраний документ \n/parse - почати парсинг і записування даних у таблицю \n/status - отримати поточний статус парсері \n/stop - зупинити парсинг")

# Головна функція для запуску бота
def main():
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Налаштування команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setdoc", setdoc))
    application.add_handler(CommandHandler("getdoc", getdoc))
    application.add_handler(CommandHandler("listdocs", listdocs))
    application.add_handler(CommandHandler("parse", parse))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("stop", stop_parsing))
    application.add_handler(CommandHandler("help", help))

    # Запуск бота
    application.run_polling()

if __name__ == "__main__":
    main()