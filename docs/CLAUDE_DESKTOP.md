# Підключення mcp_mtproto до Claude Desktop

Перед цим виконайте кроки 1–5 з [README](../README.md): мають бути встановлені залежності, заповнений `.env` і створений файл `data/session.session` (вхід у Telegram). Claude Desktop запускає сервер сам і не вміє питати код із Telegram, тому вхід обов'язково робиться заздалегідь, у терміналі.

## 1. Дізнайтеся повні шляхи

Вам потрібні два шляхи — до Python із віртуального середовища та до папки проєкту.

**macOS** (у терміналі, всередині папки проєкту):

```
pwd
```

Припустимо, виведено `/Users/olena/mcp_mtproto`. Тоді:
- папка проєкту: `/Users/olena/mcp_mtproto`
- Python: `/Users/olena/mcp_mtproto/.venv/bin/python`

**Windows** (у PowerShell, всередині папки проєкту):

```
(Get-Location).Path
```

Припустимо, виведено `C:\Users\Olena\mcp_mtproto`. Тоді:
- папка проєкту: `C:\Users\Olena\mcp_mtproto`
- Python: `C:\Users\Olena\mcp_mtproto\.venv\Scripts\python.exe`

Шляхи мають бути **повними** (не `~`, не `.`), інакше Claude Desktop не знайде файли.

## 2. Відкрийте конфіг Claude Desktop

Найпростіше: Claude Desktop → **Settings** (Налаштування) → **Developer** → **Edit Config**. Відкриється папка з файлом `claude_desktop_config.json`. Якщо файлу немає, створіть його.

Де він лежить:

| ОС | Шлях |
|---|---|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |

(Якщо Claude Desktop на Windows встановлено з Microsoft Store, файл може лежати в `%LOCALAPPDATA%\Packages\Claude_…\LocalCache\Roaming\Claude\`. Надійніше відкривати його саме через Settings → Developer → Edit Config.)

## 3. Додайте сервер

### macOS

```json
{
  "mcpServers": {
    "mcp-mtproto": {
      "command": "/Users/olena/mcp_mtproto/.venv/bin/python",
      "args": ["-m", "app.server"],
      "env": {
        "PYTHONPATH": "/Users/olena/mcp_mtproto"
      }
    }
  }
}
```

### Windows

У JSON зворотний слеш треба подвоювати (`\\`). Або використовуйте прямі слеші `/` — вони теж працюють.

```json
{
  "mcpServers": {
    "mcp-mtproto": {
      "command": "C:\\Users\\Olena\\mcp_mtproto\\.venv\\Scripts\\python.exe",
      "args": ["-m", "app.server"],
      "env": {
        "PYTHONPATH": "C:\\Users\\Olena\\mcp_mtproto"
      }
    }
  }
}
```

Замініть `/Users/olena/mcp_mtproto` та `C:\\Users\\Olena\\mcp_mtproto` на свої шляхи з кроку 1.

Якщо в конфігу вже є інші сервери, не замінюйте весь файл: додайте `"mcp-mtproto": { ... }` всередину наявного блоку `mcpServers`, не забуваючи про кому між записами.

Ключі `TELEGRAM_API_ID` і `TELEGRAM_API_HASH` у конфіг вписувати не треба: сервер сам читає їх із `.env` у папці проєкту.

## 4. Повністю перезапустіть Claude Desktop

Просто закрити вікно недостатньо — програма має вийти повністю:

- **macOS:** `Cmd+Q` (або Claude → Quit Claude).
- **Windows:** правою кнопкою на значку Claude в системному треї → **Quit**; потім запустіть програму знову.

## 5. Перевірте підключення

1. У новому чаті поряд з полем введення з'явиться значок інструментів (або «Search and tools»). Відкрийте його: у списку має бути `mcp-mtproto` з інструментами `list_dialogs`, `get_history`, `download_media` тощо.
2. Напишіть: «Покажи мої 10 останніх чатів у Telegram». Claude попросить дозвіл викликати інструмент `list_dialogs`; дозвольте.

Якщо Claude показує ваші чати, усе працює.

## Якщо не працює

Подивіться журнал сервера:

| ОС | Де журнал |
|---|---|
| macOS | `~/Library/Logs/Claude/mcp-server-mcp-mtproto.log` (і загальний `mcp.log`) |
| Windows | `%APPDATA%\Claude\logs\mcp-server-mcp-mtproto.log` |

Типові причини:

| У журналі / симптом | Причина й рішення |
|---|---|
| сервера немає в списку інструментів | Помилка в JSON (зайва/відсутня кома чи лапка) або Claude Desktop не перезапущено повністю. Перевірте файл у будь-якому JSON-валідаторі й перезапустіть програму через Quit. |
| `No such file or directory` / «не удается найти указанный файл» | Неправильний шлях до Python у `command`. На Windows перевірте подвоєні `\\`. |
| `ModuleNotFoundError: No module named 'app'` | Не вказано `PYTHONPATH` або він веде не в корінь проєкту. |
| `ModuleNotFoundError: telethon` / `mcp` | Залежності встановлено не в те середовище: виконайте `pip install -r requirements.txt` саме з `.venv` (крок 3 README) і вкажіть у `command` Python із цього `.venv`. |
| «Сессия не найдена» | Не виконано вхід: запустіть `python -m app.auth` (крок 5 README) із папки проєкту. |
| «TELEGRAM_API_ID / TELEGRAM_API_HASH не заданы» | Порожній або відсутній `.env` у корені проєкту. |

Щоб побачити помилку на власні очі, запустіть у терміналі ту саму команду, що в конфігу:

- macOS: `PYTHONPATH=/Users/olena/mcp_mtproto /Users/olena/mcp_mtproto/.venv/bin/python -m app.server`
- Windows (PowerShell): `$env:PYTHONPATH="C:\Users\Olena\mcp_mtproto"; C:\Users\Olena\mcp_mtproto\.venv\Scripts\python.exe -m app.server`

Якщо сервер запустився й мовчить, помилка не в ньому, а в конфігу Claude Desktop.

## Оновлення й видалення

- **Оновити:** у папці проєкту `git pull`, потім `pip install -r requirements.txt` у віртуальному середовищі, потім повністю перезапустіть Claude Desktop.
- **Вимкнути:** видаліть запис `mcp-mtproto` із `claude_desktop_config.json` і перезапустіть програму.
- **Повністю прибрати доступ до акаунта:** видаліть файл `data/session.session` і завершіть сеанс у Telegram → Налаштування → Пристрої.
