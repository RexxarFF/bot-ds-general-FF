# FunFernus Bot + WebSocket — установка на Bothost

Эта сборка специально подготовлена для Bothost: Discord-бот и realtime WebSocket работают в одном Python-процессе.

## 1. Что загружать

Загрузи содержимое архива `FunFernusBot-Realtime-BOTHOST-FULL.zip` в проект Bothost либо в Git-репозиторий, подключённый к Bothost.

Главный файл: `main.py`.
Зависимости: `requirements.txt`.
Команда запуска: `python main.py`.

## 2. Создай `.env`

Возьми файл `BOTHOST-READY.env`, переименуй его в `.env` и заполни обязательные значения:

- `DISCORD_TOKEN` — токен Discord-бота;
- `GUILD_ID` — ID Discord-сервера;
- `CONFIG_CHANNEL_ID` — ID служебного канала конфигурации;
- `ADMIN_USER_IDS` — Discord ID администраторов через запятую;
- `WEB_DB_HOST`, `WEB_DB_NAME`, `WEB_DB_USER`, `WEB_DB_PASSWORD` — общая MySQL/MariaDB сайта;
- RCON — только если нужен;
- `REALTIME_PUBLIC_URL` — адрес WebSocket-домена Bothost.

`REALTIME_SECRET` в шаблоне намеренно НЕ содержит рабочего секрета. Сгенерируй его командой `python generate_realtime_secret.py` или любым криптографически стойким способом и укажи ОДНО И ТО ЖЕ значение в `.env` бота и приватном `realtime.php` сайта.

ВАЖНО: `.env` нельзя публиковать в GitHub.

## 3. Настройки Bothost

Для web/realtime части нужен опубликованный домен/веб-приложение Bothost.

Python должен слушать:

- host: `0.0.0.0`;
- port: переменная `PORT`, которую задаёт платформа.

В этой сборке `PORT` имеет ПРИОРИТЕТ над `REALTIME_PORT`. Поэтому вручную подгонять код под выданный Bothost порт не нужно.

Fallback `REALTIME_PORT=8000` используется только если `PORT` отсутствует.

## 4. Домен

После публикации домена получишь адрес наподобие:

`https://YOUR-SUBDOMAIN.bothost.tech`

Тогда в `.env`:

`REALTIME_PUBLIC_URL=wss://YOUR-SUBDOMAIN.bothost.tech/ws`

## 5. Настрой сайт

Файл:

`realtime.php.bothost.ready`

положи в приватную конфигурацию сайта как:

`/private/funfernus/realtime.php`

Перед загрузкой замени `YOUR-SUBDOMAIN` в двух местах:

- `public_url` → `wss://YOUR-SUBDOMAIN.bothost.tech/ws`
- `internal_url` → `https://YOUR-SUBDOMAIN.bothost.tech/internal/publish`

В шаблоне стоит placeholder. Перед запуском укажи здесь тот же реальный `REALTIME_SECRET`, что и в `.env` бота. Сам realtime/WebSocket при этом остаётся включённым и работает через `/ws`.

## 6. Установи зависимости

Bothost должен установить зависимости из `requirements.txt`:

- discord.py
- python-dotenv
- PyMySQL
- aiohttp

Если требуется ручная команда:

`pip install -r requirements.txt`

## 7. Запуск

Команда:

`python main.py`

Отдельный Node.js/WebSocket-процесс НЕ нужен.

При запуске один процесс поднимает:

1. Discord Bot;
2. Website Bridge;
3. aiohttp WebSocket/HTTP realtime service.

## 8. Проверка realtime

Открой в браузере:

`https://YOUR-SUBDOMAIN.bothost.tech/health`

или корень домена:

`https://YOUR-SUBDOMAIN.bothost.tech/`

Ожидается JSON с `"ok": true` и `discord_bot_embedded: true`.

В Discord выполни:

`/realtime_status`

## 9. Проверка мессенджера

Открой сайт в двух браузерах/аккаунтах.

1. Пользователь A открывает чат с B.
2. B открывает тот же чат.
3. A отправляет сообщение.
4. У A сообщение появляется сразу optimistic.
5. У B оно должно появиться без F5.
6. Проверь typing, read/delivered, reaction, edit/delete.

## 10. Что делать, если WebSocket не подключается

Проверь по порядку:

1. `/health` открывается через HTTPS.
2. В `.env` `REALTIME_ENABLED=true`.
3. `REALTIME_HOST=0.0.0.0`.
4. Bothost передал `PORT`.
5. `REALTIME_PUBLIC_URL` содержит `wss://`, а не `https://`.
6. `REALTIME_ALLOWED_ORIGINS` содержит точный домен сайта.
7. `REALTIME_SECRET` одинаковый в `.env` и PHP-конфиге.
8. В консоли браузера нет 403 Origin/Ticket ошибок.
9. Сайт реально использует V9 realtime frontend/API.

## 11. Переменные `.env`

### Discord
- `DISCORD_TOKEN`
- `GUILD_ID`
- `CONFIG_CHANNEL_ID`
- `ADMIN_USER_IDS`

### Minecraft/RCON
- `RCON_TEST_MODE`
- `RCON_ENABLED`
- `RCON_HOST`
- `RCON_PORT`
- `RCON_PASSWORD`

### Website Bridge
- `WEB_DB_HOST`
- `WEB_DB_PORT`
- `WEB_DB_NAME`
- `WEB_DB_USER`
- `WEB_DB_PASSWORD`
- `WEB_DB_SSL`
- `WEB_PUBLIC_URL`

### Realtime
- `REALTIME_ENABLED`
- `REALTIME_HOST`
- `REALTIME_PORT`
- `PORT` (выдаётся платформой, имеет приоритет)
- `REALTIME_PATH`
- `REALTIME_SECRET`
- `REALTIME_ALLOWED_ORIGINS`
- `REALTIME_MAX_CONNECTIONS_PER_USER`
- `REALTIME_HEARTBEAT_SECONDS`
- `REALTIME_PUBLIC_URL`

## 12. Безопасность

- Никогда не коммить `.env`.
- Не публикуй `BOTHOST-READY.env` и `realtime.php.bothost.ready`.
- Если архив/секрет утёк, выполни `python generate_realtime_secret.py` и замени секрет и в боте, и на сайте.
- `/internal/publish` защищён общим secret header.
- Браузер не может менять сообщения напрямую через WebSocket: изменения проходят через PHP API и его permissions.
