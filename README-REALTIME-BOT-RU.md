# FunFernus Discord Bot + Realtime WebSocket

Актуальная сборка подготовлена для Bothost и обычных VPS.

Для Bothost начинай с файла:

`BOTHOST-INSTALL-RU.md`

Главное:

- один `python main.py` запускает Discord-бота и WebSocket;
- общий конфиг всего бота — `.env`;
- шаблон — `.env.example`;
- полуготовый файл со сгенерированным realtime-secret — `BOTHOST-READY.env`;
- Bothost `PORT` автоматически имеет приоритет над `REALTIME_PORT`;
- сервер слушает `0.0.0.0` для container/reverse-proxy hosting;
- `/` и `/health` возвращают health JSON;
- WebSocket route — `/ws`;
- publish endpoint — `/internal/publish`;
- сайт использует `realtime.php.bothost.ready`.

Отдельный Node.js realtime-server больше не нужен.
