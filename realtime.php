<?php
// FunFernus Site -> Bothost realtime
// ВАЖНО: secret должен в точности совпадать с REALTIME_SECRET из .env бота.
return [
    'enabled' => true,
    'public_url' => 'wss://YOUR-SUBDOMAIN.bothost.tech/ws',
    'internal_url' => 'https://YOUR-SUBDOMAIN.bothost.tech/internal/publish',
    'secret' => 'PASTE_THE_SAME_NEW_REALTIME_SECRET_HERE',
];
