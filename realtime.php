<?php
// FunFernus Site -> Bothost realtime
// ВАЖНО: secret должен в точности совпадать с REALTIME_SECRET из .env бота.
return [
    'public_url' => 'wss://YOUR-BOTHOST-DOMAIN/ws',
    'internal_url' => 'https://YOUR-BOTHOST-DOMAIN/internal/publish',
    'secret' => 'PASTE_NEW_64_HEX_REALTIME_SECRET_HERE',
];
