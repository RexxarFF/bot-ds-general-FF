<?php
// FunFernus Site -> Bothost realtime
// ВАЖНО: secret должен в точности совпадать с REALTIME_SECRET из .env бота.
return [
    'enabled' => true,
    'public_url' => 'wss://bot-1786364486-7540-rexxarchim123.bothost.tech/ws',
    'internal_url' => 'https://bot-1786364486-7540-rexxarchim123.bothost.tech/internal/publish',
    'secret' => 'PASTE_SAME_REALTIME_SECRET',
];
