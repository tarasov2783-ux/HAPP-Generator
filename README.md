curl -sL https://raw.githubusercontent.com/tarasov2783-ux/HAPP-Generator/main/install.sh | bash

# HAPP Python port

Python/FastAPI-версия вашего Node.js сервиса с сохранением основной логики и отдельной Python-библиотекой для генерации `happ://crypt...` ссылок.

## Что внутри

- `server.py` — аналог `server.js` на FastAPI
- `happ_crypto.py` — Python-аналог вызова `createHappCryptoLink(..., 'v4', true)`
- `public/` — ваши HTML-файлы без переделки
- `db.json` — JSON-хранилище, совместимое по идее с текущей схемой
- `requirements.txt` — зависимости

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 3000
```

## ENV

```bash
export ADMIN_USER=admin
export ADMIN_PASS=changeme123
```

## Важно

`happ_crypto.py` сейчас делает генерацию через официальный HAPP API. Это самый безопасный путь, чтобы сохранить формат `happ://crypt...` без Node.

Если захотите, следующим шагом можно сделать вторую версию `happ_crypto.py` с локальным fallback-шифрованием под `crypt4`.
