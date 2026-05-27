#!/bin/bash
# Установка зависимостей для Solana Token Analyzer

echo "📦 Устанавливаю зависимости..."

pip install git+https://github.com/paoloanzn/free-solscan-api.git requests rich

echo ""
echo "✅ Готово! Примеры использования:"
echo ""
echo "  # Stage 1 — быстрый скоринг по CA"
echo "  python analyze.py 4UD4LVWrg7RxPra1n9nf5P27nrQXiF3kzwwL1NFJQavP"
echo ""
echo "  # Stage 1 + трассировка dev wallet"
echo "  python analyze.py 4UD4LVWrg7RxPra1n9nf5P27nrQXiF3kzwwL1NFJQavP --deep"
echo ""
echo "  # Трассировка кошелька дева MOMUS ($47K вывод)"
echo "  python trace_wallet.py HdcrLZ2HcJkoEpMtw7XQKJ9b7YjHYfsNoiAQb1R1dzaK"
echo ""
echo "  # Второй кошелёк ($12K вывод)"
echo "  python trace_wallet.py 4nbS6VGx2yzPEVPkAPpUhPButrofWnXiyfUvRzfEkBrM"
echo ""
echo "  # Проверить любой CA"
echo "  python analyze.py <CA>"
