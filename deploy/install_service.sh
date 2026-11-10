#!/bin/bash
# Script de instalação do serviço Edge Gateway no Systemd (Linux / Raspberry Pi)
set -e

SERVICO="edge-gateway.service"
DIR_DESTINO="/etc/systemd/system"
DIR_ORIGEM="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Instalador do Serviço Edge Gateway Daemon ==="

if [ "$EUID" -ne 0 ]; then
  echo "Erro: Este script precisa ser executado como root (use sudo)."
  exit 1
fi

echo "1. Copiando $SERVICO para $DIR_DESTINO..."
cp "$DIR_ORIGEM/$SERVICO" "$DIR_DESTINO/$SERVICO"
chmod 644 "$DIR_DESTINO/$SERVICO"

echo "2. Recarregando o daemon do systemd..."
systemctl daemon-reload

echo "3. Habilitando inicialização automática no boot..."
systemctl enable "$SERVICO"

echo "4. Iniciando o serviço agora..."
systemctl restart "$SERVICO" || true

echo "=== Instalação concluída com sucesso! ==="
echo "Comandos úteis:"
echo " - Verificar status: sudo systemctl status $SERVICO"
echo " - Visualizar logs:   sudo journalctl -u $SERVICO -f"
echo " - Parar serviço:     sudo systemctl stop $SERVICO"

