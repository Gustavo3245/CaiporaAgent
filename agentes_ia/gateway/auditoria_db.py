"""Banco de dados SQLite local para auditoria e buffer de sincronização com a nuvem.

Garante persistência transacional (ACID) no nó de borda mesmo em caso de queda de energia
ou perda de conectividade com a nuvem.
Utiliza apenas a biblioteca padrão sqlite3 do Python.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class AuditoriaSQLite:
    """Repositório persistente de auditoria e ações do Gateway de Borda."""

    def __init__(self, caminho_banco: str | Path = "resultados/auditoria.db") -> None:
        self.caminho = Path(caminho_banco)
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._inicializar_banco()

    def _obter_conexao(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.caminho, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _inicializar_banco(self) -> None:
        with self._lock, self._obter_conexao() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auditoria (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    alvo TEXT NOT NULL,
                    falha TEXT,
                    acao TEXT,
                    confianca REAL,
                    decisao TEXT,
                    regra TEXT,
                    motivo TEXT,
                    executada INTEGER NOT NULL,
                    detalhes_json TEXT,
                    sincronizado INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_auditoria_sync ON auditoria(sincronizado, id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_auditoria_alvo ON auditoria(alvo, timestamp);"
            )
            conn.commit()

    def registrar(self, reg: dict[str, Any]) -> int:
        """Registra uma ação de política ou resultado de comando."""
        timestamp = reg.get("timestamp") or datetime.now(timezone.utc).isoformat()
        tipo = reg.get("tipo", "acao_proposta")
        alvo = reg.get("alvo") or reg.get("device_id") or "desconhecido"
        falha = reg.get("falha")
        acao = reg.get("acao")
        confianca = float(reg.get("confianca", 0.0))
        decisao = reg.get("decisao")
        regra = reg.get("regra")
        motivo = reg.get("motivo")
        executada = 1 if reg.get("executada", True) else 0
        detalhes_json = json.dumps(reg, ensure_ascii=False)

        with self._lock, self._obter_conexao() as conn:
            cursor = conn.execute(
                """
                INSERT INTO auditoria (
                    timestamp, tipo, alvo, falha, acao, confianca, decisao,
                    regra, motivo, executada, detalhes_json, sincronizado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0);
                """,
                (
                    timestamp,
                    tipo,
                    alvo,
                    falha,
                    acao,
                    confianca,
                    decisao,
                    regra,
                    motivo,
                    executada,
                    detalhes_json,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def obter_ultimos(self, limite: int = 50) -> list[dict[str, Any]]:
        """Devolve os registros mais recentes."""
        with self._lock, self._obter_conexao() as conn:
            cursor = conn.execute(
                "SELECT * FROM auditoria ORDER BY id DESC LIMIT ?;", (limite,)
            )
            return [dict(linha) for linha in cursor.fetchall()]

    def obter_pendentes_sincronizacao(self, limite: int = 100) -> list[dict[str, Any]]:
        """Devolve registros pendentes de envio para o Coordenador na Nuvem."""
        with self._lock, self._obter_conexao() as conn:
            cursor = conn.execute(
                "SELECT * FROM auditoria WHERE sincronizado = 0 ORDER BY id ASC LIMIT ?;",
                (limite,),
            )
            return [dict(linha) for linha in cursor.fetchall()]

    def marcar_como_sincronizado(self, ids: list[int]) -> None:
        """Marca registros como transmitidos com sucesso para a nuvem."""
        if not ids:
            return
        with self._lock, self._obter_conexao() as conn:
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE auditoria SET sincronizado = 1 WHERE id IN ({placeholders});",
                ids,
            )
            conn.commit()

    def contar_total(self) -> int:
        with self._lock, self._obter_conexao() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM auditoria;")
            return int(cursor.fetchone()[0])

    def contar_pendentes(self) -> int:
        with self._lock, self._obter_conexao() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM auditoria WHERE sincronizado = 0;")
            return int(cursor.fetchone()[0])
