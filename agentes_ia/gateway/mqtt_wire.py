"""Codificação e decodificação binária do protocolo MQTT 3.1.1 (biblioteca padrão)."""

from __future__ import annotations

import socket
import struct

PKT_CONNECT = 1
PKT_CONNACK = 2
PKT_PUBLISH = 3
PKT_PUBACK = 4
PKT_SUBSCRIBE = 8
PKT_SUBACK = 9
PKT_UNSUBSCRIBE = 10
PKT_UNSUBACK = 11
PKT_PINGREQ = 12
PKT_PINGRESP = 13
PKT_DISCONNECT = 14


def codificar_tamanho_restante(tamanho: int) -> bytes:
    """Codifica o Remaining Length conforme especificação MQTT 3.1.1."""
    saida = bytearray()
    while True:
        byte = tamanho % 128
        tamanho //= 128
        if tamanho > 0:
            byte |= 0x80
        saida.append(byte)
        if tamanho == 0:
            break
    return bytes(saida)


def ler_exato(sock: socket.socket, n: int) -> bytes:
    """Lê exatamente n bytes do socket ou levanta EOFError."""
    buf = bytearray()
    while len(buf) < n:
        pedaco = sock.recv(n - len(buf))
        if not pedaco:
            raise EOFError("Conexão encerrada pelo peer")
        buf.extend(pedaco)
    return bytes(buf)


def ler_pacote(sock: socket.socket) -> tuple[int, int, bytes]:
    """Lê um pacote MQTT 3.1.1 completo: (tipo, flags, payload_completo)."""
    primeiro = ler_exato(sock, 1)[0]
    tipo = (primeiro >> 4) & 0x0F
    flags = primeiro & 0x0F

    multiplicador = 1
    tamanho = 0
    while True:
        byte = ler_exato(sock, 1)[0]
        tamanho += (byte & 0x7F) * multiplicador
        if (byte & 0x80) == 0:
            break
        multiplicador *= 128
        if multiplicador > 128 * 128 * 128:
            raise ValueError("Remaining Length inválido no cabeçalho MQTT")

    corpo = ler_exato(sock, tamanho) if tamanho > 0 else b""
    return tipo, flags, corpo


def codificar_str(texto: str | bytes) -> bytes:
    """Codifica string UTF-8 ou bytes com prefixo de tamanho de 2 bytes big-endian."""
    dados = texto.encode("utf-8") if isinstance(texto, str) else texto
    return struct.pack("!H", len(dados)) + dados


def ler_str(buf: bytes, offset: int) -> tuple[str, int]:
    """Lê uma string UTF-8 prefixada por 2 bytes a partir de offset."""
    dados, novo_offset = ler_bytes(buf, offset)
    return dados.decode("utf-8"), novo_offset


def ler_bytes(buf: bytes, offset: int) -> tuple[bytes, int]:
    """Lê bloco binário prefixado por 2 bytes a partir de offset."""
    if offset + 2 > len(buf):
        raise ValueError("Buffer insuficiente para ler tamanho do bloco MQTT")
    (tam,) = struct.unpack("!H", buf[offset : offset + 2])
    offset += 2
    if offset + tam > len(buf):
        raise ValueError("Buffer insuficiente para ler bloco MQTT")
    return buf[offset : offset + tam], offset + tam


def montar_connect(
    client_id: str,
    keepalive: int = 30,
    clean_session: bool = True,
    will: tuple[str, bytes, int, bool] | None = None,
) -> bytes:
    """Constrói pacote CONNECT MQTT 3.1.1 com suporte opcional a LWT."""
    flags = 0x02 if clean_session else 0x00
    payload = bytearray(codificar_str(client_id))

    if will is not None:
        w_topic, w_payload, w_qos, w_retain = will
        flags |= 0x04 | ((w_qos & 0x03) << 3) | (0x20 if w_retain else 0x00)
        payload.extend(codificar_str(w_topic))
        payload.extend(codificar_str(w_payload))

    var_header = b"\x00\x04MQTT\x04" + struct.pack("!BH", flags, keepalive)
    corpo = var_header + bytes(payload)
    return bytes([PKT_CONNECT << 4]) + codificar_tamanho_restante(len(corpo)) + corpo


def montar_publish(
    topico: str, payload: bytes, qos: int = 0, retain: bool = False, packet_id: int = 1
) -> bytes:
    """Constrói um pacote PUBLISH MQTT 3.1.1 completo."""
    flags = ((qos & 0x03) << 1) | (0x01 if retain else 0x00)
    header_var = bytearray(codificar_str(topico))
    if qos > 0:
        header_var.extend(struct.pack("!H", packet_id))
    corpo = bytes(header_var) + payload
    return bytes([(PKT_PUBLISH << 4) | flags]) + codificar_tamanho_restante(len(corpo)) + corpo


def montar_subscribe(packet_id: int, topico: str, qos: int = 1) -> bytes:
    """Constrói pacote SUBSCRIBE MQTT 3.1.1."""
    corpo = struct.pack("!H", packet_id) + codificar_str(topico) + bytes([qos & 0x03])
    return bytes([(PKT_SUBSCRIBE << 4) | 0x02]) + codificar_tamanho_restante(len(corpo)) + corpo
