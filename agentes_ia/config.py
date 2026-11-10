import copy
import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent


def mesclar(base: dict, over: dict) -> dict:
    """Mescla recursivamente 'over' sobre 'base' (sem alterar os originais)."""
    r = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(r.get(k), dict):
            r[k] = mesclar(r[k], v)
        else:
            r[k] = copy.deepcopy(v)
    return r


def carregar(caminho=None, over=None) -> dict:
    with open(caminho or RAIZ / "config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    return mesclar(cfg, over or {})


def carregar_politicas(caminho=None) -> dict:
    with open(caminho or RAIZ / "politicas.json", encoding="utf-8") as f:
        return json.load(f)
