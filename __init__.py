"""Asar platform plugin for Hermes Agent — точка входа.

Новые версии Hermes берут register() из adapter.py по plugin.yaml, старые (лето 2026) требуют __init__.py.
Адаптер грузим лениво из register(), чтобы discovery оставался лёгким; работает и как пакет, и как одиночный модуль.
"""
import importlib.util
import os


def _load_adapter():
    try:
        from .adapter import register  # пакетная загрузка
        return register
    except ImportError:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "adapter.py")
        spec = importlib.util.spec_from_file_location("asar_platform_adapter", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.register


def register(ctx):
    _load_adapter()(ctx)
