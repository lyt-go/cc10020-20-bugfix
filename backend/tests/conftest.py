"""测试隔离：每个用例结束后把持久化绑定还原到全局存储。

领域服务在导入时绑定 ``json_store`` 名字，部分用例会把 ``app.persistence.json_store``
换成临时目录。只还原 persistence 而不还原服务模块，会让后一个测试文件里的服务继续
写已删除的临时目录，出现“创建成功但查不到”的串扰。
"""
from __future__ import annotations

import sys

import pytest

import app.persistence
from app.services import SERVICES


def _service_modules() -> list:
    modules = {sys.modules[type(svc).__module__] for svc in SERVICES.values()}
    return sorted(modules, key=lambda m: m.__name__)


@pytest.fixture(autouse=True)
def _restore_store_bindings():
    persistence_store = app.persistence.json_store
    baseline = {module: module.json_store for module in _service_modules()}
    yield
    app.persistence.json_store = persistence_store
    for module, store in baseline.items():
        module.json_store = store
