import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from crypto_market_pipeline import _validate_record


# =====================================================
# TESTS — _validate_record
# =====================================================

def test_registro_valido():
    payload = {
        "id":              "bitcoin",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   60000,
        "market_cap":      1000000000,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert errors == []


def test_coin_id_nulo():
    payload = {
        "id":              None,
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   60000,
        "market_cap":      1000000000,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert "coin_id is null or empty" in errors


def test_coin_id_vacio():
    payload = {
        "id":              "",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   60000,
        "market_cap":      1000000000,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert "coin_id is null or empty" in errors


def test_precio_cero():
    payload = {
        "id":              "bitcoin",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   0,
        "market_cap":      1000000000,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert any("invalid current_price" in e for e in errors)


def test_precio_negativo():
    payload = {
        "id":              "bitcoin",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   -100,
        "market_cap":      1000000000,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert any("invalid current_price" in e for e in errors)


def test_market_cap_nulo():
    payload = {
        "id":              "bitcoin",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   60000,
        "market_cap":      None,
        "market_cap_rank": 1,
    }
    errors = _validate_record(payload)
    assert any("invalid market_cap" in e for e in errors)


def test_market_cap_rank_cero():
    payload = {
        "id":              "bitcoin",
        "symbol":          "btc",
        "name":            "Bitcoin",
        "current_price":   60000,
        "market_cap":      1000000000,
        "market_cap_rank": 0,
    }
    errors = _validate_record(payload)
    assert any("invalid market_cap_rank" in e for e in errors)


def test_multiples_errores():
    payload = {
        "id":              None,
        "symbol":          None,
        "name":            None,
        "current_price":   None,
        "market_cap":      None,
        "market_cap_rank": None,
    }
    errors = _validate_record(payload)
    assert len(errors) == 6