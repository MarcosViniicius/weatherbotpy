from telegram_bot import notifications


def test_target_chat_ids_parses_multiple_values(monkeypatch):
    monkeypatch.setattr(notifications, "TELEGRAM_CHAT_ID", "123, 456 ,789")

    assert notifications._target_chat_ids() == ["123", "456", "789"]


def test_target_chat_ids_ignores_empty_values(monkeypatch):
    monkeypatch.setattr(notifications, "TELEGRAM_CHAT_ID", "  , 123 ,, ")

    assert notifications._target_chat_ids() == ["123"]
