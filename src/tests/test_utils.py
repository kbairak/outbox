import pytest

from outbox.utils import parse_duration


def test_parse_duration_zeros() -> None:
    assert parse_duration("0") == 0
    assert parse_duration("0ms") == 0
    assert parse_duration("0s") == 0
    assert parse_duration("0m") == 0
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("0h")


def test_parse_duration_milliseconds() -> None:
    assert parse_duration("1ms") == 1
    assert parse_duration("123ms") == 123
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("1234ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("01ms")


def test_parse_duration_seconds() -> None:
    assert parse_duration("1s") == 1_000
    assert parse_duration("12s") == 12_000
    assert parse_duration("59s") == 59_000
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("60s")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("123s")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("01s")


def test_parse_duration_minutes() -> None:
    assert parse_duration("1m") == 60_000
    assert parse_duration("12m") == 12 * 60 * 1000
    assert parse_duration("59m") == 59 * 60 * 1000
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("60m")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("01m")


def test_combinations() -> None:
    assert parse_duration("12s34ms") == 12_034
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12s3456ms")

    assert parse_duration("12m34s") == 12 * 60 * 1000 + 34_000
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m345s")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("123m45s")

    assert parse_duration("12m34ms") == 12 * 60 * 1000 + 34
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m3456ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("123m45ms")

    assert parse_duration("12m34s45ms") == 12 * 60 * 1000 + 34_045
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m34s4567ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m345s56ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("123m45s56ms")

    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m34s056ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("12m034s56ms")
    with pytest.raises(ValueError, match=r".*"):
        parse_duration("012m34s56ms")
