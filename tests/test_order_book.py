import pytest

from src.engine.order_book import OrderBook

def test_orders_match_at_resting_ask_price():
    book = OrderBook()

    assert book.add_order(2003, 1, 15100, 5) == []

    trades = book.add_order(2004, 0, 15250, 8)

    assert trades == [
        {
            "buy_order_id": 2004,
            "sell_order_id": 2003,
            "price": 15100,
            "quantity": 5,
        }
    ]


def test_partial_fill_keeps_remaining_buy_quantity():
    book = OrderBook()

    book.add_order(2003, 1, 15100, 5)
    book.add_order(2004, 0, 15250, 8)

    # Five units traded; the remaining three should rest in the bid book.
    assert book.bids == {15250: [(2004, 3)]}
    assert book.asks == {}

def test_buy_matches_multiple_sell_orders():
    book = OrderBook()

    book.add_order(201, 1, 14900, 4)
    book.add_order(202, 1, 15000, 6)

    trades = book.add_order(301, 0, 15000, 7)

    assert len(trades) == 2
    assert [trade["sell_order_id"] for trade in trades] == [201, 202]
    assert [trade["quantity"] for trade in trades] == [4, 3]

    assert book.asks == {15000: [(202, 3)]}

def test_sell_matches_multiple_buy_orders():
    book = OrderBook()

    book.add_order(301, 0, 15000, 4)
    book.add_order(302, 0, 14900, 6)

    trades = book.add_order(401, 1, 14900, 7)

    assert len(trades) == 2
    assert [trade["buy_order_id"] for trade in trades] == [301, 302]
    assert [trade["quantity"] for trade in trades] == [4, 3]

    assert book.bids == {14900: [(302, 3)]}

def test_fifo_priority_for_same_price():
    book = OrderBook()

    book.add_order(501, 0, 15000, 3)
    book.add_order(502, 0, 15000, 5)

    trades = book.add_order(601, 1, 15000, 4)

    assert len(trades) == 2
    assert [trade["buy_order_id"] for trade in trades] == [501, 502]
    assert [trade["quantity"] for trade in trades] == [3, 1]

    assert book.bids == {15000: [(502, 4)]}

def test_invalid_order_side():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(701, 5, 15000, 3)

def test_invalid_order_price():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(801, 0, 0, 5)


def test_invalid_order_quantity():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(802, 0, 15000, -5)

def test_zero_order_quantity():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(803, 0, 15000, 0)
def test_boolean_side_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(901, True, 15000, 5)


def test_float_price_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(902, 0, 15000.5, 5)


def test_float_quantity_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(903, 0, 15000, 2.5)


def test_boolean_quantity_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(904, 0, 15000, True)
def test_boolean_order_id_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(True, 0, 15000, 5)


def test_float_order_id_is_rejected():
    book = OrderBook()

    with pytest.raises(TypeError):
        book.add_order(1.5, 0, 15000, 5)


def test_zero_order_id_is_rejected():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(0, 0, 15000, 5)


def test_negative_order_id_is_rejected():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(-1, 0, 15000, 5)


def test_order_id_above_uint32_is_rejected():
    book = OrderBook()

    with pytest.raises(ValueError):
        book.add_order(4294967296, 0, 15000, 5)
