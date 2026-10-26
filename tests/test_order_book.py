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