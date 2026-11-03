class OrderBook:
    """
    Limit Order Book implementing price-time priority matching.
    Supports BUY (side 0) and SELL (side 1) orders.
    """
    def __init__(self):
        self.bids = {}  # BUY orders sorted descending by price
        self.asks = {}  # SELL orders sorted ascending by price

    def add_order(self, order_id: int, side: int, price_ticks: int, quantity: int):
        trade_events = []
        
        if side == 0:  # BUY Order
            while quantity > 0 and self.asks:
                best_ask_price = min(self.asks.keys())
                if price_ticks < best_ask_price:
                    break
                
                ask_order = self.asks[best_ask_price][0]
                ask_id, ask_qty = ask_order[0], ask_order[1]
                
                if quantity >= ask_qty:
                    trade_events.append({
                        "buy_order_id": order_id,
                        "sell_order_id": ask_id,
                        "price": best_ask_price,
                        "quantity": ask_qty
                    })
                    quantity -= ask_qty
                    self.asks[best_ask_price].pop(0)
                    if not self.asks[best_ask_price]:
                        del self.asks[best_ask_price]
                else:
                    trade_events.append({
                        "buy_order_id": order_id,
                        "sell_order_id": ask_id,
                        "price": best_ask_price,
                        "quantity": quantity
                    })
                    self.asks[best_ask_price][0] = (ask_id, ask_qty - quantity)
                    quantity = 0
            
            if quantity > 0:
                if price_ticks not in self.bids:
                    self.bids[price_ticks] = []
                self.bids[price_ticks].append((order_id, quantity))
                
        else:  # SELL Order
            while quantity > 0 and self.bids:
                best_bid_price = max(self.bids.keys())
                if price_ticks > best_bid_price:
                    break
                
                bid_order = self.bids[best_bid_price][0]
                bid_id, bid_qty = bid_order[0], bid_order[1]
                
                if quantity >= bid_qty:
                    trade_events.append({
                        "buy_order_id": bid_id,
                        "sell_order_id": order_id,
                        "price": best_bid_price,
                        "quantity": bid_qty
                    })
                    quantity -= bid_qty
                    self.bids[best_bid_price].pop(0)
                    if not self.bids[best_bid_price]:
                        del self.bids[best_bid_price]
                else:
                    trade_events.append({
                        "buy_order_id": bid_id,
                        "sell_order_id": order_id,
                        "price": best_bid_price,
                        "quantity": quantity
                    })
                    self.bids[best_bid_price][0] = (bid_id, bid_qty - quantity)
                    quantity = 0
            
            if quantity > 0:
                if price_ticks not in self.asks:
                    self.asks[price_ticks] = []
                self.asks[price_ticks].append((order_id, quantity))
                
        return trade_events
