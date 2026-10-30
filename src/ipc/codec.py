"""ChronosMatch Binary Order Codec.

Encodes and decodes orders into the agreed 16-byte binary format:
    Header / Layout: struct.Struct("!IIII")
    - order_id:    uint32 (4 bytes)
    - price_ticks: uint32 (4 bytes) — 100 ticks per rupee
    - quantity:    uint32 (4 bytes)
    - side:        uint32 (4 bytes) — 0 = BUY, 1 = SELL
"""

from __future__ import annotations

import struct
from typing import Any, Dict, Union

ORDER_STRUCT = struct.Struct("!IIII")
ORDER_SIZE = ORDER_STRUCT.size  # exactly 16 bytes
TICKS_PER_RUPEE = 100

SIDE_BUY_INT = 0
SIDE_SELL_INT = 1
SIDE_MAP_TO_INT = {
    "BUY": SIDE_BUY_INT,
    "SELL": SIDE_SELL_INT,
    SIDE_BUY_INT: SIDE_BUY_INT,
    SIDE_SELL_INT: SIDE_SELL_INT,
}
SIDE_MAP_TO_STR = {
    SIDE_BUY_INT: "BUY",
    SIDE_SELL_INT: "SELL",
}

MAX_UINT32 = 0xFFFFFFFF


class OrderCodec:
    """Encoder and decoder for 16-byte fixed-layout binary orders."""

    TICKS_PER_RUPEE: int = TICKS_PER_RUPEE
    ORDER_SIZE: int = ORDER_SIZE
    STRUCT: struct.Struct = ORDER_STRUCT

    @staticmethod
    def validate_fields(order: Dict[str, Any]) -> tuple[int, int, int, int]:
        """Validate order fields and convert to binary integers.

        Returns:
            Tuple of (order_id, price_ticks, quantity, side_int)

        Raises:
            TypeError: If field types are invalid.
            ValueError: If field values violate domain bounds.
        """
        if not isinstance(order, dict):
            raise TypeError(f"Order must be a dictionary, got {type(order).__name__}")

        # 1. order_id
        if "order_id" not in order:
            raise ValueError("Order missing required field 'order_id'")
        order_id = order["order_id"]
        if isinstance(order_id, bool) or not isinstance(order_id, int):
            raise TypeError(f"order_id must be an integer, got {type(order_id).__name__}")
        if order_id < 0 or order_id > MAX_UINT32:
            raise ValueError(f"order_id must be between 0 and {MAX_UINT32}, got {order_id}")

        # 2. price & price_ticks
        if "price" in order:
            price = order["price"]
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                raise TypeError(f"price must be a numeric value, got {type(price).__name__}")
            if price <= 0:
                raise ValueError(f"price must be positive, got {price}")
            price_ticks = int(round(price * TICKS_PER_RUPEE))
        elif "price_ticks" in order:
            price_ticks = order["price_ticks"]
            if isinstance(price_ticks, bool) or not isinstance(price_ticks, int):
                raise TypeError(f"price_ticks must be an integer, got {type(price_ticks).__name__}")
        else:
            raise ValueError("Order must contain 'price' or 'price_ticks'")

        if price_ticks <= 0 or price_ticks > MAX_UINT32:
            raise ValueError(f"price_ticks must be between 1 and {MAX_UINT32}, got {price_ticks}")

        # 3. quantity
        if "quantity" not in order:
            raise ValueError("Order missing required field 'quantity'")
        quantity = order["quantity"]
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise TypeError(f"quantity must be an integer, got {type(quantity).__name__}")
        if quantity <= 0 or quantity > MAX_UINT32:
            raise ValueError(f"quantity must be between 1 and {MAX_UINT32}, got {quantity}")

        # 4. side
        if "side" not in order:
            raise ValueError("Order missing required field 'side'")
        raw_side = order["side"]
        if isinstance(raw_side, str):
            side_key = raw_side.strip().upper()
        elif isinstance(raw_side, int) and not isinstance(raw_side, bool):
            side_key = raw_side
        else:
            raise TypeError(f"side must be str ('BUY'/'SELL') or int (0/1), got {type(raw_side).__name__}")

        if side_key not in SIDE_MAP_TO_INT:
            raise ValueError(f"Invalid side '{raw_side}'; must be 'BUY', 'SELL', 0, or 1")
        side_int = SIDE_MAP_TO_INT[side_key]

        return order_id, price_ticks, quantity, side_int

    @classmethod
    def encode(cls, order: Dict[str, Any]) -> bytes:
        """Encode an order dictionary into a 16-byte packed bytes object."""
        order_id, price_ticks, quantity, side_int = cls.validate_fields(order)
        return cls.STRUCT.pack(order_id, price_ticks, quantity, side_int)

    @classmethod
    def pack_into(cls, buffer: Any, offset: int, order: Dict[str, Any]) -> None:
        """Encode an order directly into an existing buffer/mmap at the given offset."""
        order_id, price_ticks, quantity, side_int = cls.validate_fields(order)
        cls.STRUCT.pack_into(buffer, offset, order_id, price_ticks, quantity, side_int)

    @classmethod
    def decode(
        cls,
        data: Union[bytes, bytearray, memoryview],
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Decode a 16-byte record from bytes/mmap into an order dictionary."""
        if len(data) - offset < cls.ORDER_SIZE:
            raise ValueError(
                f"Buffer too small to decode order: available {len(data) - offset} bytes, "
                f"required {cls.ORDER_SIZE} bytes"
            )

        order_id, price_ticks, quantity, side_int = cls.STRUCT.unpack_from(data, offset)

        if side_int not in SIDE_MAP_TO_STR:
            raise ValueError(f"Decoded invalid side integer: {side_int} (expected 0 or 1)")

        side_str = SIDE_MAP_TO_STR[side_int]
        price = price_ticks / float(cls.TICKS_PER_RUPEE)

        return {
            "order_id": order_id,
            "side": side_str,
            "price": price,
            "quantity": quantity,
        }

    @classmethod
    def unpack_from(
        cls,
        buffer: Any,
        offset: int,
    ) -> Dict[str, Any]:
        """Unpack an order dictionary directly from an mmap or buffer at offset."""
        return cls.decode(buffer, offset=offset)


def encode_order(order: Dict[str, Any]) -> bytes:
    """Convenience function to encode an order dictionary to 16 bytes."""
    return OrderCodec.encode(order)


def decode_order(data: Union[bytes, bytearray, memoryview], offset: int = 0) -> Dict[str, Any]:
    """Convenience function to decode an order from a buffer."""
    return OrderCodec.decode(data, offset=offset)
