
import mmap
import struct

with open("orders.dat", "r+b") as file:
    with mmap.mmap(file.fileno(), 32) as memory:
        order = struct.unpack_from("!IIII", memory, 16)

        print("Order ID:", order[0])
        print("Price:", order[1])
        print("Quantity:", order[2])
        print("Side:", "BUY" if order[3] == 0 else "SELL")
