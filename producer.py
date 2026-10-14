import mmap
import struct

with open("orders.dat", "w+b") as file:
    file.write(b"\x00" * 32)
    file.flush()

    with mmap.mmap(file.fileno(), 32) as memory:
        struct.pack_into("!IIII", memory, 16, 2002, 15025, 10, 0)
        memory.flush()

        print("Order written successfully!")
