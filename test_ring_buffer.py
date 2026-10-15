from src.ipc.ring_buffer import RingBuffer

rb = RingBuffer(4)

rb.put(10)
rb.put(20)
rb.put(30)

print(rb.get())
print(rb.get())

rb.put(40)

print(rb.get())
print(rb.get())
