class RingBuffer:
    def __init__(self, capacity):
        if capacity < 2:
            raise ValueError("Capacity must be at least 2")

        self.capacity = capacity
        self.buffer = [None] * capacity
        self.write_index = 0
        self.read_index = 0

    def put(self, item):
        next_index = (self.write_index + 1) % self.capacity

        if next_index == self.read_index:
            raise BufferError("Ring buffer is full")

        self.buffer[self.write_index] = item
        self.write_index = next_index

    def get(self):
        if self.read_index == self.write_index:
            return None

        item = self.buffer[self.read_index]
        self.buffer[self.read_index] = None
        self.read_index = (self.read_index + 1) % self.capacity
        return item