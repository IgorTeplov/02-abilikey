import os


class PrintedStack:
    def __init__(self, lines=4, simple=False):
        self.lines = lines
        self.buffer = []
        self.simple = simple

    def print(self, message):
        if not self.simple:
            os.system('clear')
            if len(self.buffer) + 1 > self.lines:
                self.buffer.pop(0)
            self.buffer.append(str(message))
            for line in self.buffer:
                print(line)
        else:
            print(message)
