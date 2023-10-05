class Row:
    t: int = 0
    value: bytes = bytes()
    q: bytes = bytes()

    def __str__(self):
        return 'Row{' + f'T: {self.t}, Value: {self.value}, Q: {self.q}' + '}'
