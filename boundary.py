class Boundary:
    minB: int = 0
    maxB: int = 0
    rowCount: int = 0
    startCP: str = ""
    endCP: str = ""

    def __str__(self):
        s = "Boundary"
        s += "{"
        s += f'Min: {self.minB}, Max: {self.maxB}, RowCount: {self.rowCount}'
        s += "}"
        return s
