import row


class RecWitchCP:
    recs: list = []
    startCP: str = ""
    endCP: str = ""
    hasContinuation: bool = False

    def __str__(self):
        s = "RecWitchCP"
        s += "{"
        s += f'RecsLength: {len(self.recs)}, StartCP: {self.startCP}, EndCP: {self.endCP}, HasContinuation: {self.hasContinuation}'
        s += "}"
        return s
