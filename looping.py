class Looping:
    typ: int = 0
    lt: str = ""
    lifeTime: int = 0

    def __str__(self):
        s = "Looping"
        s += "{"
        s += f'Name: {self.name}, Path: {self.path}, Comment: {self.comment}'
        s += "}"
        return s
