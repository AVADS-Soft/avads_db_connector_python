import looping


class Series:
    name: str = ""
    seriesType: int = 0
    seriesId: int = 0
    comment: str = ""
    viewTimeMod: int = 0
    looping = looping.Looping()
    cl: int = 0

    def __str__(self):
        s = "Series"
        s += "{"
        s += f'Name: {self.name}, Id: {self.seriesId}'
        s += "}"
        return s
