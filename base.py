import looping


class Base:
    name: str = ""
    path: str = ""
    comment: str = ""
    status: int = 0
    dataSize: int = 0
    looping = looping.Looping()
    dbSize: str = ""
    fsType: str = ""
    autoAddSeries: bool = False
    autoSave: bool = False
    autoSaveDuration: str = ""
    autoSaveInterval: str = ""

    def __str__(self):
        s = "Base"
        s += "{"
        s += f'Name: {self.name}, Path: {self.path}, Comment: {self.comment}'
        s += "}"
        return s
