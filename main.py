import socket
import io
import lib
import typ
import struct
import base
import series
import pack


host = '127.0.0.1'
port = 7777
login = 'admin'
password = 'admin'

print("host:", host)
print("port:", port)
print("login:", login)
print("password:", password)

baseName: str = 'test tcp api'
baseId: int = 222
series0: int = 0
series1: int = 1
series2: int = 2
series3: int = 3
class0: int = 0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(None)
s.connect((host, port))

try:
    version = lib.protocolVersion(s)
    print("version:", version)
    salt, h = lib.loginGetKeys(s, login, password)
    print("salt:", salt)
    print("h:", h)
    sessionKey = lib.loginValidPass(s, h)
    print("sessionKey:", sessionKey)

    try:
        baseInfo = lib.baseGetInfo(s, baseName)
        print("base info:", baseInfo)
    except BaseException as err:
        print(err)

    try:
        lib.baseRemove(s, baseName)
        print("removed:", baseName)
    except BaseException as err:
        print(err)

    ba = base.Base()
    ba.name = baseName
    ba.comment = baseName
    ba.path = "./db/test_tcp_api"
    ba.dbSize = "100m"
    ba.fsType = lib.FS_FS
    ba.autoAddSeries = True
    lib.baseCreate(s, ba)
    print("base created:", ba)
    se = series.Series()
    se.name = "LINT_single"
    se.seriesType = typ.LINT
    se.seriesId = series0
    lib.seriesCreate(s, baseName, se)
    print("series created:", se)
    se = series.Series()
    se.name = "LINT_cache"
    se.seriesType = typ.LINT
    se.seriesId = series2
    lib.seriesCreate(s, baseName, se)
    print("series created:", se)
    se = series.Series()
    se.name = "LINT_multi"
    se.seriesType = typ.STRING
    se.seriesId = series3
    se.cl = 1
    lib.seriesCreate(s, baseName, se)
    print("series created:", se)

    lib.baseOpen(s, baseId, baseName)
    print("base opened:", baseId, baseName)

    for i in range(10):
        lib.dataAddRow(s, baseId, series0, class0, i, 92, i)

    data = bytes()
    for i in range(10):
        b = pack.packRow(series1, typ.SimpleClass, i, 92, i)
        data += b
    count = lib.dataAddRowCache(s, baseId, data)
    print("count:", count)

    data = bytes()
    for i in range(10):
        b = pack.packRow(series2, typ.SimpleClass, i, 92, i)
        data += b
    lib.dataAddRows(s, baseId, data)

    data = bytes()
    for i in range(10):
        value = f"Разкудрить твою налева все четыре колеса {i} раз"
        b = pack.packRow(series3, typ.BlobClass, i, 92, value)
        data += b
    lib.dataAddRows(s, baseId, data)

    bou = lib.dataGetBoundary(s, baseId, series0)
    print("boundary:", bou)
    rec = lib.dataGetValueAtTime(s, baseId, series2, 2, typ.SimpleClass)
    print("record:", rec)
    recRange = lib.dataGetRangeDirection(
        s, baseId, series2, typ.SimpleClass, 1, 100, 0, 10, 0)
    print(recRange)

    count = lib.dataDeleteRows(s, baseId, series2, 5, 7)
    print("count:", count)

    recRange = lib.dataGetRangeDirection(
        s, baseId, series2, typ.SimpleClass, 1, 100, 0, 10, 0
    )
    print(recRange)

    cp = lib.dataGetCP(s, baseId, series2, 4)
    print("cp:", cp)

    recs = lib.dataGetFromCP(s, baseId, cp, 1, 2, typ.SimpleClass)
    print(recs)

    recs = lib.dataGetRangeFromCP(
        s, baseId, cp, 1, 10, typ.SimpleClass, 2, 8, 0)
    print(recs)

    cp = lib.dataGetCP(s, baseId, series3, 4)
    print("cp:", cp)

    recs = lib.dataGetRangeFromCP(
        s, baseId, cp, 1, 10, typ.BlobClass, 2, 8, 0)
    print(recs)

finally:
    lib.baseClose(s, baseId)
    print('end')
