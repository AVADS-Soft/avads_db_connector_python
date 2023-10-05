import socket
import struct
import io
import hashlib
import pack
import typ
import row
import boundary
import rec_witch_cp
import base
import series
import base

FS_FS: str = "fs"  # Файл базы пишется единым сегментом
FS_MULTIPART: str = "fs_mp"  # Файл бары разбивается по кускам 1 гб
FS_MEMORY: str = "mem_fs"

LoginGetKeys = 0
LoginValidPass = 1
RestoreSession = 2
GetProtocolVersion = 254

Disconnect = 0
BaseCreate = 1
BaseOpen = 2
BaseGetInfo = 3
BaseGetList = 4
BaseRemove = 5
BaseUpdate = 6
BaseClose = 7
SeriesCreate = 8
SeriesRemove = 9
SeriesUpdate = 10
SeriesGetAll = 11
SeriesGetInfo = 12
UserGetList = 13
UserGetInfo = 14
UserCreate = 15
UserRemove = 16
UserUpdate = 17
PropsGetList = 18
PropsGetInfo = 19
PropsSet = 20
DataGetBoundary = 21
DataGetCP = 22
DataGetFromCP = 23
DateGetRangeFromCP = 24
DateGetRangeDirection = 25
DataAddRow = 26
DataDeleteRow = 27
DataDeleteRows = 28
DataAddRowCache = 29
DataGetValueAtTime = 30
DataMathFunc = 31
DataAddRows = 32
DataGetLastValue = 33


def protocolVersion(s):
    b = pack.packUInt8(GetProtocolVersion)
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "protocol version: " + msg
    l, offset = pack.unpackUInt32(b, offset)
    version, offset = pack.unpackUInt8(b, offset)
    return version


def loginGetKeys(s, login, password):
    b = bytes(login, 'utf-8')
    b = pack.packUInt8(LoginGetKeys) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    (c, i,), b = struct.unpack(">Bi", b[:5]), b[5:]
    s = b[:i]
    i = s.index(b'\x00')
    ss = password + str(s[:i], 'utf-8')
    salt = hashlib.md5(ss.encode('utf-8')).hexdigest()
    ss = salt + str(s[i+1:], 'utf-8')
    h = hashlib.md5(ss.encode('utf-8')).hexdigest()
    return (salt, h)


def loginValidPass(s, h):
    b = bytes(h, 'utf-8')
    b = pack.packUInt8(LoginValidPass) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    (c, i,), b = struct.unpack(">Bi", b[:5]), b[5+4:]
    s = b[:i]
    if c != 0:
        raise BaseException(s)
    return str(s, 'utf-8')


def baseGetInfo(s, name):
    b = pack.packString(name)
    b = pack.packUInt8(BaseGetInfo) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "base get info: " + msg
        raise BaseException(msg)
    res = base.Base()
    res.name, offset = pack.unpackString(b, offset)
    res.path, offset = pack.unpackString(b, offset)
    res.comment, offset = pack.unpackString(b, offset)
    res.status, offset = pack.unpackInt64(b, offset)
    res.looping.typ, offset = pack.unpackUInt8(b, offset)
    res.looping.lt, offset = pack.unpackString(b, offset)
    res.dbSize, offset = pack.unpackString(b, offset)
    res.fsType, offset = pack.unpackString(b, offset)
    res.autoAddSeries, offset = pack.unpackBoolean(b, offset)
    res.autoSave, offset = pack.unpackBoolean(b, offset)
    res.autoSaveDuration, offset = pack.unpackString(b, offset)
    res.autoSaveInterval, offset = pack.unpackString(b, offset)
    return res


def baseRemove(s, name):
    b = pack.packString(name)
    b = pack.packUInt8(BaseRemove) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "base remove: " + msg
        raise BaseException(msg)
    return


def baseCreate(s: socket.socket, ba: base.Base):
    b = pack.packString(ba.name)
    b += pack.packString(ba.comment)
    b += pack.packString(ba.path)
    b += pack.packString(ba.fsType)
    b += pack.packString(ba.dbSize)
    b += pack.packUInt8(ba.looping.typ)
    b += pack.packString(ba.looping.lt)
    b += pack.packBoolean(ba.autoAddSeries)
    b += pack.packBoolean(ba.autoSave)
    b += pack.packString(ba.autoSaveDuration)
    b += pack.packString(ba.autoSaveInterval)
    b = pack.packUInt8(BaseCreate) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "base create: " + msg
        raise BaseException(msg)
    return


def baseOpen(s: socket.socket, baseId: int, name: str):
    b = pack.packInt64(baseId)
    b += pack.packString(name)
    b = pack.packUInt8(BaseOpen) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "base open: " + msg
        raise BaseException(msg)
    return


def baseClose(s: socket.socket, baseId: int):
    b = pack.packInt64(baseId)
    b = pack.packUInt8(BaseClose) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "base close: " + msg
        raise BaseException(msg)
    return


def seriesCreate(s: socket.socket, baseName: str, se: series.Series):
    b = pack.packString(baseName)
    b += pack.packInt64(se.seriesId)
    b += pack.packString(se.name)
    b += pack.packInt64(se.seriesType)
    b += pack.packUInt8(se.viewTimeMod)
    b += pack.packString(se.comment)
    b += pack.packUInt8(se.looping.typ)
    b += pack.packString(se.looping.lt)
    b = pack.packUInt8(SeriesCreate) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "series create: " + msg
        raise BaseException(msg)
    return


def dataAddRow(
    s: socket.socket, baseId: int, seriesId: int, cl: int, t: int, q: int, v
):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b += pack.packUInt8(cl)
    b += pack.packInt64(t)
    match cl:
        case typ.SimpleClass:
            b += pack.valToBinary(v)
        case typ.BlobClass:
            b += struct.pack('>i', 8) + pack.valToBinary(v)
        case _:
            msg = "data add row: invalid class"
            raise BaseException(msg)
    b += pack.packInt64(q)
    b = struct.pack('>Bi', DataAddRow, len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data add row: " + msg
        raise BaseException(msg)
    return


def dataGetBoundary(s: socket.socket, baseId: int, seriesId: int):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b = pack.packUInt8(DataGetBoundary) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get boundary: " + msg
        raise BaseException(msg)
    bou = boundary.Boundary()
    bou.minB, offset = pack.unpackInt64(b, offset)
    bou.maxB, offset = pack.unpackInt64(b, offset)
    bou.rowCount, offset = pack.unpackInt64(b, offset)
    bou.startCP, offset = pack.unpackString(b, offset)
    bou.endCP, offset = pack.unpackString(b, offset)
    return bou


def dataGetValueAtTime(
    s: socket.socket, baseId: int, seriesId: int, t: int, cl: int
):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b += pack.packInt64(t)
    b = pack.packUInt8(DataGetValueAtTime) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get value at time: " + msg
        raise BaseException(msg)
    r = row.Row()
    r.t, offset = pack.unpackInt64(b, offset)
    match cl:
        case typ.SimpleClass:
            r.value = b[offset:offset+8]
            offset += 8
        case typ.BlobClass:
            r.value, offset = pack.unpackBytes(b, offset)
        case _:
            msg = "data get value at time: invalid class"
            raise BaseException(msg)
    r.q = b[offset:offset+4]
    return r


def dataAddRowCache(s: socket.socket, baseId: int, data: bytes):
    b = pack.packInt64(baseId)
    b += data
    b = pack.packUInt8(DataAddRowCache) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data add row cache: " + msg
        raise BaseException(msg)
    count, offset = pack.unpackInt64(b, offset)
    return count


def dataAddRows(s: socket.socket, baseId: int, data: bytes):
    b = pack.packInt64(baseId)
    b += data
    b = pack.packUInt8(DataAddRows) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    if c != 0:
        l, offset = pack.unpackUInt32(b, offset)
        msg, offset = pack.unpackString(b, offset)
        msg = "data add rows: " + msg
        raise BaseException(msg)
    return


def dataGetRangeDirection(
    s: socket.socket,
    baseId: int,
    seriesId: int,
    cl: int,
    direct: int,
    limit: int,
    minB: int,
    maxB: int,
    dpi: int,
):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b += pack.packUInt8(direct)
    b += pack.packInt64(limit)
    b += pack.packInt64(minB)
    b += pack.packInt64(maxB)
    b += pack.packInt16(dpi)
    b = pack.packUInt8(DateGetRangeDirection) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get range direction: " + msg
        raise BaseException(msg)
    r = rec_witch_cp.RecWitchCP()
    r.startCP, offset = pack.unpackString(b, offset)
    r.endCP, offset = pack.unpackString(b, offset)
    r.hasContinuation, offset = pack.unpackBoolean(b, offset)
    count, offset = pack.unpackInt64(b, offset)
    for i in range(count):
        ro = row.Row()
        ro.t, offset = pack.unpackInt64(b, offset)
        match cl:
            case typ.SimpleClass:
                ro.value = b[offset:offset+8]
                offset += 8
            case typ.BlobClass:
                ro.value, offset = pack.unpackBytes(b, offset)
            case _:
                msg = "data get range direction: invalid class"
                raise BaseException(msg)
        ro.q = b[offset:offset+4]
        offset += 4
        r.recs.append(ro)
    return r


def dataDeleteRows(
    s: socket.socket,
    baseId: int,
    seriesId: int,
    timeStart: int,
    timeEnd: int
):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b += pack.packInt64(timeStart)
    b += pack.packInt64(timeEnd)
    b = pack.packUInt8(DataDeleteRows) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data delete rows: " + msg
        raise BaseException(msg)
    count, offset = pack.unpackInt64(b, offset)
    return count


def dataGetCP(
    s: socket.socket,
    baseId: int,
    seriesId: int,
    t: int,
):
    b = pack.packInt64(baseId)
    b += pack.packInt64(seriesId)
    b += pack.packInt64(t)
    b = pack.packUInt8(DataGetCP) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get CP: " + msg
        raise BaseException(msg)
    cp, offset = pack.unpackString(b, offset)
    return cp


def dataGetFromCP(
    s: socket.socket,
    baseId: int,
    cp: str,
    direct: int,
    limit: int,
    cl: int
):
    b = pack.packInt64(baseId)
    b += pack.packString(cp)
    b += pack.packUInt8(direct)
    b += pack.packInt64(limit)
    b = pack.packUInt8(DataGetFromCP) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get from CP: " + msg
        raise BaseException(msg)
    r = rec_witch_cp.RecWitchCP()
    r.startCP, offset = pack.unpackString(b, offset)
    r.endCP, offset = pack.unpackString(b, offset)
    r.hasContinuation, offset = pack.unpackBoolean(b, offset)
    recsLength, offset = pack.unpackInt64(b, offset)
    r.recs = []
    for i in range(recsLength):
        ro = row.Row()
        ro.t, offset = pack.unpackInt64(b, offset)
        match cl:
            case typ.SimpleClass:
                ro.value = b[offset:offset+8]
                offset += 8
            case typ.BlobClass:
                ro.value, offset = pack.unpackBytes(b, offset)
            case _:
                msg = "data get from CP: invalid class"
                raise BaseException(msg)
        ro.q = b[offset:offset+4]
        offset += 4
        r.recs.append(ro)
    return r


def dataGetRangeFromCP(
    s: socket.socket,
    baseId: int,
    cp: str,
    direct: int,
    limit: int,
    cl: int,
    minB: int,
    maxB: int,
    dpi: int,
):
    b = pack.packInt64(baseId)
    b += pack.packString(cp)
    b += pack.packUInt8(direct)
    b += pack.packInt64(limit)
    b += pack.packInt64(minB)
    b += pack.packInt64(maxB)
    b += pack.packInt16(dpi)
    b = pack.packUInt8(DateGetRangeFromCP) + pack.packUInt32(len(b)) + b
    s.sendall(b)
    b = s.recv(1024)
    if not b:
        msg = "invalid data"
        raise BaseException(msg)
    offset = 0
    c, offset = pack.unpackUInt8(b, offset)
    l, offset = pack.unpackUInt32(b, offset)
    if c != 0:
        msg, offset = pack.unpackString(b, offset)
        msg = "data get range from CP: " + msg
        raise BaseException(msg)
    r = rec_witch_cp.RecWitchCP()
    r.startCP, offset = pack.unpackString(b, offset)
    r.endCP, offset = pack.unpackString(b, offset)
    r.hasContinuation, offset = pack.unpackBoolean(b, offset)
    recsLength, offset = pack.unpackInt64(b, offset)
    r.recs = []
    for i in range(recsLength):
        ro = row.Row()
        ro.t, offset = pack.unpackInt64(b, offset)
        match cl:
            case typ.SimpleClass:
                ro.value = b[offset:offset+8]
                offset += 8
            case typ.BlobClass:
                ro.value, offset = pack.unpackBytes(b, offset)
            case _:
                msg = "data get range from CP: invalid class"
                raise BaseException(msg)
        ro.q = b[offset:offset+4]
        offset += 4
        r.recs.append(ro)
    return r
