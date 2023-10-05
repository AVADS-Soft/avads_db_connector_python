import row
import typ


def packString(s: str):
    b = bytes(s, 'utf-8')
    b = len(b).to_bytes(4, byteorder='big', signed=False) + b
    return b


def unpackBytes(b: bytes, offset: int):
    start = offset
    end = start+4
    i = int.from_bytes(b[start:end], byteorder='big', signed=False)
    start = end
    end += i
    return b[start:end], end


def unpackString(b: bytes, offset: int):
    b, offset = unpackBytes(b, offset)
    return str(b, 'utf-8'), offset


def packInt64(i: int):
    return i.to_bytes(8, byteorder='big', signed=True)


def packInt16(i: int):
    return i.to_bytes(2, byteorder='big', signed=True)


def unpackInt64(b: bytes, offset: int):
    end = offset+8
    return int.from_bytes(b[offset:end], byteorder='big', signed=True), end


def unpackInt32(b: bytes, offset: int):
    start = offset
    end = start+4
    b = b[start:end]
    i = int.from_bytes(b, byteorder='big', signed=True)
    return i, end


def packUInt32(i: int):
    return i.to_bytes(4, byteorder='big')


def unpackUInt32(b: bytes, offset: int):
    start = offset
    end = start+4
    i = int.from_bytes(b[start:end], byteorder='big', signed=False)
    return i, end


def packUInt8(i: int):
    return i.to_bytes(1, byteorder='big', signed=False)


def unpackUInt8(b: bytes, offset: int):
    start = offset
    end = start+1
    i = int.from_bytes(b[start:end], byteorder='big', signed=False)
    return i, end


def unpackBoolean(b: bytes, offset: int):
    start = offset
    end = start+1
    i = bool.from_bytes(b[start:end], byteorder='big')
    return i, end


def packBoolean(v: bool):
    return v.to_bytes(1, byteorder='big')


def valToBinary(v):
    match v:
        case bool():
            u = 0
            if v:
                u = 1
            return packInt64(u)
        case int():
            return packInt64(v)
        case str():
            return packString(v)
        case bytes():
            return v
        case _:
            msg = "invalid type"
            raise BaseException(msg)
    return


def packRow(seriesId: int, cl: int, t: int, q: int, value: any):
    match cl:
        case typ.SimpleClass:
            b = packInt64(seriesId)
            b += packUInt8(cl)
            b += packInt64(t)
            b += valToBinary(value)
            b += packUInt32(q)
            return b

        case typ.BlobClass:
            b = packInt64(seriesId)
            b += packUInt8(cl)
            b += packInt64(t)
            match value:
                case str():
                    b += packString(value)
                case _:
                    msg = "pack row: invalid value"
                    raise BaseException(msg)
            b += packUInt32(q)
            return b

        case _:
            msg = "pack row: invalid class"
            raise BaseException(msg)
