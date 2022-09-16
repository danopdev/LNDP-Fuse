#!/usr/bin/python3

from asyncio.unix_events import _UnixSelectorEventLoop
from genericpath import isdir
import sys
import os
import errno
import time
from tokenize import String
from wsgiref.simple_server import server_version
from xmlrpc.client import Boolean
import requests
from zeroconf import ServiceBrowser, ServiceListener, Zeroconf
from dataclasses import dataclass
from fusepy import FUSE, FuseOSError, Operations, LoggingMixIn
from urllib.parse import urlencode


def now():
    return int(time.time())



class Cache:
    def __init__(self, timeout):
        self.data = {}
        self.timeout = timeout

    def get(self, key):
        try:
            value, time = self.data[key]
            if (now() - time) > self.timeout:
                del self.data[key]
                return None
            return value
        except Exception as e:
            print(e)
        return None

    def set(self, key, value):
        try:
            self.data[key] = (value, now())
        except:
            pass

    def getOrUpdate(self, key, callback):
        value = self.get(key)
        if not value is None:
            return value

        value = callback()
        self.set(key, value)
        return value


@dataclass
class LNDPServerInfo:
    name: str
    port: int
    address: str
    ssl: bool
    time: int


class ZeroConfListener(ServiceListener):
    def getServerName( self, name: str ) -> str:
        return name.split('.')[0]

    def addServer( self, zc: Zeroconf, type_: str, name: str ) -> None:
        global lndpServers
        serverName = self.getServerName( name )
        info = zc.get_service_info(type_, name)
        addressBin = info.addresses[0]
        addressStr = '.'.join([ "%s" % x for x in addressBin ])
        port = info.port
        ssl = False
        if b'ssl' in info.properties:
            ssl = info.properties[b'ssl'] == b'true'
        if serverName in lndpServers:
            server = lndpServers[serverName]
            server.port = port
            server.address = addressStr
            server.ssl = ssl
            server.time = now()
        else:
            lndpServers[serverName] = LNDPServerInfo(serverName, port, addressStr, ssl, now())

        print(f"Add / Update: {lndpServers[serverName]}")


    def removeServer( self, name: str ) -> None:
        global lndpServers
        serverName = self.getServerName( name )
        if serverName in lndpServers:
            del lndpServers[serverName]
            print(f"Remove: {serverName}")

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        #self.addServer( zc, type_, name ) #there will always be an update_service
        pass

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.addServer( zc, type_, name )

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.removeServer( name )



class LNDPFuse(Operations, LoggingMixIn):
    FILE_DESCRIPTOR_MAX = 32
    CACHE_TIMEOUT = 10


    def __init__(self):
        self.usedFileDescriptors = [ None for i in range(LNDPFuse.FILE_DESCRIPTOR_MAX) ]
        self.cache = Cache(LNDPFuse.CACHE_TIMEOUT)


    #descriptors
    def _descriptorOpen( self, server: LNDPServerInfo, path: str ) -> int:
        for fileDescriptor in range(LNDPFuse.FILE_DESCRIPTOR_MAX):
            if self.usedFileDescriptors[fileDescriptor] is None:
                self.usedFileDescriptors[fileDescriptor] = (server, path)
                return fileDescriptor

        return -1


    def _descriptorClose( self, descriptor: int ):
        self.usedFileDescriptors[descriptor] = None


    # Helpers
    # =======

    def _raiseFileNotFound(self, errCode = errno.ENOENT):
        raise FuseOSError(errCode)


    def _splitPath(
                self,
                receivedPath: str,
                callbackRoot,
                callbackServer,
                callbackPath,
                callbackCustomParams = None,
                errCode = errno.ENOENT):
        if not receivedPath.startswith('/'):
            self._raiseFileNotFound(errCode)

        if '/' == receivedPath:
            if callbackRoot is None:
                self._raiseFileNotFound(errCode)
            try:
                return callbackRoot(callbackCustomParams)
            except:
                self._raiseFileNotFound(errCode)

        if receivedPath.endswith('/'):
            self._raiseFileNotFound(errCode)

        fields = receivedPath.split('/', 2)
        if len(fields) < 2:
            self._raiseFileNotFound(errCode)

        serverName = fields[1]
        if not serverName in lndpServers:
            self._raiseFileNotFound(errCode)

        server = lndpServers[serverName]
        if len(fields) < 3:
            if callbackServer is None:
                self._raiseFileNotFound(errCode)
            try:
                return callbackServer(server, callbackCustomParams)
            except:
                self._raiseFileNotFound(errCode)

        if callbackPath is None:
            self._raiseFileNotFound(errCode)

        try:
            return callbackPath(server, '/' + fields[2], callbackCustomParams)
        except:
            self._raiseFileNotFound(errCode)


    def _remoteCall( self, server: LNDPServerInfo, request: str, path: str, extraParams: dict, useGet: Boolean = True, uploadBlock = None ) -> requests.Response:
            prefix = 'https' if server.ssl else 'http'
            params = {} if extraParams is None else extraParams.copy()
            params['path'] = path
            url = f"{prefix}://{server.address}:{server.port}/{request}"

            if useGet:
                return requests.get(url, params=params, verify=False)

            files = None if uploadBlock is None else { 'block' : uploadBlock }
            return requests.post(url, params=params, verify=False, files=files)


    def _getBinary( self, server: LNDPServerInfo, request: str, path: str, extraParams: dict = None ):
        try:
            return self._remoteCall( server, request, path, extraParams ).content
        except Exception as e:
            print(e)
            self._raiseFileNotFound()


    def _getJson( self, server: LNDPServerInfo, request: str, path: str, extraParams: dict = None, useGet: Boolean = True, uploadBlock = None ):
        try:
            return self._remoteCall( server, request, path, extraParams, useGet, uploadBlock ).json()
        except Exception as e:
            print(e)
            self._raiseFileNotFound()


    def _lndpQueryDocument(self, server: LNDPServerInfo, path: str):
        return self._getJson( server, 'queryDocument', path )


    def _lndpQueryChildDocuments(self, server: LNDPServerInfo, path: str):
        return self._getJson( server, 'queryChildDocuments', path )


    def _lndpCreateDocument(self, server: LNDPServerInfo, parentPath: str, name: str, isDir: bool):
        return self._getJson( server, 'documentCreate', parentPath, { 'name': name, 'isdir': isDir } )


    def _lndpRenameDocument(self, server: LNDPServerInfo, parentPath: str, newname: str):
        return self._getJson( server, 'documentRename', parentPath, { 'newname': newname } )


    def _lndpReadDocument(self, server: LNDPServerInfo, path: str, offset: int, size: int):
        print(f"[_lndpReadDocument] size:{size}, offset:{offset}")
        return self._getBinary( server, 'documentRead', path, { 'offset': offset, 'size': size } )


    def _lndpWriteDocument(self, server: LNDPServerInfo, path: str, offset: int, data):
        return self._getJson( server, 'documentAppend', path, { 'offset': offset }, False, data )


    # Filesystem methods
    # ==================
    #chmod
    def chmod(self, path, mode):
        print(f"[chmod] path: {path}, mode: {mode}")
        return 0


    #chown
    def chown(self, path, uid, gid):
        print(f"[chown] path: {path}, uid: {uid}, gid: {gid}")
        return 0


    #getattr
    def _getattrRoot(self, params):
        return {
            'st_atime': startTime,
            'st_ctime': startTime,
            'st_mtime': startTime,
            'st_uid': uid,
            'st_gid': gid,
            'st_mode': 0o40555,
            'st_size': 4096,
            'st_nlink': 0,
        }


    def _getattrServer(self, server: LNDPServerInfo, params):
        return {
            'st_atime': server.time,
            'st_ctime': server.time,
            'st_mtime': server.time,
            'st_uid': uid,
            'st_gid': gid,
            'st_mode': 0o40555,
            'st_size': 4096,
            'st_nlink': 0,
        }


    def _getattrPathReal(self, server: LNDPServerInfo, path: str, params):
        json = self._lndpQueryDocument( server, path )[0]
        time = int(json['date']) // 1000
        mode = 0o555 if json['isreadonly'] else 0o777
        mode += 0o40000 if json['isdir'] else 0o100000

        return {
            'st_atime': time,
            'st_ctime': time,
            'st_mtime': time,
            'st_uid': uid,
            'st_gid': gid,
            'st_mode': mode,
            'st_size': int(json['size']),
            'st_nlink': 0,
        }


    def _getattrPath(self, server: LNDPServerInfo, path: str, params):
        return self.cache.getOrUpdate(
            ('getattr', server.name, path),
            lambda: self._getattrPathReal(server, path, params)
        )


    def getattr(self, path, fh=None):
        print(f"[getattr] path: {path}, fh: {fh}")
        return self._splitPath(
            path,
            self._getattrRoot,
            self._getattrServer,
            self._getattrPath,
            None
        )

    #access
    # def access(self, path, mode):
    #     return 0

    #readdir
    def _readdirRoot(self, params):
        return lndpServers.keys()


    def _readdirServer(self, server: LNDPServerInfo, params):
        return self._readdirPath(server, '/', params)


    def _readdirPath(self, server: LNDPServerInfo, path: str, params):
        json = self._lndpQueryChildDocuments(server, path)
        return [ item['name'] for item in json ]


    def readdir(self, path, fh):
        print(f"[readdir] path: {path}, fh: {fh}")

        return self._splitPath(
            path,
            self._readdirRoot,
            self._readdirServer,
            self._readdirPath,
            None
        )


    #rmdir
    # def rmdir(self, path):
    #     print(f"[rmdir] path: {path}")
    #     return None


    # #mkdir
    # def _mkdirPath(self, server: LNDPServerInfo, path: str, params):
    #     parentPath, name = path.rsplit('/', 1)
    #     self._lndpCreateDocument( server, parentPath, name, True )
    #     return 0


    # def mkdir(self, path: str, mode):
    #     return self._splitPath(
    #         path,
    #         None,
    #         None,
    #         self._mkdirPath,
    #         mode
    #     )


    # #rename
    # def _renamePath(self, server: LNDPServerInfo, path: str, newName):
    #     self._lndpRenameDocument(server, path, newName)
    #     return 0


    # def rename(self, old, new):
    #     return self._splitPath(
    #         old,
    #         None,
    #         None,
    #         self._renamePath,
    #         new
    #     )


    # File methods
    # ============

    #open
    def _openPath(self, server: LNDPServerInfo, path: str, flags):
        print(f"[_openPath] path={path}")
        self._lndpQueryDocument(server, path)
        return self._descriptorOpen(server, path)


    def open(self, path, flags):
        print(f"[open] path: {path}, flags: {flags}")
        return self._splitPath(
            path,
            None,
            None,
            self._openPath,
            flags
        )


    #create
    def _createPath(self, server: LNDPServerInfo, path: str, mode):
        parentPath, name = path.rsplit('/', 1)
        self._lndpCreateDocument( server, parentPath, name, False )
        return self._descriptorOpen(server, path)


    def create(self, path, mode, fi=None):
        print(f"[create] path: {path}, mode: {mode}, fi: {fi}")
        return self._splitPath(
            path,
            None,
            None,
            self._createPath,
            mode
        )


    #read
    def read(self, path, length, offset, fh):
        print(f"[read] path: {path}, length: {length}, offset: {offset}, fh: {fh}")
        server, serverPath = self.usedFileDescriptors[fh]
        return self._lndpReadDocument( server, serverPath, offset, length )


    # #write
    # def write(self, path, buf, offset, fh):
    #     print(f"[write] path: {path}, length: {len(buf)}, offset: {offset}, fh: {fh}")
    #     server, serverPath = self.usedFileDescriptors[fh]
    #     return self._lndpWriteDocument( server, serverPath, offset, buf )


    #release
    def release(self, path, fh):
        print(f"[release] path: {path}, fh: {fh}")
        self._descriptorClose( fh )
        return 0


    #flush
    def flush(self, path, fh): #not needed (there is no cache)
        print(f"[flush] path: {path}, fh: {fh}")
        return 0


    #fsync
    def fsync(self, path, fdatasync, fh): #not needed (there is no cache)
        print(f"[fsync] path: {path}, fdatasync: {fdatasync}, fh: {fh}")
        return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Format: {sys.argv[0]} <mount point>")
        sys.exit(1)

    uid = os.getuid()
    gid = os.getgid()
    startTime = now()
    lndpServers = {}
    zeroconf = Zeroconf()
    browser = ServiceBrowser(zeroconf, "_lndp._tcp.local.", ZeroConfListener())

    requests.packages.urllib3.disable_warnings()
    #logging.basicConfig(level=logging.DEBUG)

    FUSE(LNDPFuse(), sys.argv[1], nothreads=True, foreground=True)

    try:
        input("Press enter to exit...\n\n")
    finally:
        zeroconf.close()
