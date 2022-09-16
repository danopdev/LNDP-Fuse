# Local Network Document Provider (FUse access from Linux systems)

For the linux server checkout [LNDP-Server](https://github.com/danopdev/LNDP-Server) project.
For the Android client checkout [LNDP-Android](https://github.com/danopdev/LNDP-Android).

## Install

> **_NOTE:_** Tested with node 14 & 16.

Clone this repository:
```
git clone git@github.com:danopdev/LNDP-Fuse.git
```

> **_NOTE:_** You may need to install some python packages ... to update this section.

## Mount

```
cd LNDP-Fuse
./lndp-fuse.py <path>
```

Where the path is will be the root mount point.
In this folder it will show a folder for each server.

## TODO ##

* Add write support:
  * Create files / directories
  * Delete files / directories
  * Write to files
