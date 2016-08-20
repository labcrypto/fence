# Fence - A durable queue

### Installation
#### Linux (Debian-based distributions)
First, install abettor-c, abettor-cc and hottentot from following links:

[LabCryptoOrg/abettor-c](https://github.com/LabCryptoOrg/abettor-c)

[LabCryptoOrg/abettor-cc](https://github.com/LabCryptoOrg/abettor-cc)

[LabCryptoOrg/hottentot](https://github.com/LabCryptoOrg/hottentot)


Read READMEs carefully for installation instructions.

Then, clone fence source code from github.

```shell
 $ git clone https://github.com/LabCryptoOrg/fence
```

Compile and install sources.

```shell
 $ cd fence
 $ mkdir -p build
 $ cd build
 $ cmake ..
 $ make
 $ sudo make install
```

Copy `master.conf` to `/usr/bin` for master node configuration.

```shell
 $ cd fence
 $ sudo cp src/master/master.conf /usr/bin
```

Copy `slave.conf` to `/usr/bin` for slave node configuration.

```shell
 $ cd fence
 $ sudo cp src/slave/slave.conf /usr/bin
```

Make workig directories.

```shell
 $ sudo mkdir -p /opt/org/labcrypto/fence/master/work
 $ sudo mkdir -p /opt/org/labcrypto/fence/slave/work
```

Start master and slave nodes.

```shell
 $ sudo fence-master
 $ sudo fence-slave
```

### Developers

Kamran Amini  (kam.cpp@gmail.com)
