# Abettor - Helper libraries written in C

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
 $ cp fence/src/master/master.conf /usr/bin
```

Copy `slave.conf` to `/usr/bin` for slave node configuration.

```shell
 $ cp fence/src/slave/slave.conf /usr/bin
```

### Developers

Kamran Amini  (kam.cpp@gmail.com)
