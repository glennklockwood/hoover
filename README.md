HOOVER
================================================================================

Hoover is the framework we will use to recover incomplete Darshan logs from
compute nodes' ramdisks and reconstruct valid Darshan logs.

It is a three-component system that uses RabbitMQ to transport and load balance
pieces of Darshan logs.  The current architectural documentation can be found
here:

https://sites.google.com/a/lbl.gov/glennklockwood/nersc-infrastructure/rabbitmq

This infrastructure has been created as a part of the [TOKIO project][], funded
by the U.S. Department of Energy Office of Science's Advanced Scientific
Computing Research program under the Storage Systems and I/O project.  **This
software is not licensed for distribution.  Contact its author if you would
like a license to use any part of this software.**

Installation
--------------------------------------------------------------------------------
Hoover relies on [rabbitmq-c][] library, openssl, and libz.  To compile
rabbitmq-c, download the package, untar it, cd into the untarred directory, then

    libtoolize
    autoreconf -i
    ./configure --prefix=$PWD/_install --enable-static
    make install

Then edit the Hoover Makefile and set `RMQ_C_DIR` appropriately.  You may also
want to set `OTHER_PKGS_DIR` to reflect the location where libssl and libz are
installed.

[TOKIO project]: https://www.nersc.gov/research-and-development/tokio/
[rabbitmq-c]: https://github.com/alanxz/rabbitmq-c
