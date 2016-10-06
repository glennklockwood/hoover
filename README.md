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

[TOKIO project]: https://www.nersc.gov/research-and-development/tokio/
