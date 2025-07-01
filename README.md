## Archive Query Service ##

The archive query service repository contains service to query the new RPC 2.0 long term archive. It consists out of the
following services:

* [archive-query-service-v1](v1/README.md) ... provides backwards compatible endpoints to replace some of the current
  archiver endpoints transparently (so that we can use the new storage backend with the old rpc service endpoints).
* [archive-query-service-v2](v2/README.md) ... provides new endpoints that are more flexible and leverage the new storage 
  architecture.

