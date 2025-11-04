## Archive Query Service v1 ##

Replaces the following endpoints:

* `/v1/status`
* `/v1/ticks/{tick_number}/transactions`
* `/v1/ticks/{tick_number}/approved-transactions`
* `/v1/transactions/{tx_id}`
* `/v1/tx-status/{tx_id}`
* `/v2/identities/{identity}/transactions`
* `/v2/identities/{identity}/transfers`
* `/v2/ticks/{tick_number}/transactions`
* `/v2/transactions/{tx_id}`
* `/v1/ticks/{tick_number}/tick-data`

See [transactions.proto](protobuf/transactions.proto) for a complete overview of the provided endpoints.

The endpoints should be backwards compatible but not to 100%. For example order of returned objects might be different or
currently unused properties and functionalities missing.