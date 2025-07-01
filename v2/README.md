# Archive Query Service v2 #

## Endpoints

Provides the following endpoints:

* `/getTransactionByHash`
* `/getTransactionsForTick`
* `/getTransactionsForIdentity`
* `/getTickData`
* `/getLastProcessedTick`
* `/getProcessedTicksIntervals`

### Get transactions for Identity

Returns the transactions for one identity sorted by tick number descending.

Method: `POST`
Path: `/getTransactionsForIdentity`
Accept: `application/json`

#### Request

_GetTransactionsForIdentityRequest_

| Name       | Type               | Necessity | Description                                                                                                       | 
|------------|--------------------|-----------|-------------------------------------------------------------------------------------------------------------------|
| identity   | string             | required  | 60 characters uppercase identity.                                                                                 | 
| filters    | map<string,string> | optional  | Filters that restrict results to single value.<br/> Allowed fields are: source, destination, amount, inputType    |
| ranges     | map<string,Range>  | optional  | Filters that restrict results to a value range.<br/> Allowed fields are: amount, tickNumber, inputType, timestamp |
| pagination | Pagination         | optional  | Allows to specify the first record and the number of records to be retrieved.                                     |

_Range_

| Name      | Type   | Necessity | Description                               | 
|-----------|--------|-----------|-------------------------------------------|
| `<field>` | string | required  | Name of the field you wish to search for. |
| gt        | string | optional  | Greater than.                             |
| gte       | string | optional  | Greater than or equal to.                 |
| lt        | string | optional  | Less than.                                |
| lte       | string | optional  | Less than or equal to.                    |

Only one lower bound (`gt` or `gte`) and one upper bound (`lt` or `lte`) can be specified.

_Pagination_

| Name   | Type   | Necessity | Description                                                                                         |
|--------|--------|-----------|-----------------------------------------------------------------------------------------------------|
| offset | uint32 | optional  | The offset of the first record to return. Defaults to zero (first record). Maximum offset is 10000. |
| size   | uint32 | optional  | Defaults to 10. Maximum size is 1000.                                                               |

_Examples_

Show up to 10 qu burn transactions that are larger than one million and within tick range 25563000 and 28300000
and that are sent from IIJHZSNPDRYYXCQBWNGKBSWYYDCARTYPOBXGOXZEVEZMMWYHPBVXZLJARRCB.

```json
{
    "identity": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
    "filters": {
           "source": "IIJHZSNPDRYYXCQBWNGKBSWYYDCARTYPOBXGOXZEVEZMMWYHPBVXZLJARRCB",
           "inputType": "0"
    },
    "ranges": {
        "amount": {
            "gt": "1000000"
        },
        "tickNumber": {
            "gte": "25563000",
            "lte": "28300000"
        }
    },
    "pagination": {
      "offset": 0,
      "size": 10
    }
}
```

And see [request examples](v2-query-requests.http) for more.

#### Response

_GetTransactionsForIdentityResponse_

| Name         | Type        | Description                                        | 
|--------------|-------------|----------------------------------------------------|
| validForTick | uint32      | Up to which tick number the response is valid for. |
| hits         | Hits        | Information about returned hits (records).         |
| transactions | Transaction | List of transactions that match the query.         |

_Hits_

| Name  | Type   | Description                                                               |
|-------|--------|---------------------------------------------------------------------------|
| total | uint32 | Total number of matching records (capped at 10000).                       |
| from  | uint32 | Requested first matching record offset (equal to offset from pagination). |
| size  | uint32 | Requested result size (equal to size from pagination).                    |

_Transactions_

| Name        | Type    | Description                                         |
|-------------|---------|-----------------------------------------------------|
| hash        | string  | Hash of the transaction.                            |
| amount      | uint64  | Amount of the transaction.                          |
| source      | string  | Source identity of the transaction (sender).        |
| destination | string  | Destination identity of the transaction (receiver). |
| tickNumber  | uint32  | Number of the tick the transaction was included in. |
| timestamp   | uint64  | Timestamp of the transaction.                       |
| inputType   | uint32  | Input type of the transaction.                      |
| inputSize   | uint32  | Size of the transaction input in bytes.             |
| inputData   | string  | Input data of the transaction in base64 format.     |
| signature   | string  | Signature of the transaction in base64 format.      |
| moneyFlew   | boolean | Transaction status / money flew flag. Deprecated.   |

_Example_

```json
{
  "validForTick": 28316138,
  "hits": {
    "total": 29,
    "from": 0,
    "size": 3
  },
  "transactions": [
    {
      "hash": "nmjcrptpgnfejciqbgtuinfyfhucrcmshkgxwzygugylfrvwwiedfdobqprc",
      "amount": "5178846339",
      "source": "IIJHZSNPDRYYXCQBWNGKBSWYYDCARTYPOBXGOXZEVEZMMWYHPBVXZLJARRCB",
      "destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
      "tickNumber": 28296606,
      "timestamp": "1751345877000",
      "inputType": 0,
      "inputSize": 0,
      "inputData": "",
      "signature": "+HQ51gPgO9hi2kSmr4UmbHrhYbVruMvLt7ihZm+JqVhMqTcPUGplNhfwYSS/zVlmdaALctjn069EBOaFE/oiAA==",
      "moneyFlew": true
    },
    {
      "hash": "bhdiubkdllzwheoiuziqybysjrvfgmanlatxdgpofhamwuwensgnrnygwxgj",
      "amount": "5058432266",
      "source": "IIJHZSNPDRYYXCQBWNGKBSWYYDCARTYPOBXGOXZEVEZMMWYHPBVXZLJARRCB",
      "destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
      "tickNumber": 28272503,
      "timestamp": "1751300280000",
      "inputType": 0,
      "inputSize": 0,
      "inputData": "",
      "signature": "kKBgs+dsz+UXhtnfARuTKiM2RdOxalqtAMtod/Ohj7Qk4nO8dczi96law+CotsdmN9hseKSslw4YlgY3zAYaAA==",
      "moneyFlew": true
    },
    {
      "hash": "qucjxvqimjxyreziaguvtmzqzqcdssksmwkkaogdbaeoqddoowiwfzhfuggk",
      "amount": "5027366135",
      "source": "IIJHZSNPDRYYXCQBWNGKBSWYYDCARTYPOBXGOXZEVEZMMWYHPBVXZLJARRCB",
      "destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
      "tickNumber": 28262192,
      "timestamp": "1751280470000",
      "inputType": 0,
      "inputSize": 0,
      "inputData": "",
      "signature": "AGWddy/b8pdhqFnKSkH4CKJ27bySX/491yQtwiTw8Aben2kAV3PGizNxmVcEwl7fpWsG1L2w0sLm+cjvkisTAA==",
      "moneyFlew": true
    }
  ]
}
```

## References

The documentation might not be complete or up-to-date due to changes.
See [messages.proto](api/archive-query-service/v2/messages.proto) 
and [query_services.proto](api/archive-query-service/v2/query_services.proto) 
for full details.