package integration

const e2eEventsIndex = "qubic-event-logs-e2e"

const productionEventsMapping = `{
	"settings": {
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings": {
		"dynamic": "strict",
		"properties": {
			"epoch":                  { "type": "integer" },
			"tickNumber":             { "type": "unsigned_long" },
			"timestamp":              { "type": "date", "format": "epoch_millis" },
			"emittingContractIndex":  { "type": "unsigned_long" },
			"transactionHash":        { "type": "keyword", "ignore_above": 60 },
			"logId":                  { "type": "unsigned_long" },
			"logDigest":              { "type": "keyword", "ignore_above": 60 },
			"type":                   { "type": "short" },
			"category":               { "type": "byte" },
			"source":                 { "type": "keyword", "ignore_above": 60 },
			"destination":            { "type": "keyword", "ignore_above": 60 },
			"amount":                 { "type": "unsigned_long" },
			"assetName":              { "type": "keyword", "ignore_above": 60 },
			"assetIssuer":            { "type": "keyword", "ignore_above": 60 },
			"numberOfShares":         { "type": "unsigned_long" },
			"managingContractIndex":  { "type": "unsigned_long" },
			"unitOfMeasurement":      { "type": "binary" },
			"numberOfDecimalPlaces":  { "type": "byte", "index": false },
			"deductedAmount":         { "type": "unsigned_long" },
			"remainingAmount":        { "type": "long" },
			"contractIndex":          { "type": "unsigned_long" },
			"contractIndexBurnedFor": { "type": "unsigned_long" }
		}
	}
}`

// seedEvent mirrors the ES document structure (the internal event struct is unexported).
type seedEvent struct {
	Epoch                  uint32 `json:"epoch"`
	TickNumber             uint32 `json:"tickNumber"`
	Timestamp              uint64 `json:"timestamp"`
	EmittingContractIndex  uint64 `json:"emittingContractIndex"`
	TransactionHash        string `json:"transactionHash"`
	LogID                  uint64 `json:"logId"`
	LogDigest              string `json:"logDigest"`
	Type                   uint32 `json:"type"`
	Category               uint32 `json:"category"`
	Source                 string `json:"source"`
	Destination            string `json:"destination"`
	Amount                 uint64 `json:"amount"`
	AssetName              string `json:"assetName"`
	AssetIssuer            string `json:"assetIssuer"`
	NumberOfShares         uint64 `json:"numberOfShares"`
	ManagingContractIndex  uint64 `json:"managingContractIndex"`
	UnitOfMeasurement      string `json:"unitOfMeasurement"`
	NumberOfDecimalPlaces  uint32 `json:"numberOfDecimalPlaces"`
	DeductedAmount         uint64 `json:"deductedAmount"`
	RemainingAmount        int64  `json:"remainingAmount"`
	ContractIndex          uint64 `json:"contractIndex"`
	ContractIndexBurnedFor uint64 `json:"contractIndexBurnedFor"`
}

var seedType0 = seedEvent{
	Epoch: 100, TickNumber: 15000, Timestamp: 1700000001000,
	EmittingContractIndex: 1, TransactionHash: "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh",
	LogID: 1, LogDigest: "digest0", Type: 0, Category: 0,
	Source: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", Destination: "BZBQFLLBNCXEMGQOUAPQYSWCBHRBJJFHFFLSENFLEVKEIYVHDSOFWKUUPGJD", Amount: 5000,
}

var seedType1 = seedEvent{
	Epoch: 100, TickNumber: 15001, Timestamp: 1700000002000,
	EmittingContractIndex: 1, TransactionHash: "atrpnwqfgkjlbchsdyeimxouvzatrpnwqfgkjlbchsdyeimxouvzatrpnwqf",
	LogID: 2, LogDigest: "digest1", Type: 1, Category: 1,
	AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", NumberOfShares: 1000000,
	ManagingContractIndex: 5, AssetName: "QX",
	NumberOfDecimalPlaces: 2, UnitOfMeasurement: "dW5pdHM=",
}

var seedType2 = seedEvent{
	Epoch: 100, TickNumber: 15002, Timestamp: 1700000003000,
	EmittingContractIndex: 2, TransactionHash: "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh",
	LogID: 3, LogDigest: "digest2", Type: 2, Category: 0,
	Source: "DLRMHGPFARAKPFLBCIFGQWFPMFPAQKESVFAIGGHFXQFBKGMUBBGPCJFKNMMD", Destination: "EPFNIJQGQBSLQLGDDJGHRGQNGOBRLFRTGHBHIJGYLRGCLHJOCCQDHGKLONNE",
	AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", AssetName: "TOKEN", NumberOfShares: 500,
}

var seedType3 = seedEvent{
	Epoch: 101, TickNumber: 16000, Timestamp: 1700000004000,
	EmittingContractIndex: 3, TransactionHash: "bkuedoxghrlmcfitjwangpyqzbkuedoxghrlmcfitjwangpyqzbkuedoxghr",
	LogID: 4, LogDigest: "digest3", Type: 3, Category: 0,
	Source: "FQGOKLRHRCTNRMHEEKHIBRHOPHCSMGSUHIBIJKHZMSHDMNKIPDREIHHLPPPF", Destination: "GRHPLMSISDUPSNIFFLKJCSIPQIDTNHTVIJCJKLIANSKENLLJQESFJIIMQQRG",
	AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", AssetName: "TOKEN", NumberOfShares: 300,
}

var seedType8 = seedEvent{
	Epoch: 101, TickNumber: 16001, Timestamp: 1700000005000,
	TransactionHash: "cmvfepyihksndgjtuxbohrqzacmvfepyihksndgjtuxbohrqzacmvfepyihks",
	LogID: 5, LogDigest: "digest8", Type: 8, Category: 0,
	Source: "HSIQQNTTJTEVRPOJGGMLKDSQRJEUPIUWJKDKLMJBTOLFOMMMKRFTGKKJNRSH", Amount: 9999, ContractIndexBurnedFor: 7,
}

var seedType13 = seedEvent{
	Epoch: 101, TickNumber: 16002, Timestamp: 1700000006000,
	TransactionHash: "dnwgfqzjiltoehukvycpiskabdnwgfqzjiltoehukvycpiskabdnwgfqzjilt",
	LogID: 6, LogDigest: "digest13", Type: 13, Category: 0,
	DeductedAmount: 50000, RemainingAmount: 100000, ContractIndex: 3,
}
