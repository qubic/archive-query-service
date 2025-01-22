package tx

//message Transaction {
//  string source_id = 1;
//  string dest_id = 2;
//  int64 amount = 3;
//  uint32 tick_number = 4;
//  uint32 input_type = 5;
//  uint32 input_size = 6;
//  string input_hex = 7;
//  string signature_hex = 8;
//  string tx_id = 9;
//}

type Tx struct {
	TxID       string `db:"tx_id" json:"txID"`
	SourceID   string `db:"source_id" json:"sourceID"`
	DestID     string `db:"dest_id" json:"destID"`
	Amount     int64  `db:"amount" json:"amount"`
	TickNumber uint32 `db:"tick_number" json:"tickNumber"`
	InputType  uint32 `db:"input_type" json:"inputType"`
	InputSize  uint32 `db:"input_size" json:"inputSize"`
	Input      string `db:"input" json:"input"`
	Signature  string `db:"signature" json:"signature"`
}
