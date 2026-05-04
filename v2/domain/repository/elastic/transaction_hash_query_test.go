package elastic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTransactionByHashQuery(t *testing.T) {
	hash := "test-hash"
	expected := `{"query":{"term":{"hash":"test-hash"}},"size":1}`
	actual := createTransactionByHashQuery(hash)
	assert.Equal(t, expected, actual)
}
