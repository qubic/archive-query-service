package elastic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTickDataByTickNumberQuery(t *testing.T) {
	tickNumber := uint32(123)
	expected := `{"query":{"term":{"tickNumber":"123"}},"size":1}`
	actual := createTickDataByTickNumberQuery(tickNumber)
	assert.Equal(t, expected, actual)
}
