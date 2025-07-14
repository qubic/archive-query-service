package elastic

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestComputorsListElasticRepository_createComputorsListQuery(t *testing.T) {

	epoch := uint32(105)

	expectedQuery := `{
		"track_total_hits": "true",
		"query": {
			"match": {
				"epoch": 105
			}
		},
		"sort": {
			"tickNumber": "desc"
		},
		"size": 100
	}`

	query, err := createComputorsListQuery(epoch)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	require.JSONEq(t, expectedQuery, query.String())
}
