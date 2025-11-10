package grpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiverQueryService_createInternalError(t *testing.T) {
	result := createInternalError("some message", fmt.Errorf("error details"))
	require.NotNil(t, result)
	require.Contains(t, result.Error(), "some message")
	require.Contains(t, result.Error(), "code = Internal")
	require.NotContains(t, result.Error(), "error details") // don't leak details
}
