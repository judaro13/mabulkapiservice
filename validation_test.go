package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateEnvVars(t *testing.T) {
	os.Setenv("RABBIT_URL", "")
	assert.Panics(t, validateEnvVars)
}
