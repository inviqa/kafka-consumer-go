package config

import (
	"os"
	"testing"

	"github.com/go-test/deep"
)

func TestEnvVarAsIntSlice(t *testing.T) {
	defer os.Clearenv()

	tests := []struct {
		name    string
		envVal  string
		want    []int
		wantErr bool
	}{
		{
			name:    "valid comma-separated ints",
			envVal:  "1,2,3",
			want:    []int{1, 2, 3},
		},
		{
			name:    "valid-comma-separated ints with spaces",
			envVal:  "1, 2, 3",
			want:    []int{1, 2, 3},
		},
		{
			name:    "invalid int values",
			envVal:  "1,foo,bar",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			os.Clearenv()
			os.Setenv("TEST_VAL", tt.envVal)
			got, err := envVarAsIntSlice("TEST_VAL")

			if (tt.wantErr) != (err != nil) {
				t.Errorf("wantErr: %v, error: %v", tt.wantErr, err)
			}

			if diff := deep.Equal(tt.want, got); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestEnvVarAsBool(t *testing.T) {
	defer os.Clearenv()

	tests := []struct {
		name    string
		envVal  string
		want    bool
	}{
		{
			name:    "true value",
			envVal:  "true",
			want:    true,
		},
		{
			name:   "1 value",
			envVal: "1",
			want:   true,
		},
		{
			name:   "on value",
			envVal: "on",
			want:   true,
		},
		{
			name:   "false value",
			envVal: "false",
		},
		{
			name:   "off value",
			envVal: "off",
		},
		{
			name:   "0 value",
			envVal: "0",
		},
		{
			name:   "other value",
			envVal: "foo",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			os.Clearenv()
			os.Setenv("TEST_VAL", tt.envVal)
			if got := envVarAsBool("TEST_VAL"); tt.want != got {
				t.Errorf("wanted: %v, got: %v", tt.want, got)
			}
		})
	}
}