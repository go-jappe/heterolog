package slack

import (
	"errors"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "valid",
			cfg: Config{
				Token:   "xoxb-test-token",
				Channel: "test-channel",
			},
		},
		{
			name: "empty token",
			cfg: Config{
				Channel: "test-channel",
			},
			want: ErrTokenRequired,
		},
		{
			name: "whitespace token",
			cfg: Config{
				Token:   " \t\n",
				Channel: "test-channel",
			},
			want: ErrTokenRequired,
		},
		{
			name: "empty channel",
			cfg: Config{
				Token: "xoxb-test-token",
			},
			want: ErrChannelRequired,
		},
		{
			name: "whitespace channel",
			cfg: Config{
				Token:   "xoxb-test-token",
				Channel: " \t\n",
			},
			want: ErrChannelRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.cfg)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("ValidateConfig() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("ValidateConfig() error = %v, want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}
