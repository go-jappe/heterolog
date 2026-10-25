package clickhouse

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name: "valid config with zero timeout sentinel",
			cfg: Config{
				Endpoint: "http://localhost:8123",
				Table:    "logs",
			},
		},
		{
			name: "negative timeout",
			cfg: Config{
				Endpoint: "http://localhost:8123",
				Table:    "logs",
				Timeout:  -time.Second,
			},
			wantErr: ErrNegativeTimeout,
		},
		{
			name: "rejects unsupported endpoint scheme",
			cfg: Config{
				Endpoint: "ftp://localhost:8123",
				Table:    "logs",
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "missing endpoint",
			cfg: Config{
				Table: "logs",
			},
			wantErr: ErrEndpointRequired,
		},
		{
			name: "endpoint lacks scheme and host",
			cfg: Config{
				Endpoint: "localhost:8123",
				Table:    "logs",
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			// schemeはhttpなので、host欠落の検査だけが拒否理由になります。
			name: "endpoint lacks host",
			cfg: Config{
				Endpoint: "http:///logs",
				Table:    "logs",
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains user info",
			cfg: Config{
				Endpoint: "http://user:password@localhost:8123",
				Table:    "logs",
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "endpoint contains query",
			cfg: Config{
				Endpoint: "http://localhost:8123?query=select",
				Table:    "logs",
			},
			wantErr: ErrInvalidEndpoint,
		},
		{
			name: "missing table",
			cfg: Config{
				Endpoint: "http://localhost:8123",
			},
			wantErr: ErrTableRequired,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateConfig(tt.cfg)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// endpointのURLにuser info（ユーザー名・パスワード）があり解析できない場合も、errorの文面と、原因として
// 残すnet/urlの*url.Errorにuser infoを含めません。資格情報を文面へ出さないことが要件なので、
// 文面を直接確認します。
func TestValidateConfigDoesNotExposeEndpointUserInfo(t *testing.T) {
	t.Parallel()

	const user, password = "endpoint-user", "S3cret-Passw0rd"
	for _, endpoint := range []string{
		// pathのescapeが不正です。
		"https://" + user + ":" + password + "@clickhouse.example.test/%zz",
		// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
		"https://" + user + ":S3cret/Passw0rd@clickhouse.example.test/",
	} {
		err := ValidateConfig(Config{Endpoint: endpoint, Table: "logs"})
		require.ErrorIs(t, err, ErrInvalidEndpoint)
		var urlErr *url.Error
		require.ErrorAs(t, err, &urlErr, "the URL parse error must stay in the error chain")
		for _, secret := range []string{user, "S3cret", "Passw0rd"} {
			require.NotContains(t, err.Error(), secret, "error text must not expose the endpoint user info")
			require.NotContains(t, urlErr.URL, secret, "the parse error URL must not expose the endpoint user info")
		}
	}
}
