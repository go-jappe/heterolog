package endpointurl

import (
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// 解析できるURLは、user infoを含めてurl.Parseと同じ結果を返します。
func TestParseMatchesURLParseForValidURLs(t *testing.T) {
	for _, raw := range []string{
		"https://logs.example.test:9200",
		"https://endpoint-user:S3cret-Passw0rd@logs.example.test:9200/proxy%2Fpath",
		"http://[::1]:3100/loki/api/v1/push",
	} {
		got, err := Parse(raw)
		want, wantErr := url.Parse(raw)
		if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Parse(%q) = %#v, %v; want url.Parse result %#v, %v", raw, got, err, want, wantErr)
		}
	}
}

// user infoを含まない入力の解析エラーは、url.Parseのerrorをそのまま返します。
func TestParseKeepsErrorsWithoutUserInfo(t *testing.T) {
	for _, raw := range []string{"https://logs.example.test/%zz", "https://logs example.test/", ":9200"} {
		_, err := Parse(raw)
		_, want := url.Parse(raw)
		if want == nil || !reflect.DeepEqual(err, want) {
			t.Fatalf("Parse(%q) error = %#v, want the url.Parse error %#v", raw, err, want)
		}
	}
}

// 解析できないURLにuser infoがあれば、errorの文面と*url.Errorのフィールドにユーザー名・パスワードを
// 含めません。資格情報が文面に出ないことが要件なので、文面を直接確認します。user info以外の原因は、
// user infoを置き換えたURLの解析エラーとして残し、user infoが原因ならそのことを示します。
func TestParseDoesNotExposeUserInfoInErrors(t *testing.T) {
	const user, password = "endpoint-user", "S3cret-Passw0rd"
	userInfo := user + ":" + password
	for _, tt := range []struct {
		name string
		raw  string
		// wantURLは、errorの*url.Errorが示すuser infoを置き換えたURLです。
		wantURL string
		// invalidUserInfoは、user infoを置き換えると解析できるため、user infoが原因と報告することを表します。
		invalidUserInfo bool
	}{
		{
			name:    "invalid path escape",
			raw:     "https://" + userInfo + "@logs.example.test/%zz",
			wantURL: "https://xxxxx@logs.example.test/%zz",
		},
		{
			name:    "invalid port",
			raw:     "https://" + userInfo + "@logs.example.test:port/",
			wantURL: "https://xxxxx@logs.example.test:port/",
		},
		{
			name:    "at sign in the password",
			raw:     "https://" + user + ":" + password + "@x@logs.example.test/%zz",
			wantURL: "https://xxxxx@logs.example.test/%zz",
		},
		{
			name:    "missing scheme",
			raw:     ":" + userInfo + "@logs.example.test",
			wantURL: ":xxxxx@logs.example.test",
		},
		{
			name:            "invalid escape in the password",
			raw:             "https://" + userInfo + "%zz@logs.example.test/",
			wantURL:         "https://xxxxx@logs.example.test/",
			invalidUserInfo: true,
		},
		{
			// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
			name:            "unescaped slash in the password",
			raw:             "https://" + user + ":S3cret/Passw0rd@logs.example.test/",
			wantURL:         "https://xxxxx@logs.example.test/",
			invalidUserInfo: true,
		},
		{
			name:            "unescaped fragment delimiter in the password",
			raw:             "https://" + user + ":S3cret#Passw0rd@logs.example.test/",
			wantURL:         "https://xxxxx@logs.example.test/",
			invalidUserInfo: true,
		},
		{
			name:            "unescaped query delimiter in the password",
			raw:             "https://" + user + ":S3cret?Passw0rd@logs.example.test/",
			wantURL:         "https://xxxxx@logs.example.test/",
			invalidUserInfo: true,
		},
		{
			name:            "control character in the password",
			raw:             "https://" + user + ":S3cret\x7fPassw0rd@logs.example.test/",
			wantURL:         "https://xxxxx@logs.example.test/",
			invalidUserInfo: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := url.Parse(tt.raw); err == nil {
				t.Fatalf("url.Parse(%q) succeeded; the case must exercise a parse error", tt.raw)
			}
			parsed, err := Parse(tt.raw)
			var urlErr *url.Error
			if parsed != nil || !errors.As(err, &urlErr) || urlErr.Op != "parse" || urlErr.URL != tt.wantURL {
				t.Fatalf("Parse(%q) = %v, %#v; want a parse *url.Error for %q", tt.raw, parsed, err, tt.wantURL)
			}
			if tt.invalidUserInfo {
				if !errors.Is(urlErr.Err, errInvalidUserInfo) {
					t.Fatalf("Parse(%q) cause = %v, want the invalid user info", tt.raw, urlErr.Err)
				}
			} else if _, want := url.Parse(tt.wantURL); !reflect.DeepEqual(err, want) {
				t.Fatalf("Parse(%q) error = %#v, want the parse error of the redacted URL %#v", tt.raw, err, want)
			}
			for _, secret := range []string{user, "S3cret", "Passw0rd"} {
				if strings.Contains(err.Error(), secret) || strings.Contains(urlErr.URL, secret) || strings.Contains(urlErr.Err.Error(), secret) {
					t.Fatalf("Parse(%q) error %q exposes %q from the user info", tt.raw, err, secret)
				}
			}
		})
	}
}
