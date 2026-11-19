package auth

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Haklifesavr/switchyard/internal/config"
)

func TestAuthenticate(t *testing.T) {
	a := New(config.Auth{Keys: []config.Key{{Name: "web", SHA256: strings.ToUpper(Hash("sy_secret")), RequestsPerMinute: 10}}})
	cases := []struct {
		header string
		want   error
	}{
		{"", ErrMissing},
		{"Basic abc", ErrMissing},
		{"Bearer ", ErrMissing},
		{"Bearer nope", ErrInvalid},
		{"Bearer sy_secret", nil},
	}
	for _, c := range cases {
		r := httptest.NewRequest("POST", "/", nil)
		if c.header != "" {
			r.Header.Set("Authorization", c.header)
		}
		k, err := a.Authenticate(r)
		if err != c.want {
			t.Errorf("%q: err = %v, want %v", c.header, err, c.want)
		}
		if c.want == nil && (k == nil || k.Name != "web" || k.RequestsPerMinute != 10) {
			t.Errorf("%q: key = %+v", c.header, k)
		}
	}
}

func TestDisabledAllowsAnonymous(t *testing.T) {
	k, err := New(config.Auth{Disabled: true}).Authenticate(httptest.NewRequest("GET", "/", nil))
	if err != nil || k != Anonymous {
		t.Fatalf("got %v %v", k, err)
	}
}

func TestGenerateIsUniqueAndHashable(t *testing.T) {
	a, _ := Generate()
	b, _ := Generate()
	if a == b || !strings.HasPrefix(a, "sy_") || len(a) != 51 || len(Hash(a)) != 64 {
		t.Fatalf("bad keys %q %q", a, b)
	}
}
