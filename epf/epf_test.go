package epf

import (
	"fmt"
	"os"
	"testing"
)

func TestVersion(t *testing.T) {
	_, _, err := Version()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLogin(t *testing.T) {
	_, err := login()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoginFailed(t *testing.T) {
	_, err := Login("foo", "bar")
	if err == nil {
		t.Fatal("expected login failure")
	}
}

func TestFiles(t *testing.T) {
	sess := mustLogin()
	_, err := sess.Files()
	if err != nil {
		t.Fatal(err)
	}
}

func login() (*Session, error) {
	return Login(mustGetenv("EPF_EMAIL"), mustGetenv("EPF_PASSWORD"))
}

func mustLogin() *Session {
	sess, err := login()
	if err != nil {
		panic(fmt.Errorf("login failed: %v", err))
	}
	return sess
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
