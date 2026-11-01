package main

import (
	"os"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	cli "github.com/go-jappe/heterolog/internal/presentation/cli/logdecrypt"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr, cli.Dependencies{
		Keygen:  bootstrap.GenerateEncryptionIdentity,
		Decrypt: bootstrap.DecryptFile,
	}))
}
