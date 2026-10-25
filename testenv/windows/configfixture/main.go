// configfixture generates an offline configuration root for Windows tests.
package main

import (
	"fmt"
	"github.com/go-jappe/heterolog/internal/testutil/configfixture"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: configfixture <public .env.example> <new output directory>")
		os.Exit(2)
	}
	if err := generateFixture(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("generated public configuration fixtures:", os.Args[2])
}

func generateFixture(source, destination string) error {
	return configfixture.Generate(source, destination)
}
