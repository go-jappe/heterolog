package heterolog_test

import (
	"context"
	"log"

	"github.com/go-jappe/heterolog"
)

// LoadEnvConfig reads HETEROLOG_ variables and the named env file without
// changing the process environment. Unset settings use the defaults, which
// write to stdout.
func ExampleLoadEnvConfig() {
	env, err := heterolog.LoadEnvConfig(".env")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	runtime, err := env.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			log.Print(err)
		}
	}()

	if err := runtime.Info(ctx, "service started"); err != nil {
		log.Print(err)
	}
}
