package config

import (
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/pkg/env"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestLoadSpillTypesUsesDefaultWhenEnvironmentIsMissingOrBlank(t *testing.T) {
	defaults := []pkgdurable.SpillType{pkgdurable.SpillType("stdout")}
	if got := loadSpillTypes(
		env.GetEnvSliceString("MISSING_DIRECT_DEGRADE_SPILL_TYPES_TEST", nil),
		defaults,
	); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("missing environment spill types = %v, want %v", got, defaults)
	}

	key := "EXPLICIT_DIRECT_DEGRADE_SPILL_TYPES_TEST"
	t.Setenv(key, "")
	if got := loadSpillTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("explicit empty spill types = %v, want defaults %v", got, defaults)
	}

	t.Setenv(key, " \t ")
	if got := loadSpillTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("whitespace spill types = %v, want defaults %v", got, defaults)
	}

	t.Setenv(key, " STDOUT,Unknown,stdout ")
	want := []pkgdurable.SpillType{
		pkgdurable.SpillType("stdout"),
		pkgdurable.SpillType("unknown"),
		pkgdurable.SpillType("stdout"),
	}
	if got := loadSpillTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit spill types = %v, want %v", got, want)
	}
}

func TestLoadSpillTypesPreservesExplicitNone(t *testing.T) {
	key := "EXPLICIT_NONE_DIRECT_DEGRADE_SPILL_TYPES_TEST"
	t.Setenv(key, " NONE ")

	got := loadSpillTypes(env.GetEnvSliceString(key, nil), []pkgdurable.SpillType{pkgdurable.SpillType("stdout")})
	want := []pkgdurable.SpillType{pkgdurable.SpillTypeNone}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit none spill types = %v, want %v", got, want)
	}
}
