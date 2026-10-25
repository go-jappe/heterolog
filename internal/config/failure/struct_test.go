package failure

import (
	"reflect"
	"testing"

	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestParseSpillDestinationPreservesUnknownValueForFactoryValidation(t *testing.T) {
	got := ParseSpillDestination("  Unsupported-Store  ", pkgdurable.SpillDestinationStdout)
	if want := pkgdurable.SpillDestination("unsupported-store"); got != want {
		t.Fatalf("ParseSpillDestination = %q, want %q", got, want)
	}
}

func TestParseSpillDestinationUsesDefaultOnlyForEmptyValue(t *testing.T) {
	got := ParseSpillDestination("   ", pkgdurable.SpillDestinationStdout)
	if got != pkgdurable.SpillDestinationStdout {
		t.Fatalf("ParseSpillDestination = %q, want stdout", got)
	}
}

func TestParseSpillTypesParsesDirectDegradeDestinations(t *testing.T) {
	got := ParseSpillTypes([]string{" stdout ", "S3", "nfs", "NONE"})
	want := []pkgdurable.SpillType{
		DegradeDestinationStdout,
		DegradeDestinationS3,
		DegradeDestinationNfs,
		pkgdurable.SpillTypeNone,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseSpillTypes() = %v, want %v", got, want)
	}
}

func TestParseSpillTypesPreservesInvalidAndDuplicateValuesForValidation(t *testing.T) {
	got := ParseSpillTypes([]string{" stdout ", " Future-Store ", "", "STDOUT"})
	want := []pkgdurable.SpillType{
		DegradeDestinationStdout,
		pkgdurable.SpillType("future-store"),
		pkgdurable.SpillType(""),
		DegradeDestinationStdout,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseSpillTypes() = %v, want %v", got, want)
	}
}

func TestParseOverloadPolicyUsesDefaultOnlyForEmptyValue(t *testing.T) {
	if got := ParseOverloadPolicy(" \t\n", pkgdurable.OverloadPolicyDrop); got !=
		pkgdurable.OverloadPolicyDrop {
		t.Fatalf("ParseOverloadPolicy(empty) = %q, want drop", got)
	}
	if got := ParseOverloadPolicy(
		" DeGrAdE ",
		pkgdurable.OverloadPolicyDrop,
	); got != pkgdurable.OverloadPolicyDegrade {
		t.Fatalf("ParseOverloadPolicy(degrade) = %q, want degrade", got)
	}
	if got := ParseOverloadPolicy(
		" Future-Policy ",
		pkgdurable.OverloadPolicyDrop,
	); got != pkgdurable.OverloadPolicy("future-policy") {
		t.Fatalf("ParseOverloadPolicy(unknown) = %q, want future-policy", got)
	}
}
