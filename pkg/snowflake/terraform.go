package snowflake

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type TFResource interface {
	// Address is combination of a resource type and a resource name.

	Address() string // Example: snowflake_schema.example

	ResourceName() string // ResourceName is the name of a resource
	ID() string           // ID is the unique ID representing a resource
	HCL() []byte          // HCL generates the Hashicorp Configuration Language of a resource
}

func JoinToLower(sep string, terms ...string) string {
	if len(terms) == 0 {
		panic("bad usage.")
	}
	return strings.ToLower(strings.Join(terms, sep))
}

func GenerateTFImport(resourceType, resourceName, ID string) string {
	return fmt.Sprintf(`terraform import %v.%v "%v"`, resourceType, resourceName, ID)
}

func GenerateProvider(outputDir string) error {
	const provider = `terraform {
	required_providers {
	  snowflake = {
		source  = "chanzuckerberg/snowflake"
		version = "0.32.0"
		  }
		}
	  }`

	out := filepath.Join(outputDir, "provider.tf")
	return ioutil.WriteFile(out, []byte(provider), 0644)
}
