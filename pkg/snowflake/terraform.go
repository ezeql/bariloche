package snowflake

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
)

type TFResource interface {
	Address() string
	ID() string
	HCL() []byte
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
