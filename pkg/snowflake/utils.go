package snowflake

import (
	"database/sql"
	"log"
	"strings"

	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
)

type helper struct {
	Body *hclwrite.Body
	File *hclwrite.File
}

func (h helper) String() string {
	return string(h.File.Bytes())
}

func (h helper) SetAttributeString(k string, v string) helper {
	if len(v) > 0 {
		h.Body.SetAttributeValue(k, cty.StringVal(v))
	}
	return h
}

func (h helper) SetAttributeList(k string, vals []string) helper {
	v, err := gocty.ToCtyValue(vals, cty.List(cty.String))
	if err != nil {
		log.Fatalln(err)
	}
	h.Body.SetAttributeValue(k, v)
	return h
}

func (h helper) LineBreak() helper {
	h.Body.AppendNewline()
	return h
}

func SetAttributeIfNotEmpty(body *hclwrite.Body, k, v string) {
	if len(v) > 0 {
		body.SetAttributeValue(k, cty.StringVal(v))
	}
}

func (h helper) SetTableColumn(c Column) helper {
	colBody := h.Body.AppendNewBlock("column", nil).Body()
	colBody.SetAttributeValue("name", cty.StringVal(c.name))
	colBody.SetAttributeValue("type", cty.StringVal(c._type))

	if c._default != nil {
		SetAttributeIfNotEmpty(colBody, "default", c._default.expression)
	}

	colBody.SetAttributeValue("nullable", cty.BoolVal(c.nullable))

	SetAttributeIfNotEmpty(colBody, "comment", c.comment)
	return h
}

func (h helper) SetAttributeNullString(k string, v sql.NullString) helper {
	if len(v.String) > 0 {
		h.Body.SetAttributeValue(k, cty.StringVal(v.String))
	}
	return h
}

func (h helper) SetAttributeBool(k string, v bool) helper {
	h.Body.SetAttributeValue(k, cty.BoolVal(v))
	return h
}

func (h helper) SetAttributeNullBool(k string, v sql.NullBool) helper {
	h.Body.SetAttributeValue(k, cty.BoolVal(v.Bool))
	return h
}

func buildTerraformHelper(resource, name string) helper {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	newBlock := rootBody.AppendNewBlock("resource", []string{resource, strings.ToLower(name)})
	tableBody := newBlock.Body()
	return helper{
		File: f,
		Body: tableBody,
	}
}
