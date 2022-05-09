# bariloche

Bariloche is a tool for generating terraform files and state from existing Snowflake.com

## install

`go install github.com/ezeql/bariloche@latest`

[Inspired by terraformer](https://github.com/GoogleCloudPlatform/terraformer/)

## Why?

## Config

## Description

[Provider](https://github.com/chanzuckerberg/terraform-provider-snowflake)

[A lot of code from](https://github.com/chanzuckerberg/terraform-provider-snowflake)

Configuration:

Commands:

```bash
# TODO: missing flags
bariloche connTest #checks snowflake connection
bariloche generateAll #generates all resources
bariloche generateDatabases # generates all databases
bariloche generatePipes # generates all pipes
bariloche generateProvider # generates all provider
bariloche generateRoles # generates all roles
bariloche generateSchema # generates all schema
bariloche generateStages # generates all stages
bariloche generateTables # generates all tables
bariloche generateUsers # generates all users
bariloche generateViews # generates all views
bariloche generateWarehouses # generates all warehouses
```

## What's Bariloche?
