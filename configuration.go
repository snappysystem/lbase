package lbase

/*
 lbase configuration file uses json format. A typical configuration
 looks like:

{
  "StringProperties" : [
     {
       "Name": "lbase.tmp.dir",
       "Value": "/tmp/lbase-${user.name}",
     },

     {
       "Name": "lbase.configuration",
       "Value": "http://localhost:54310",
     },
  ],

  "IntProperties": [
     {
       "Name": "lbase.replication",
       "Value": 3,
     }
  ],

  "ListProperties": [
     {
       "Name": "lbase.server_name",
       "Value": [
         "10.0.2.1:8001",
         "10.0.2.2:8001",
         "10.0.2.3:8001",
         "10.0.2.4:8001",
       ],
     },
  ],
}
*/

import (
	"io"
)

type StringProperty struct {
	Name, Value string
}

type IntProperty struct {
	Name string
	Value int64
}

type Float64Property struct {
	Name string
	Value float64
}

type ListProperty struct {
	Name string
	Value []string
}

type Configuration struct {
	StringProperties []StringProperty
	IntProperties []IntProperty
	Float64Properties []Float64Property
	ListProperties []ListProperty
}

// Add and merge resource from an input stream.
func (c *Configuration) AddResource(reader io.Reader) {
}

// Get the value of named property.
func (c *Configuration) Get(name, defaultValue string) string {
	return ""
}

// Get the value of named property.
func (c *Configuration) GetInt64(name string, defaultValue int64) int64 {
	return 0
}

// Get the value of named property.
func (c *Configuration) GetFloat64(name string, defaultValue float64) float64 {
	return 0
}

// Get the value of named property.
func (c *Configuration) GetStrings(name string) []string {
	return []string{}
}

// Unset a previously set property.
func (c *Configuration) Unset(name string) {
}

// Set a property.
func (c *Configuration) Set(name string, value string) {
}

// Set a property.
func (c *Configuration) SetInt64(name string, value int64) {
}

// Set a property.
func (c *Configuration) SetFloat64(name string, value float64) {
}

// Set a property.
func (c *Configuration) SetStrings(name string, value []string) {
}

// Write the configuration to an output stream.
func (c *Configuration) WriteJson(s io.Writer) {
}
