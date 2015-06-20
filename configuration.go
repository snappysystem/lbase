/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
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
	Name  string
	Value int64
}

type Float64Property struct {
	Name  string
	Value float64
}

type ListProperty struct {
	Name  string
	Value []string
}

type Configuration struct {
	StringProperties  []StringProperty
	IntProperties     []IntProperty
	Float64Properties []Float64Property
	ListProperties    []ListProperty
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
