// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package common

import (
	"strings"
)

var SPACES = "    "
var USEDFOR = "USED FOR:"
var PERMISSION = "REQUIRED PERMISSION:"
var EXAMPLES = "EXAMPLES:"
var OUTPUT = "OUTPUT:"
var SCOPE = "SCOPE:"

type LongDescription struct {
	CommandUsedFor    string
	CommandPermission string
	CommandExamples   []Example
	CommandOutput     []Output
	CommandScope      string
}

type Example struct {
	Desc    string
	Command string
}

type Output struct {
	Desc string
	Out  string
}

func (desc *LongDescription) ToString() string {
	out := USEDFOR + "\n" +
		SPACES + desc.CommandUsedFor + "\n\n" +
		PERMISSION + "\n" +
		SPACES + desc.CommandPermission + "\n\n" +
		OUTPUT + "\n" +
		desc.outputToString()

	if desc.CommandScope != "" {
		out += SCOPE + "\n" +
			SPACES + desc.CommandScope + "\n\n"
	}

	return out
}

func (desc *LongDescription) ExampleToString() string {
	var result string
	for _, v := range desc.CommandExamples {
		result += SPACES + "#" + v.Desc + "\n" + SPACES + v.Command + "\n\n"
	}
	return result
}

func (desc *LongDescription) outputToString() string {
	var result string
	for _, v := range desc.CommandOutput {
		result += SPACES + "#" + v.Desc + "\n" + makeSpace(v.Out) + "\n"
	}
	return result
}

func makeSpace(s string) string {
	var res string
	lines := strings.Split(s, "\n")
	for _, l := range lines {
		res += SPACES + l + "\n"
	}
	return res
}
