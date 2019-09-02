package pulsar

import "strings"

var SPACES = "    "
var USED_FOR = "USED FOR:"
var PERMISSION = "REQUIRED PERMISSION:"
var EXAMPLES = "EXAMPLES:"
var OUTPUT = "OUTPUT:"

type LongDescription struct {
	CommandUsedFor    string
	CommandPermission string
	CommandExamples   []Example
	CommandOutput     []Output
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
	return USED_FOR + "\n" +
		SPACES + desc.CommandUsedFor + "\n\n" +
		PERMISSION + "\n" +
		SPACES + desc.CommandPermission + "\n\n" +
		EXAMPLES + "\n" +
		desc.exampleToString() +
		OUTPUT + "\n" +
		desc.outputToString()
}

func (desc *LongDescription) exampleToString() string {
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
