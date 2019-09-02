package pulsar

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLongDescription_exampleToString(t *testing.T) {
	desc := LongDescription{}
	example := Example{
		Desc:    "command description",
		Command: "command",
	}
	desc.CommandExamples = []Example{example}
	res := desc.exampleToString()

	expect := "    #command description\n" +
		"    command\n\n"

	assert.Equal(t, expect, res)
}

func TestLongDescription_ToString(t *testing.T) {
	desc := LongDescription{}
	desc.CommandUsedFor = "command used for"
	desc.CommandPermission = "command permission"
	example := Example{}
	example.Desc = "command description"
	example.Command = "command"
	desc.CommandExamples = []Example{example}
	out := Output{
		Desc: "Output",
		Out:  "Out line 1\nOut line 2",
	}
	desc.CommandOutput = []Output{out}

	expect := "USED FOR:\n" +
		"    " + desc.CommandUsedFor + "\n\n" +
		"REQUIRED PERMISSION:\n" +
		"    " + desc.CommandPermission + "\n\n" +
		"EXAMPLES:\n" +
		"    " + "#" + example.Desc + "\n" +
		"    " + example.Command + "\n\n" +
		"OUTPUT:\n" +
		"    " + "#" + out.Desc + "\n" +
		"    " + "Out line 1" + "\n" +
		"    " + "Out line 2" + "\n\n"

	result := desc.ToString()

	assert.Equal(t, expect, result)
}
