package parser

import (
	"fmt"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/token"
)

// SyntaxError describes a parsing failure with source position context.
type SyntaxError struct {
	Pos token.Position
	Msg string
}

func (e *SyntaxError) Error() string {
	if e == nil {
		return ""
	}
	if e.Pos.Line > 0 && e.Pos.Column > 0 {
		return fmt.Sprintf("line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Msg)
	}
	return e.Msg
}
