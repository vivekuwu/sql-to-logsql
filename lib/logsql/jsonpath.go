package logsql

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode"
)

type jsonPath struct {
	Strict bool
	Steps  []jsonPathStep
}

type jsonPathStep struct {
	Key   string
	Index *int
}

func parseJSONPath(raw string) (*jsonPath, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JSON path cannot be empty",
		}
	}

	path := strings.TrimSpace(raw)
	lower := strings.ToLower(path)
	result := &jsonPath{}

	switch {
	case strings.HasPrefix(lower, "strict "):
		result.Strict = true
		path = strings.TrimSpace(path[len("strict "):])
	case strings.HasPrefix(lower, "lax "):
		result.Strict = false
		path = strings.TrimSpace(path[len("lax "):])
	}

	if path == "" || path[0] != '$' {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JSON path must start with $",
		}
	}
	path = path[1:]
	runes := []rune(path)

	i := 0
	for {
		skipSpaces(runes, &i)
		if i >= len(runes) {
			break
		}

		switch runes[i] {
		case '.':
			i++
			skipSpaces(runes, &i)
			if i >= len(runes) {
				return nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: JSON path ends unexpectedly after .",
				}
			}
			start := i
			for i < len(runes) {
				r := runes[i]
				if r == '.' || r == '[' {
					break
				}
				i++
			}
			if start == i {
				return nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: JSON path contains empty segment",
				}
			}
			key := strings.TrimSpace(string(runes[start:i]))
			if key == "" {
				return nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: JSON path contains empty segment",
				}
			}
			result.Steps = append(result.Steps, jsonPathStep{Key: key})
		case '[':
			i++
			skipSpaces(runes, &i)
			if i >= len(runes) {
				return nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: JSON path has unterminated []",
				}
			}
			step, err := parseBracketStep(runes, &i)
			if err != nil {
				return nil, err
			}
			result.Steps = append(result.Steps, step)
			skipSpaces(runes, &i)
			if i >= len(runes) || runes[i] != ']' {
				return nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: JSON path has unterminated []",
				}
			}
			i++
		default:
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported JSON path token %q", string(runes[i])),
			}
		}
	}

	if len(result.Steps) == 0 {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JSON path must reference nested field",
		}
	}
	return result, nil
}

func (p *jsonPath) HasOnlyKeys() ([]string, bool) {
	if p == nil {
		return nil, false
	}
	keys := make([]string, 0, len(p.Steps))
	for _, step := range p.Steps {
		if step.Index != nil || step.Key == "" {
			return nil, false
		}
		keys = append(keys, step.Key)
	}
	return keys, true
}

func parseBracketStep(runes []rune, pos *int) (jsonPathStep, error) {
	if *pos >= len(runes) {
		return jsonPathStep{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JSON path has unterminated []",
		}
	}
	switch runes[*pos] {
	case '\'', '"':
		quote := runes[*pos]
		*pos++
		var builder strings.Builder
		for *pos < len(runes) {
			r := runes[*pos]
			*pos++
			if r == '\\' {
				if *pos >= len(runes) {
					return jsonPathStep{}, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: "translator: JSON path has invalid escape sequence",
					}
				}
				builder.WriteRune(runes[*pos])
				*pos++
				continue
			}
			if r == quote {
				break
			}
			builder.WriteRune(r)
		}
		key := strings.TrimSpace(builder.String())
		if key == "" {
			return jsonPathStep{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: JSON path contains empty segment",
			}
		}
		return jsonPathStep{Key: key}, nil
	default:
		start := *pos
		for *pos < len(runes) && unicode.IsDigit(runes[*pos]) {
			*pos++
		}
		if start == *pos {
			return jsonPathStep{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: JSON path contains unsupported token inside []",
			}
		}
		value := string(runes[start:*pos])
		index, err := strconv.Atoi(value)
		if err != nil {
			return jsonPathStep{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: JSON path contains unsupported index",
				Err:     err,
			}
		}
		return jsonPathStep{Index: &index}, nil
	}
}

func skipSpaces(runes []rune, pos *int) {
	for *pos < len(runes) && unicode.IsSpace(runes[*pos]) {
		*pos++
	}
}
