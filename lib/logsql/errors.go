package logsql

type TranslationError struct {
	Code    int
	Message string
	Err     error
}

func (e *TranslationError) Error() string {
	return e.Message
}

func (e *TranslationError) Unwrap() error {
	return e.Err
}
