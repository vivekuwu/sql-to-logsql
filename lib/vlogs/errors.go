package vlogs

type APIError struct {
	Code    int
	Message string
	Err     error
}

func (e *APIError) Error() string {
	return e.Message
}

func (e *APIError) Unwrap() error {
	return e.Err
}
