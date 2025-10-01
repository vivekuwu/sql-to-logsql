package viewstore

type StoreError struct {
	Code    int
	Message string
	Err     error
}

func (e *StoreError) Error() string {
	return e.Message
}

func (e *StoreError) Unwrap() error {
	return e.Err
}
