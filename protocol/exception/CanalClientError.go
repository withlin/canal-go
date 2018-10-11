package exception

type CanalClientError struct {
	msg string
}

func (e *CanalClientError) Error() string {
	return e.msg
}
