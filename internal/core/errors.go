package core

type YieldToConsumerError struct {
	Reason string
	Data   []byte
}

func (e *YieldToConsumerError) Error() string {
	return e.Reason
}
