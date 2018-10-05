package position

type TimePosition struct {
	Timestamp int64
}

func NewTimePosition(timestamp int64) *TimePosition {
	tstamp := &TimePosition{Timestamp: timestamp}
	return tstamp
}
