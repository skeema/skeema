package tengo

// Sequence represents a MariaDB sequence object.
type Sequence struct {
	Name      string     `json:"name"`
	Type      ColumnType `json:"type"` // default is bigint (signed), but can be overridden in MariaDB 10.5+
	Increment int64      `json:"increment"`
	Minimum   string     `json:"minimum"`    // string due to valid range being from math.MinInt64 to math.MaxUint64
	Maximum   string     `json:"maximum"`    // string due to valid range being from math.MinInt64 to math.MaxUint64
	Start     string     `json:"start"`      // string due to valid range being from math.MinInt64 to math.MaxUint64
	CacheSize uint64     `json:"cache_size"` // 0 for no cache
	Cycle     bool       `json:"cycle,omitempty"`
	Comment   string     `json:"comment,omitempty"`
}
