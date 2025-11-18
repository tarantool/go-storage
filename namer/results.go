package namer

import (
	"iter"
)

// Results represents Namer working result.
type Results struct {
	isSingle     bool             // True if result contains only one object name.
	isSingleName string           // Cached name when isSingle=true.
	result       map[string][]Key // Grouped keys: object name -> key list.
}

func getFirstFromMap(m map[string][]Key) string {
	for name := range m {
		return name
	}

	return ""
}

// NewResults creates a new Results instance from the provided initial data.
func NewResults(initial map[string][]Key) Results {
	return Results{
		isSingle:     len(initial) == 1,
		isSingleName: getFirstFromMap(initial),
		result:       initial,
	}
}

// SelectSingle gets keys for single-name case (if applicable).
func (r *Results) SelectSingle() ([]Key, bool) {
	if r.isSingle {
		return r.result[r.isSingleName], true
	}

	return nil, false
}

// Items return iterator over all name->keys groups.
func (r *Results) Items() iter.Seq2[string, []Key] {
	return func(yield func(str string, res []Key) bool) {
		for i, v := range r.result {
			if !yield(i, v) {
				return
			}
		}
	}
}

// Result returns the underlying results map.
func (r *Results) Result() map[string][]Key {
	return r.result
}

// Select gets keys for a specific object name.
func (r *Results) Select(name string) ([]Key, bool) {
	if i, ok := r.result[name]; ok {
		return i, true
	}

	return nil, false
}

// Len returns the number of unique object names.
func (r *Results) Len() int {
	return len(r.result)
}
