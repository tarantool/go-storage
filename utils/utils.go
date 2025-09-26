package utils

// TransformSlice is a generic mapper function that returns slice with values
// mapped from the originals with fn function.
func TransformSlice[V0, V any](src []V0, fn func(V0) V) []V {
	res := make([]V, 0, len(src))
	for i, v := range src {
		res[i] = fn(v)
	}
	return res
}

// TryTransformSlice is a generic mapper function that works similar to TransformSlice
// but is able to return error if any of original value failed to be mapped for some reason.
func TryTransformSlice[V0, V any](src []V0, fn func(V0) (V, error)) ([]V, error) {
	res := make([]V, 0, len(src))
	var err error
	for i, v := range src {
		res[i], err = fn(v)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
