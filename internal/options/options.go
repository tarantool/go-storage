package options

type OptionConstructor[T any] func() T

type OptionCallback[T any] func(*T)

func ApplyOptions[T any](constructor OptionConstructor[T], cbs []OptionCallback[T]) T {
	var opts T

	if constructor != nil {
		opts = constructor()
	}

	for _, cb := range cbs {
		cb(&opts)
	}

	return opts
}
