package locker

import (
	"context"
	"fmt"
	"strings"
)

// Prefixed returns a locker.Factory that scopes every lock name under prefix
// before delegating to inner. The prefix is concatenated to the caller's name
// with no separator, matching the convention used by storage.Prefixed:
// Prefixed("/ns", inner).NewLocker(ctx, "/lock", …) calls
// inner.NewLocker(ctx, "/ns/lock", …).
//
// An empty prefix yields a transparent passthrough. A non-empty prefix must
// start with "/" (else ErrPrefixNoLeadingSlash) and must not end with "/"
// (else ErrPrefixTrailingSlash). Interior "/" separators are allowed
// (e.g. "/foo/bar").
//
// Composition: Prefixed("/a", Prefixed("/b", inner)) ≡ Prefixed("/a/b", inner).
// Nested wrappers are flattened at construction so the outer prefix is the
// leftmost segment, mirroring storage.Prefixed.
func Prefixed(prefix string, inner Factory) (Factory, error) {
	if prefix != "" {
		if !strings.HasPrefix(prefix, "/") {
			return nil, fmt.Errorf("%w: %q", ErrPrefixNoLeadingSlash, prefix)
		}

		if strings.HasSuffix(prefix, "/") {
			return nil, fmt.Errorf("%w: %q", ErrPrefixTrailingSlash, prefix)
		}
	}

	if prefix == "" {
		return inner, nil
	}

	if existing, ok := inner.(*prefixedFactory); ok {
		return &prefixedFactory{
			prefix: prefix + existing.prefix,
			inner:  existing.inner,
		}, nil
	}

	return &prefixedFactory{prefix: prefix, inner: inner}, nil
}

type prefixedFactory struct {
	prefix string
	inner  Factory
}

var _ Factory = (*prefixedFactory)(nil)

func (p *prefixedFactory) NewLocker(ctx context.Context, name string, opts ...Option) (Locker, error) {
	lock, err := p.inner.NewLocker(ctx, p.prefix+name, opts...)
	if err != nil {
		return nil, fmt.Errorf("new-locker: %w", err)
	}

	return lock, nil
}
