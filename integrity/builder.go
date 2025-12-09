// Package integrity provides integrity-protected storage operations.
// It includes generators, validators, and builders for creating typed storage
// with hash and signature verification.
package integrity

import (
	"slices"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

// TypedBuilder builds typed storage instances with integrity protection.
type TypedBuilder[T any] struct {
	storage    storage.Storage
	hashers    []hasher.Hasher
	signers    []crypto.Signer
	verifiers  []crypto.Verifier
	marshaller marshaller.TypedYamlMarshaller[T]

	prefix string
	namer  namer.Namer
}

// NewTypedBuilder creates a new TypedBuilder for the given storage instance.
func NewTypedBuilder[T any](storageInstance storage.Storage) TypedBuilder[T] {
	return TypedBuilder[T]{
		storage:    storageInstance,
		hashers:    []hasher.Hasher{},
		signers:    []crypto.Signer{},
		verifiers:  []crypto.Verifier{},
		marshaller: marshaller.NewTypedYamlMarshaller[T](),

		prefix: "/",
		namer:  nil, // TODO: implement default namer.
	}
}

func (s TypedBuilder[T]) copy() TypedBuilder[T] {
	return TypedBuilder[T]{
		storage:    s.storage,
		hashers:    slices.Clone(s.hashers),
		signers:    slices.Clone(s.signers),
		verifiers:  slices.Clone(s.verifiers),
		marshaller: s.marshaller,

		prefix: s.prefix,
		namer:  s.namer,
	}
}

// WithHasher adds a hasher to the builder.
func (s TypedBuilder[T]) WithHasher(h hasher.Hasher) TypedBuilder[T] {
	out := s.copy()

	s.hashers = append(s.hashers, h)

	return out
}

// WithSignerVerifier adds a signer/verifier to the builder.
func (s TypedBuilder[T]) WithSignerVerifier(sv crypto.SignerVerifier) TypedBuilder[T] {
	out := s.copy()

	s.signers = append(s.signers, sv)
	s.verifiers = append(s.verifiers, sv)

	return out
}

// WithSigner adds a signer to the builder.
func (s TypedBuilder[T]) WithSigner(signer crypto.Signer) TypedBuilder[T] {
	out := s.copy()

	s.signers = append(s.signers, signer)

	return out
}

// WithVerifier adds a verifier to the builder.
func (s TypedBuilder[T]) WithVerifier(verifier crypto.Verifier) TypedBuilder[T] {
	out := s.copy()

	s.verifiers = append(s.verifiers, verifier)

	return out
}

// WithMarshaller sets the marshaller for the builder.
func (s TypedBuilder[T]) WithMarshaller(marshaller marshaller.TypedYamlMarshaller[T]) TypedBuilder[T] {
	out := s.copy()

	s.marshaller = marshaller

	return out
}

// WithPrefix sets the key prefix for the builder.
func (s TypedBuilder[T]) WithPrefix(prefix string) TypedBuilder[T] {
	out := s.copy()

	s.prefix = prefix

	return out
}

// WithNamer sets the namer for the builder.
func (s TypedBuilder[T]) WithNamer(namer namer.Namer) TypedBuilder[T] {
	out := s.copy()

	s.namer = namer

	return out
}

// Build creates a new Typed storage instance with the configured options.
func (s TypedBuilder[T]) Build() *Typed[T] {
	var defaultNamer *namer.DefaultNamer
	if s.namer == nil {
		defaultNamer = namer.NewDefaultNamer(s.prefix, []string{}, []string{})
	} else {
		var ok bool

		defaultNamer, ok = s.namer.(*namer.DefaultNamer)
		if !ok {
			panic("namer must be *namer.DefaultNamer")
		}
	}

	gen := NewGenerator(
		defaultNamer,
		s.marshaller,
		s.hashers,
		s.signers,
	)

	val := NewValidator(
		defaultNamer,
		s.marshaller,
		s.hashers,
		s.verifiers,
	)

	return &Typed[T]{
		base:  s.storage,
		gen:   gen,
		val:   val,
		namer: defaultNamer,
	}
}
