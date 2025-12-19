package integrity

import (
	"maps"
	"slices"

	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
)

type NamerConstructor func(prefix string, hashNames []string, sigNames []string) namer.Namer

// TypedBuilder builds typed storage instances with integrity protection.
type TypedBuilder[T any] struct {
	storage    storage.Storage
	hashers    []hasher.Hasher
	signers    []crypto.Signer
	verifiers  []crypto.Verifier
	marshaller marshaller.TypedYamlMarshaller[T]

	prefix    string
	namerFunc NamerConstructor
}

// NewTypedBuilder creates a new TypedBuilder for the given storage instance.
func NewTypedBuilder[T any](storageInstance storage.Storage) TypedBuilder[T] {
	return TypedBuilder[T]{
		storage:    storageInstance,
		hashers:    []hasher.Hasher{},
		signers:    []crypto.Signer{},
		verifiers:  []crypto.Verifier{},
		marshaller: marshaller.NewTypedYamlMarshaller[T](),

		prefix:    "/",
		namerFunc: nil,
	}
}

func (s TypedBuilder[T]) copy() TypedBuilder[T] {
	return TypedBuilder[T]{
		storage:    s.storage,
		hashers:    slices.Clone(s.hashers),
		signers:    slices.Clone(s.signers),
		verifiers:  slices.Clone(s.verifiers),
		marshaller: s.marshaller,

		prefix:    s.prefix,
		namerFunc: s.namerFunc,
	}
}

// WithHasher adds a hasher to the builder.
func (s TypedBuilder[T]) WithHasher(h hasher.Hasher) TypedBuilder[T] {
	out := s.copy()

	out.hashers = append(out.hashers, h)

	return out
}

// WithSignerVerifier adds a signer/verifier to the builder.
func (s TypedBuilder[T]) WithSignerVerifier(sv crypto.SignerVerifier) TypedBuilder[T] {
	out := s.copy()

	out.signers = append(out.signers, sv)
	out.verifiers = append(out.verifiers, sv)

	return out
}

// WithSigner adds a signer to the builder.
func (s TypedBuilder[T]) WithSigner(signer crypto.Signer) TypedBuilder[T] {
	out := s.copy()

	out.signers = append(out.signers, signer)

	return out
}

// WithVerifier adds a verifier to the builder.
func (s TypedBuilder[T]) WithVerifier(verifier crypto.Verifier) TypedBuilder[T] {
	out := s.copy()

	out.verifiers = append(out.verifiers, verifier)

	return out
}

// WithMarshaller sets the marshaller for the builder.
func (s TypedBuilder[T]) WithMarshaller(marshaller marshaller.TypedYamlMarshaller[T]) TypedBuilder[T] {
	out := s.copy()

	out.marshaller = marshaller

	return out
}

// WithPrefix sets the key prefix for the builder.
func (s TypedBuilder[T]) WithPrefix(prefix string) TypedBuilder[T] {
	out := s.copy()

	out.prefix = prefix

	return out
}

// WithNamer sets the namer for the builder using a constructor function.
// The constructor function will be called during Build() with the current prefix.
func (s TypedBuilder[T]) WithNamer(namerFunc NamerConstructor) TypedBuilder[T] {
	out := s.copy()

	out.namerFunc = namerFunc

	return out
}

func (s TypedBuilder[T]) getHasherNames() []string {
	names := make([]string, 0, len(s.hashers))
	for _, hasherInstance := range s.hashers {
		names = append(names, hasherInstance.Name())
	}

	return names
}

func (s TypedBuilder[T]) getSignerNames() []string {
	names := make([]string, 0, len(s.signers))
	for _, signerInstance := range s.signers {
		names = append(names, signerInstance.Name())
	}

	return names
}

func (s TypedBuilder[T]) getVerifierNames() []string {
	names := make([]string, 0, len(s.verifiers))
	for _, verifierInstance := range s.verifiers {
		names = append(names, verifierInstance.Name())
	}

	return names
}

func (s TypedBuilder[T]) getSignerVerifierNames() []string {
	names := map[string]struct{}{}

	for _, name := range s.getSignerNames() {
		names[name] = struct{}{}
	}

	for _, name := range s.getVerifierNames() {
		names[name] = struct{}{}
	}

	return slices.Collect(maps.Keys(names))
}

// Build creates a new Typed storage instance with the configured options.
func (s TypedBuilder[T]) Build() *Typed[T] {
	if s.namerFunc == nil {
		s.namerFunc = namer.NewDefaultNamer
	}

	hasherNames := s.getHasherNames()

	gen := NewGenerator(
		s.namerFunc(s.prefix, hasherNames, s.getSignerNames()),
		s.marshaller,
		s.hashers,
		s.signers,
	)

	val := NewValidator(
		s.namerFunc(s.prefix, hasherNames, s.getVerifierNames()),
		s.marshaller,
		s.hashers,
		s.verifiers,
	)

	namerInstance := s.namerFunc(s.prefix, hasherNames, s.getSignerVerifierNames())

	return &Typed[T]{
		base:  s.storage,
		gen:   gen,
		val:   val,
		namer: namerInstance,
	}
}
