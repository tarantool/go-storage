package integrity

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/tarantool/go-storage/crypto"
	"github.com/tarantool/go-storage/hasher"
	"github.com/tarantool/go-storage/marshaller"
	"github.com/tarantool/go-storage/namer"
	"github.com/tarantool/go-storage/predicate"
)

// ErrUnknownHasherLocation is returned when a WithHashLocation key does not match any configured hasher.
var ErrUnknownHasherLocation = errors.New("codec: WithHashLocation key does not match any configured hasher")

// ErrUnknownSignerLocation is returned when a WithSignatureLocation key does not match
// any configured signer or verifier.
var ErrUnknownSignerLocation = errors.New(
	"codec: WithSignatureLocation key does not match any configured signer or verifier")

// ErrSingleHashCompactCardinality is returned when WithSingleHashCompact is set
// but the codec is not configured with exactly one hasher.
var ErrSingleHashCompactCardinality = errors.New(
	"codec: WithSingleHashCompact requires exactly one hasher")

// ErrSingleSigCompactCardinality is returned when WithSingleSigCompact is set
// but the union of signers and verifiers (deduped by name) is not of size one.
var ErrSingleSigCompactCardinality = errors.New(
	"codec: WithSingleSigCompact requires exactly one unique signer/verifier")

// CodecNamerConstructor builds a namer for a Codec given the resolved
// object/hash/sig location bindings and any LayeredOptions threaded through
// from the builder (e.g. compact-mode flags).
type CodecNamerConstructor func(
	objectLocation string,
	hashLocations []namer.LayeredHashLocation,
	sigLocations []namer.LayeredSigLocation,
	opts ...namer.LayeredOption,
) (namer.Namer, error)

// Codec holds the integrity schema for type T: marshaller, hashers,
// signers/verifiers, and three namer instances (generator, validator, and a
// top-level union). It carries no storage handle and no namespace prefix —
// namespace scoping is the storage layer's concern.
type Codec[T any] struct {
	gen        Generator[T]
	val        Validator[T]
	namer      namer.Namer
	marshaller marshaller.TypedMarshaller[T]
}

// ValueEqual creates a predicate that checks if a key's value equals the specified value.
func (c *Codec[T]) ValueEqual(value T) (Predicate, error) {
	return c.valuePredicate(value, predicate.ValueEqual)
}

// ValueNotEqual creates a predicate that checks if a key's value is not equal to the specified value.
func (c *Codec[T]) ValueNotEqual(value T) (Predicate, error) {
	return c.valuePredicate(value, predicate.ValueNotEqual)
}

// VersionEqual creates a predicate that checks if a key's version equals the specified version.
func (c *Codec[T]) VersionEqual(v int64) Predicate {
	return c.versionPredicate(v, predicate.VersionEqual)
}

// VersionNotEqual creates a predicate that checks if a key's version is not equal to the specified version.
func (c *Codec[T]) VersionNotEqual(v int64) Predicate {
	return c.versionPredicate(v, predicate.VersionNotEqual)
}

// VersionGreater creates a predicate that checks if a key's version is greater than the specified version.
func (c *Codec[T]) VersionGreater(v int64) Predicate {
	return c.versionPredicate(v, predicate.VersionGreater)
}

// VersionLess creates a predicate that checks if a key's version is less than the specified version.
func (c *Codec[T]) VersionLess(v int64) Predicate {
	return c.versionPredicate(v, predicate.VersionLess)
}

func (c *Codec[T]) valuePredicate(value T,
	predFunc func(key []byte, value any) predicate.Predicate) (Predicate, error) {
	mValue, err := c.marshaller.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to marshal predicate value", err)
	}

	return func(key []byte) predicate.Predicate {
		return predFunc(key, mValue)
	}, nil
}

func (c *Codec[T]) versionPredicate(
	v int64,
	predFunc func(key []byte, value int64) predicate.Predicate) Predicate {
	return func(key []byte) predicate.Predicate { return predFunc(key, v) }
}

// CodecBuilder[T] is a fluent, value-receiver builder for Codec[T].
// Each setter returns a copy of the builder (copy-on-write), so the original
// builder is not mutated.
type CodecBuilder[T any] struct {
	marshaller     marshaller.TypedMarshaller[T]
	hashers        []hasher.Hasher
	signers        []crypto.Signer
	verifiers      []crypto.Verifier
	objectLocation string
	hashLocations  map[string]string // hasher.Name() → location override.
	sigLocations   map[string]string // signer/verifier name → location override.
	namerFunc      CodecNamerConstructor
	compactHash    bool
	compactSig     bool
}

// NewCodecBuilder returns a new CodecBuilder with sensible defaults.
// Default marshaller: TypedYamlMarshaller[T].
// Default objectLocation: "" (resolved to "objects" at Build time).
// Default namerFunc: wraps namer.NewLayeredNamer.
func NewCodecBuilder[T any]() CodecBuilder[T] {
	return CodecBuilder[T]{
		marshaller:     marshaller.NewTypedYamlMarshaller[T](),
		hashers:        []hasher.Hasher{},
		signers:        []crypto.Signer{},
		verifiers:      []crypto.Verifier{},
		objectLocation: "",
		hashLocations:  map[string]string{},
		sigLocations:   map[string]string{},
		namerFunc:      nil,
		compactHash:    false,
		compactSig:     false,
	}
}

func (b CodecBuilder[T]) copy() CodecBuilder[T] {
	hashLocs := maps.Clone(b.hashLocations)
	sigLocs := maps.Clone(b.sigLocations)

	return CodecBuilder[T]{
		marshaller:     b.marshaller,
		hashers:        slices.Clone(b.hashers),
		signers:        slices.Clone(b.signers),
		verifiers:      slices.Clone(b.verifiers),
		objectLocation: b.objectLocation,
		hashLocations:  hashLocs,
		sigLocations:   sigLocs,
		namerFunc:      b.namerFunc,
		compactHash:    b.compactHash,
		compactSig:     b.compactSig,
	}
}

// WithMarshaller sets a custom marshaller.
func (b CodecBuilder[T]) WithMarshaller(m marshaller.TypedMarshaller[T]) CodecBuilder[T] {
	out := b.copy()

	out.marshaller = m

	return out
}

// WithHasher adds a hasher to the codec.
func (b CodecBuilder[T]) WithHasher(h hasher.Hasher) CodecBuilder[T] {
	out := b.copy()

	out.hashers = append(out.hashers, h)

	return out
}

// WithSigner adds a signer to the codec.
func (b CodecBuilder[T]) WithSigner(s crypto.Signer) CodecBuilder[T] {
	out := b.copy()

	out.signers = append(out.signers, s)

	return out
}

// WithVerifier adds a verifier to the codec.
func (b CodecBuilder[T]) WithVerifier(v crypto.Verifier) CodecBuilder[T] {
	out := b.copy()

	out.verifiers = append(out.verifiers, v)

	return out
}

// WithSignerVerifier adds a combined signer/verifier to both the signer and verifier lists.
func (b CodecBuilder[T]) WithSignerVerifier(sv crypto.SignerVerifier) CodecBuilder[T] {
	out := b.copy()

	out.signers = append(out.signers, sv)
	out.verifiers = append(out.verifiers, sv)

	return out
}

// WithObjectLocation sets the location segment for value keys.
// If not called, defaults to "objects" at Build time.
func (b CodecBuilder[T]) WithObjectLocation(loc string) CodecBuilder[T] {
	out := b.copy()

	out.objectLocation = loc

	return out
}

// WithHashLocation overrides the location segment for the named hasher.
// hasherName must match the hasher's Name() method. If not called, the
// location defaults to the hasher's Name().
func (b CodecBuilder[T]) WithHashLocation(hasherName, loc string) CodecBuilder[T] {
	out := b.copy()

	out.hashLocations[hasherName] = loc

	return out
}

// WithSignatureLocation overrides the location segment for the named signer/verifier.
// signerName must match the signer's or verifier's Name() method. If not called, the
// location defaults to the signer's Name().
func (b CodecBuilder[T]) WithSignatureLocation(signerName, loc string) CodecBuilder[T] {
	out := b.copy()

	out.sigLocations[signerName] = loc

	return out
}

// WithNamer sets a custom CodecNamerConstructor.
func (b CodecBuilder[T]) WithNamer(f CodecNamerConstructor) CodecBuilder[T] {
	out := b.copy()

	out.namerFunc = f

	return out
}

// WithSingleHashCompact opts the codec into the compact hash key layout
// (/hash/<objectLocation>/<name>, dropping the per-hasher segment). Build
// returns ErrSingleHashCompactCardinality if exactly one hasher is not
// configured.
func (b CodecBuilder[T]) WithSingleHashCompact() CodecBuilder[T] {
	out := b.copy()

	out.compactHash = true

	return out
}

// WithSingleSigCompact opts the codec into the compact sig key layout
// (/sig/<objectLocation>/<name>, dropping the per-signer segment). Build
// returns ErrSingleSigCompactCardinality if the union of signers and
// verifiers (deduped by name) is not of size one.
func (b CodecBuilder[T]) WithSingleSigCompact() CodecBuilder[T] {
	out := b.copy()

	out.compactSig = true

	return out
}

// Build constructs a *Codec[T] from the current builder state.
//
// It returns an error if any location segment is invalid, if any pair of
// segments collides, or if a WithHashLocation/WithSignatureLocation key does
// not match a configured hasher/signer/verifier (catches typos that would
// otherwise be silently ignored).
func (b CodecBuilder[T]) Build() (*Codec[T], error) {
	namerFn := b.namerFunc
	if namerFn == nil {
		namerFn = namer.NewLayeredNamer
	}

	objLoc := b.objectLocation
	if objLoc == "" {
		objLoc = "objects"
	}

	// hashLocations/sigLocations override key→loc; reject keys that have no
	// matching hasher/signer/verifier so a typo doesn't silently no-op.
	hasherNames := make(map[string]struct{}, len(b.hashers))
	for _, h := range b.hashers {
		hasherNames[h.Name()] = struct{}{}
	}

	sigNames := make(map[string]struct{}, len(b.signers)+len(b.verifiers))
	for _, s := range b.signers {
		sigNames[s.Name()] = struct{}{}
	}

	for _, v := range b.verifiers {
		sigNames[v.Name()] = struct{}{}
	}

	for name := range b.hashLocations {
		if _, ok := hasherNames[name]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownHasherLocation, name)
		}
	}

	for name := range b.sigLocations {
		if _, ok := sigNames[name]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownSignerLocation, name)
		}
	}

	genHashLocs := make([]namer.LayeredHashLocation, 0, len(b.hashers))
	for _, hasher := range b.hashers {
		loc := hasher.Name()
		if override, ok := b.hashLocations[hasher.Name()]; ok {
			loc = override
		}

		genHashLocs = append(genHashLocs, namer.LayeredHashLocation{
			HasherName: hasher.Name(),
			Location:   loc,
		})
	}

	// SignerName always carries the real signer/verifier name (not the
	// location override) because downstream lookups index by Property().
	genSigLocs := make([]namer.LayeredSigLocation, 0, len(b.signers))
	for _, signer := range b.signers {
		loc := signer.Name()
		if override, ok := b.sigLocations[signer.Name()]; ok {
			loc = override
		}

		genSigLocs = append(genSigLocs, namer.LayeredSigLocation{
			SignerName: signer.Name(),
			Location:   loc,
		})
	}

	valSigLocs := make([]namer.LayeredSigLocation, 0, len(b.verifiers))
	for _, verifier := range b.verifiers {
		loc := verifier.Name()
		if override, ok := b.sigLocations[verifier.Name()]; ok {
			loc = override
		}

		valSigLocs = append(valSigLocs, namer.LayeredSigLocation{
			SignerName: verifier.Name(),
			Location:   loc,
		})
	}

	// Top-level namer must accept keys produced by either side (signer or
	// verifier), so its sig list is the union deduplicated by name.
	seenSigNames := make(map[string]struct{})
	topSigLocs := make([]namer.LayeredSigLocation, 0, len(b.signers)+len(b.verifiers))

	for _, sl := range genSigLocs {
		if _, ok := seenSigNames[sl.SignerName]; !ok {
			seenSigNames[sl.SignerName] = struct{}{}
			topSigLocs = append(topSigLocs, sl)
		}
	}

	for _, sl := range valSigLocs {
		if _, ok := seenSigNames[sl.SignerName]; !ok {
			seenSigNames[sl.SignerName] = struct{}{}
			topSigLocs = append(topSigLocs, sl)
		}
	}

	// Compact flags must be uniform across gen/val/top — otherwise their
	// emitted/parsed key shapes diverge and round-trips break. Check
	// cardinality against every list that will become a namer's sig/hash list.
	var opts []namer.LayeredOption

	if b.compactHash {
		if len(genHashLocs) != 1 {
			return nil, fmt.Errorf("%w: got %d", ErrSingleHashCompactCardinality, len(genHashLocs))
		}

		opts = append(opts, namer.CompactSingleHash())
	}

	if b.compactSig {
		if len(genSigLocs) != 1 || len(valSigLocs) != 1 || len(topSigLocs) != 1 {
			return nil, fmt.Errorf("%w: signers=%d verifiers=%d union=%d",
				ErrSingleSigCompactCardinality, len(genSigLocs), len(valSigLocs), len(topSigLocs))
		}

		opts = append(opts, namer.CompactSingleSig())
	}

	genNamer, err := namerFn(objLoc, genHashLocs, genSigLocs, opts...)
	if err != nil {
		return nil, fmt.Errorf("codec: failed to build generator namer: %w", err)
	}

	valNamer, err := namerFn(objLoc, genHashLocs, valSigLocs, opts...)
	if err != nil {
		return nil, fmt.Errorf("codec: failed to build validator namer: %w", err)
	}

	topNamer, err := namerFn(objLoc, genHashLocs, topSigLocs, opts...)
	if err != nil {
		return nil, fmt.Errorf("codec: failed to build top-level namer: %w", err)
	}

	marsh := b.marshaller

	gen := NewGenerator[T](genNamer, marsh, b.hashers, b.signers)
	val := NewValidator[T](valNamer, marsh, b.hashers, b.verifiers)

	return &Codec[T]{
		gen:        gen,
		val:        val,
		namer:      topNamer,
		marshaller: marsh,
	}, nil
}
