package namer

import (
	"errors"
	"fmt"
	"strings"
)

// Fixed marker segments distinguishing key categories. Hash and signature keys
// always live under these literals; the per-hasher / per-signer location
// segment sits between the marker and the objectLocation. Mirrors the
// hashName / signatureName constants used by DefaultNamer.
const (
	layeredHashMarker = "hash"
	layeredSigMarker  = "sig"
)

// Sentinel errors for NewLayeredNamer validation.
var (
	// ErrEmptyHasherName is returned when a hash entry has an empty HasherName.
	ErrEmptyHasherName = errors.New("namer: hash entry has empty HasherName")
	// ErrEmptySignerName is returned when a sig entry has an empty SignerName.
	ErrEmptySignerName = errors.New("namer: sig entry has empty SignerName")
	// ErrDuplicateLocation is returned when a location segment is used more than once within a category.
	ErrDuplicateLocation = errors.New("namer: duplicate location segment")
	// ErrSegmentEmpty is returned when a location segment is empty.
	ErrSegmentEmpty = errors.New("namer: segment must not be empty")
	// ErrSegmentLeadingSlash is returned when a segment starts with '/'.
	ErrSegmentLeadingSlash = errors.New("namer: segment must not start with '/'")
	// ErrSegmentTrailingSlash is returned when a segment ends with '/'.
	ErrSegmentTrailingSlash = errors.New("namer: segment must not end with '/'")
	// ErrSegmentInnerSlash is returned when a segment contains '/'.
	ErrSegmentInnerSlash = errors.New("namer: segment must not contain '/'")
	// ErrObjectLocationReserved is returned when objectLocation's first segment
	// is one of the reserved category markers ("hash" or "sig"), which would
	// collide with hash/sig key paths during parsing.
	ErrObjectLocationReserved = errors.New(
		"namer: objectLocation must not start with reserved segment \"hash\" or \"sig\"")
	// ErrCompactSingleHashCardinality is returned when CompactSingleHash is set
	// but the configured hash list does not have exactly one entry.
	ErrCompactSingleHashCardinality = errors.New("namer: CompactSingleHash requires exactly one hash entry")
	// ErrCompactSingleSigCardinality is returned when CompactSingleSig is set
	// but the configured sig list does not have exactly one entry.
	ErrCompactSingleSigCardinality = errors.New("namer: CompactSingleSig requires exactly one sig entry")
)

// LayeredHashLocation associates a hasher name with its key location segment.
type LayeredHashLocation struct {
	HasherName string // matches hasher.Hasher.Name().
	Location   string // the location segment (e.g. "sha256").
}

// LayeredSigLocation associates a signer name with its key location segment.
type LayeredSigLocation struct {
	SignerName string // matches crypto.Signer.Name() / Verifier.Name().
	Location   string // the location segment (e.g. "ed25519").
}

// LayeredOption configures NewLayeredNamer.
type LayeredOption func(*layeredOpts)

type layeredOpts struct {
	compactHash bool
	compactSig  bool
}

// CompactSingleHash drops the per-hasher location segment in generated and
// parsed hash keys. Layout becomes /hash/<objectLocation>/<name>.
// NewLayeredNamer returns ErrCompactSingleHashCardinality if the hash list
// does not have exactly one entry.
func CompactSingleHash() LayeredOption {
	return func(o *layeredOpts) { o.compactHash = true }
}

// CompactSingleSig drops the per-signer location segment in generated and
// parsed sig keys. Layout becomes /sig/<objectLocation>/<name>.
// NewLayeredNamer returns ErrCompactSingleSigCardinality if the sig list
// does not have exactly one entry.
func CompactSingleSig() LayeredOption {
	return func(o *layeredOpts) { o.compactSig = true }
}

type layeredNamer struct {
	objectLocation string
	hashLocations  []LayeredHashLocation
	sigLocations   []LayeredSigLocation
	compactHash    bool
	compactSig     bool
	// Reverse maps populated once at construction so ParseKey is O(1)
	// regardless of hash/sig list size.
	hashIndex map[string]string // hashLocation -> hasherName.
	sigIndex  map[string]string // sigLocation  -> signerName.
}

// NewLayeredNamer constructs a Namer that emits keys with per-category
// location segments. All segment strings are validated at construction time.
//
// Layout (default):
//
//	/<objectLocation>/<name>                          (value)
//	/hash/<hashLocation>/<objectLocation>/<name>      (hash, one per hasher)
//	/sig/<sigLocation>/<objectLocation>/<name>        (sig, one per signer)
//
// objectLocation may itself be a multi-segment path (e.g. "settings/ldap").
// hashLocation and sigLocation are still single tokens since they index
// per-hasher / per-signer maps.
//
// With CompactSingleHash, the <hashLocation> segment is omitted; with
// CompactSingleSig, the <sigLocation> segment is omitted.
//
// Validation rules:
//   - hashLocation / sigLocation must be a single non-empty segment with no '/'.
//   - objectLocation must be non-empty, must not start or end with '/', and
//     must not contain empty inner segments ("//"). Its first segment must
//     not be "hash" or "sig".
//   - hashLocations must be unique within hashes; sigLocations must be unique within sigs.
//   - Compact flags require exactly one entry in their respective category.
func NewLayeredNamer(
	objectLocation string,
	hashLocations []LayeredHashLocation,
	sigLocations []LayeredSigLocation,
	opts ...LayeredOption,
) (Namer, error) {
	var resolved layeredOpts

	for _, opt := range opts {
		opt(&resolved)
	}

	err := validateObjectLocation(objectLocation)
	if err != nil {
		return nil, err
	}

	firstSeg, _, _ := strings.Cut(objectLocation, "/")
	if firstSeg == layeredHashMarker || firstSeg == layeredSigMarker {
		return nil, fmt.Errorf("%w: got %q", ErrObjectLocationReserved, objectLocation)
	}

	if resolved.compactHash && len(hashLocations) != 1 {
		return nil, fmt.Errorf("%w: got %d", ErrCompactSingleHashCardinality, len(hashLocations))
	}

	if resolved.compactSig && len(sigLocations) != 1 {
		return nil, fmt.Errorf("%w: got %d", ErrCompactSingleSigCardinality, len(sigLocations))
	}

	hashIndex := make(map[string]string, len(hashLocations))

	for _, hashLoc := range hashLocations {
		if hashLoc.HasherName == "" {
			return nil, ErrEmptyHasherName
		}

		err = validateSegment("hash Location", hashLoc.Location)
		if err != nil {
			return nil, err
		}

		if _, exists := hashIndex[hashLoc.Location]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateLocation, hashLoc.Location)
		}

		hashIndex[hashLoc.Location] = hashLoc.HasherName
	}

	sigIndex := make(map[string]string, len(sigLocations))

	for _, sigLoc := range sigLocations {
		if sigLoc.SignerName == "" {
			return nil, ErrEmptySignerName
		}

		err = validateSegment("sig Location", sigLoc.Location)
		if err != nil {
			return nil, err
		}

		if _, exists := sigIndex[sigLoc.Location]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateLocation, sigLoc.Location)
		}

		sigIndex[sigLoc.Location] = sigLoc.SignerName
	}

	return &layeredNamer{
		objectLocation: objectLocation,
		hashLocations:  hashLocations,
		sigLocations:   sigLocations,
		compactHash:    resolved.compactHash,
		compactSig:     resolved.compactSig,
		hashIndex:      hashIndex,
		sigIndex:       sigIndex,
	}, nil
}

func validateSegment(field, seg string) error {
	if seg == "" {
		return fmt.Errorf("%w: %s", ErrSegmentEmpty, field)
	}

	if strings.HasPrefix(seg, "/") {
		return fmt.Errorf("%w: %s %q", ErrSegmentLeadingSlash, field, seg)
	}

	if strings.HasSuffix(seg, "/") {
		return fmt.Errorf("%w: %s %q", ErrSegmentTrailingSlash, field, seg)
	}

	if strings.Contains(seg, "/") {
		return fmt.Errorf("%w: %s %q", ErrSegmentInnerSlash, field, seg)
	}

	return nil
}

// validateObjectLocation accepts multi-segment paths (e.g. "settings/ldap")
// while still rejecting empty input, leading or trailing '/', and empty
// inner segments ("//"). Reserved-marker checks are done by the caller.
func validateObjectLocation(seg string) error {
	if seg == "" {
		return fmt.Errorf("%w: objectLocation", ErrSegmentEmpty)
	}

	if strings.HasPrefix(seg, "/") {
		return fmt.Errorf("%w: objectLocation %q", ErrSegmentLeadingSlash, seg)
	}

	if strings.HasSuffix(seg, "/") {
		return fmt.Errorf("%w: objectLocation %q", ErrSegmentTrailingSlash, seg)
	}

	if strings.Contains(seg, "//") {
		return fmt.Errorf("%w: objectLocation %q has empty inner segment", ErrSegmentEmpty, seg)
	}

	return nil
}

// GenerateNames returns one key per category for the given object name.
// name must be non-empty; a leading "/" is stripped.
func (n *layeredNamer) GenerateNames(name string) ([]Key, error) {
	if name == "" {
		return nil, errInvalidName(name, "should not be empty")
	}

	name = strings.TrimPrefix(name, "/")

	out := make([]Key, 0, 1+len(n.hashLocations)+len(n.sigLocations))

	out = append(out, NewDefaultKey(
		name,
		KeyTypeValue,
		"",
		"/"+n.objectLocation+"/"+name,
	))

	for _, hl := range n.hashLocations {
		out = append(out, NewDefaultKey(
			name,
			KeyTypeHash,
			hl.HasherName,
			n.buildHashKey(hl.Location, name),
		))
	}

	for _, sl := range n.sigLocations {
		out = append(out, NewDefaultKey(
			name,
			KeyTypeSignature,
			sl.SignerName,
			n.buildSigKey(sl.Location, name),
		))
	}

	return out, nil
}

// ParseKey parses a raw key path back to a DefaultKey.
//
// Recognized shapes (must match the namer's configured layout):
//
//	/<objectLocation>/<name>                          → KeyTypeValue
//	/hash/<hashLocation>/<objectLocation>/<name>      → KeyTypeHash
//	/sig/<sigLocation>/<objectLocation>/<name>        → KeyTypeSignature
//
// With CompactSingleHash / CompactSingleSig the corresponding location
// segment is absent.
func (n *layeredNamer) ParseKey(raw string) (DefaultKey, error) {
	stripped := strings.TrimPrefix(raw, "/")

	first, rest, found := strings.Cut(stripped, "/")
	if !found || first == "" {
		return DefaultKey{}, errInvalidKey(raw, "key must have at least two path segments")
	}

	switch first {
	case layeredHashMarker:
		return n.parseHashKey(raw, rest)
	case layeredSigMarker:
		return n.parseSigKey(raw, rest)
	default:
		return n.parseValueKey(raw, stripped)
	}
}

// ParseKeys groups raw key paths by object name. It mirrors DefaultNamer.ParseKeys.
func (n *layeredNamer) ParseKeys(names []string, ignoreError bool) (Results, error) {
	out := map[string][]Key{}

	for _, name := range names {
		key, err := n.ParseKey(name)
		switch {
		case err != nil && ignoreError:
			continue
		case err != nil:
			return Results{}, err
		}

		out[key.name] = append(out[key.name], key)
	}

	return NewResults(out), nil
}

// Prefix returns the prefix string for value-layer range walks.
// It always covers only the objectLocation sub-tree (hashes/sigs are separate).
//
// Examples (objectLocation="objects"):
//
//	Prefix("", false)       → "/objects/"
//	Prefix("alice", false)  → "/objects/alice"
//	Prefix("users", true)   → "/objects/users/"
//	Prefix("/alice/", false) → "/objects/alice"
func (n *layeredNamer) Prefix(val string, isPrefix bool) string {
	suffix := strings.Trim(val, "/")

	var builder strings.Builder

	builder.WriteByte('/')
	builder.WriteString(n.objectLocation)
	builder.WriteByte('/')

	if suffix != "" {
		builder.WriteString(suffix)

		if isPrefix {
			builder.WriteByte('/')
		}
	}

	return builder.String()
}

func (n *layeredNamer) buildHashKey(hashLocation, name string) string {
	if n.compactHash {
		return "/" + layeredHashMarker + "/" + n.objectLocation + "/" + name
	}

	return "/" + layeredHashMarker + "/" + hashLocation + "/" + n.objectLocation + "/" + name
}

func (n *layeredNamer) buildSigKey(sigLocation, name string) string {
	if n.compactSig {
		return "/" + layeredSigMarker + "/" + n.objectLocation + "/" + name
	}

	return "/" + layeredSigMarker + "/" + sigLocation + "/" + n.objectLocation + "/" + name
}

func (n *layeredNamer) parseValueKey(raw, stripped string) (DefaultKey, error) {
	name, ok := strings.CutPrefix(stripped, n.objectLocation+"/")
	if !ok {
		first, _, _ := strings.Cut(stripped, "/")

		return DefaultKey{}, errInvalidKey(raw, fmt.Sprintf("unknown location segment %q", first))
	}

	if name == "" {
		return DefaultKey{}, errInvalidKey(raw, "name part must not be empty")
	}

	if strings.HasSuffix(name, "/") {
		return DefaultKey{}, errInvalidKey(raw, "key name should not be prefix")
	}

	return NewDefaultKey(name, KeyTypeValue, "", raw), nil
}

func (n *layeredNamer) parseHashKey(raw, rest string) (DefaultKey, error) {
	if n.compactHash {
		return n.parseCompactHashKey(raw, rest)
	}

	return n.parseFullHashKey(raw, rest)
}

func (n *layeredNamer) parseCompactHashKey(raw, rest string) (DefaultKey, error) {
	after, ok := strings.CutPrefix(rest, n.objectLocation+"/")
	if !ok {
		return DefaultKey{}, errInvalidKey(raw,
			fmt.Sprintf("hash key objectLocation does not match %q", n.objectLocation))
	}

	err := validateNamePart(raw, after, "hash")
	if err != nil {
		return DefaultKey{}, err
	}

	return NewDefaultKey(after, KeyTypeHash, n.hashLocations[0].HasherName, raw), nil
}

func (n *layeredNamer) parseFullHashKey(raw, rest string) (DefaultKey, error) {
	hashLoc, afterLoc, foundLoc := strings.Cut(rest, "/")
	if !foundLoc || hashLoc == "" {
		return DefaultKey{}, errInvalidKey(raw, "hash key missing hashLocation segment")
	}

	hasherName, ok := n.hashIndex[hashLoc]
	if !ok {
		return DefaultKey{}, errInvalidKey(raw, fmt.Sprintf("unknown hash location %q", hashLoc))
	}

	after, ok := strings.CutPrefix(afterLoc, n.objectLocation+"/")
	if !ok {
		return DefaultKey{}, errInvalidKey(raw,
			fmt.Sprintf("hash key objectLocation does not match %q", n.objectLocation))
	}

	err := validateNamePart(raw, after, "hash")
	if err != nil {
		return DefaultKey{}, err
	}

	return NewDefaultKey(after, KeyTypeHash, hasherName, raw), nil
}

func (n *layeredNamer) parseSigKey(raw, rest string) (DefaultKey, error) {
	if n.compactSig {
		return n.parseCompactSigKey(raw, rest)
	}

	return n.parseFullSigKey(raw, rest)
}

func (n *layeredNamer) parseCompactSigKey(raw, rest string) (DefaultKey, error) {
	after, ok := strings.CutPrefix(rest, n.objectLocation+"/")
	if !ok {
		return DefaultKey{}, errInvalidKey(raw,
			fmt.Sprintf("sig key objectLocation does not match %q", n.objectLocation))
	}

	err := validateNamePart(raw, after, "sig")
	if err != nil {
		return DefaultKey{}, err
	}

	return NewDefaultKey(after, KeyTypeSignature, n.sigLocations[0].SignerName, raw), nil
}

func (n *layeredNamer) parseFullSigKey(raw, rest string) (DefaultKey, error) {
	sigLoc, afterLoc, foundLoc := strings.Cut(rest, "/")
	if !foundLoc || sigLoc == "" {
		return DefaultKey{}, errInvalidKey(raw, "sig key missing sigLocation segment")
	}

	signerName, ok := n.sigIndex[sigLoc]
	if !ok {
		return DefaultKey{}, errInvalidKey(raw, fmt.Sprintf("unknown sig location %q", sigLoc))
	}

	after, ok := strings.CutPrefix(afterLoc, n.objectLocation+"/")
	if !ok {
		return DefaultKey{}, errInvalidKey(raw,
			fmt.Sprintf("sig key objectLocation does not match %q", n.objectLocation))
	}

	err := validateNamePart(raw, after, "sig")
	if err != nil {
		return DefaultKey{}, err
	}

	return NewDefaultKey(after, KeyTypeSignature, signerName, raw), nil
}

func validateNamePart(raw, name, kind string) error {
	if name == "" {
		return errInvalidKey(raw, kind+" key missing name")
	}

	if strings.HasSuffix(name, "/") {
		return errInvalidKey(raw, kind+" key name should not be prefix")
	}

	return nil
}
