package integrity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-storage/v2"
	"github.com/tarantool/go-storage/v2/internal/options"
	"github.com/tarantool/go-storage/v2/kv"
	"github.com/tarantool/go-storage/v2/namer"
	"github.com/tarantool/go-storage/v2/operation"
	"github.com/tarantool/go-storage/v2/predicate"
	storagetx "github.com/tarantool/go-storage/v2/tx"
)

// Sentinel errors for Tx operations.
var (
	ErrBranchNotFired     = errors.New("integrity: transaction branch did not fire")
	ErrTxNotCommitted     = errors.New("integrity: transaction has not been committed")
	ErrTxAlreadyCommitted = errors.New("integrity: transaction has already been committed")
)

// Response is the result of a Tx.Commit call. It deliberately exposes less
// than tx.Response so the integrity layer stays storage-agnostic.
type Response struct {
	Succeeded bool
}

type opSpan struct {
	// start, end bracket the contiguous slice of branch.ops produced by one
	// enqueue call. handler is invoked with the matching RequestResponse
	// slice after Commit; it is nil for Put/Delete (nothing to surface).
	start   int
	end     int
	handler func(rrs []storagetx.RequestResponse) error
}

// Branch holds a list of operations and spans for one side of an If/Then/Else.
// Branch is not goroutine-safe.
type Branch struct {
	parent *Tx
	isElse bool
	ops    []operation.Operation
	spans  []opSpan
}

func (b *Branch) branch() *Branch { return b }

// Branchable is satisfied by both *Tx (which routes to thenBranch) and *Branch
// (which routes to itself). The method is unexported so only types in this
// package can satisfy the interface.
type Branchable interface {
	branch() *Branch
}

// Tx is the user-facing multi-op, multi-codec transactional accumulator.
// Tx is not goroutine-safe.
type Tx struct {
	base       storage.Storage
	thenBranch *Branch // always non-nil after NewTx.
	elseBranch *Branch // nil until first tx.Else() call.
	preds      []predicate.Predicate
	buildErr   error
	committed  bool
	succeeded  bool
}

// NewTx creates a new Tx backed by the given storage.
func NewTx(s storage.Storage) *Tx {
	txn := &Tx{
		base:       s,
		thenBranch: nil,
		elseBranch: nil,
		preds:      nil,
		buildErr:   nil,
		committed:  false,
		succeeded:  false,
	}

	txn.thenBranch = &Branch{parent: txn, isElse: false, ops: nil, spans: nil}

	return txn
}

// If appends predicates to the transaction condition. Multiple calls
// accumulate, they do not replace. Returns the receiver for chaining.
func (t *Tx) If(preds ...predicate.Predicate) *Tx {
	t.preds = append(t.preds, preds...)

	return t
}

// Then returns the Then branch. Always returns the same *Branch.
func (t *Tx) Then() *Branch {
	return t.thenBranch
}

// Else lazily creates and returns the Else branch. Idempotent.
func (t *Tx) Else() *Branch {
	if t.elseBranch == nil {
		t.elseBranch = &Branch{parent: t, isElse: true, ops: nil, spans: nil}
	}

	return t.elseBranch
}

// Commit executes the accumulated operations as a single atomic storage call.
// A second call returns ErrTxAlreadyCommitted without touching storage.
func (t *Tx) Commit(ctx context.Context) (Response, error) {
	if t.committed {
		return Response{}, ErrTxAlreadyCommitted
	}

	// Set committed before checking buildErr so an error on the first call
	// still flips a subsequent call into the "already committed" branch.
	t.committed = true

	if t.buildErr != nil {
		return Response{}, t.buildErr
	}

	inner := t.base.Tx(ctx).If(t.preds...).Then(t.thenBranch.ops...)
	if t.elseBranch != nil {
		inner = inner.Else(t.elseBranch.ops...)
	}

	rsp, err := inner.Commit()
	if err != nil {
		return Response{}, fmt.Errorf("tx commit: %w", err)
	}

	t.succeeded = rsp.Succeeded

	// Only the fired branch's handlers run; the other branch's spans were
	// never sent to storage and have no RequestResponse to consume.
	firedBranch := t.thenBranch
	if !t.succeeded && t.elseBranch != nil {
		firedBranch = t.elseBranch
	}

	for _, span := range firedBranch.spans {
		if span.handler == nil {
			continue
		}

		err = span.handler(rsp.Results[span.start:span.end])
		if err != nil {
			return Response{}, err
		}
	}

	return Response{Succeeded: t.succeeded}, nil
}

// Passing *Tx to a TxGet/TxPut/TxDelete/TxRange call routes the ops onto the
// Then branch — the common case where the caller doesn't care about Else.
func (t *Tx) branch() *Branch { return t.thenBranch }

// First-error-wins: once a build error has been recorded, later enqueue
// calls become no-ops and Commit surfaces the original error.
func (t *Tx) setBuildErr(err error) {
	if t.buildErr == nil {
		t.buildErr = err
	}
}

// GetFuture holds the result of a TxGet enqueue. Call Result() after Commit.
type GetFuture[T any] struct {
	tx     *Tx
	branch *Branch
	result ValidatedResult[T]
	err    error
}

// Result returns the validated result after the Tx has been committed.
//
// Returns ErrTxNotCommitted if called before Commit.
// Returns the stored buildErr if Commit surfaced a build error.
// Returns ErrBranchNotFired if this future's branch did not fire.
func (f *GetFuture[T]) Result() (ValidatedResult[T], error) {
	if !f.tx.committed {
		return ValidatedResult[T]{}, ErrTxNotCommitted
	}

	if f.tx.buildErr != nil {
		return ValidatedResult[T]{}, f.tx.buildErr
	}

	// thenBranch fires on succeeded=true, elseBranch on succeeded=false.
	// The XOR check above flags futures whose branch did not fire.
	if f.branch.isElse == f.tx.succeeded {
		return ValidatedResult[T]{}, ErrBranchNotFired
	}

	return f.result, f.err
}

// RangeFuture holds the results of a TxRange enqueue. Call Result() after Commit.
type RangeFuture[T any] struct {
	tx     *Tx
	branch *Branch
	result []ValidatedResult[T]
	err    error
}

// Result returns the validated results after the Tx has been committed.
func (f *RangeFuture[T]) Result() ([]ValidatedResult[T], error) {
	if !f.tx.committed {
		return nil, ErrTxNotCommitted
	}

	if f.tx.buildErr != nil {
		return nil, f.tx.buildErr
	}

	if f.branch.isElse == f.tx.succeeded {
		return nil, ErrBranchNotFired
	}

	return f.result, f.err
}

// TxGet enqueues a Get operation for name onto branch b.
// Returns a *GetFuture[T] whose Result() is populated after Commit.
func (c *Codec[T]) TxGet(
	b Branchable,
	name string,
	opts ...options.OptionCallback[getOptions],
) *GetFuture[T] {
	branch := b.branch()
	txn := branch.parent

	future := &GetFuture[T]{
		tx:     txn,
		branch: branch,
		result: ValidatedResult[T]{}, //nolint:exhaustruct // zero value is intentional
		err:    nil,
	}

	if txn.buildErr != nil {
		return future
	}

	if !checkName(name) {
		txn.setBuildErr(ErrInvalidName)
		return future
	}

	keys, err := c.namer.GenerateNames(name)
	if err != nil {
		txn.setBuildErr(err)
		return future
	}

	vOpts := options.ApplyOptions[getOptions](nil, opts)

	start := len(branch.ops)

	for _, key := range keys {
		branch.ops = append(branch.ops, operation.Get([]byte(key.Build())))
	}

	end := len(branch.ops)

	branch.spans = append(branch.spans, opSpan{
		start: start,
		end:   end,
		handler: func(rrs []storagetx.RequestResponse) error {
			kvs := flattenRRs(rrs)

			results, err := c.val.Validate(kvs)
			if err != nil {
				future.err = err
				return nil //nolint:nilerr // handler never returns an error to Commit
			}

			switch {
			case len(results) == 0:
				future.err = ErrNotFound
			case len(results) > 1 && !vOpts.ignoreMoreThanOneResult:
				future.err = ErrMoreThanOneResult
			default:
				result := results[0]
				if result.Error != nil {
					failedToDecode := result.Value.IsZero()

					if vOpts.ignoreVerificationError && !failedToDecode {
						future.result = result
					} else {
						future.err = result.Error
					}
				} else {
					future.result = result
				}
			}

			return nil
		},
	})

	return future
}

// TxPut enqueues Put operations for name/value onto branch b.
// Returns the first error encountered during build (if any); subsequent
// enqueue calls after an error are no-ops returning nil.
func (c *Codec[T]) TxPut(b Branchable, name string, value T) error {
	branch := b.branch()
	txn := branch.parent

	if txn.buildErr != nil {
		return nil //nolint:nilerr // first-error-wins: later enqueues are no-ops.
	}

	if !checkName(name) {
		txn.setBuildErr(ErrInvalidName)
		return ErrInvalidName
	}

	kvs, err := c.gen.Generate(name, value)
	if err != nil {
		txn.setBuildErr(err)
		return err
	}

	for _, kv := range kvs {
		branch.ops = append(branch.ops, operation.Put(kv.Key, kv.Value))
	}

	// Span is recorded with a nil handler so result-slot indices stay aligned
	// with branch.ops; Commit skips nil handlers when walking spans.
	start := len(branch.ops) - len(kvs)

	branch.spans = append(branch.spans, opSpan{start: start, end: len(branch.ops), handler: nil})

	return nil
}

// TxDelete enqueues Delete operations for name onto branch b.
func (c *Codec[T]) TxDelete(
	b Branchable,
	name string,
	opts ...options.OptionCallback[deleteOptions],
) error {
	branch := b.branch()
	txn := branch.parent

	if txn.buildErr != nil {
		return nil //nolint:nilerr // first-error-wins: later enqueues are no-ops.
	}

	vOpts := options.ApplyOptions[deleteOptions](nil, opts)

	if !vOpts.withPrefix && !checkName(name) {
		txn.setBuildErr(ErrInvalidName)
		return ErrInvalidName
	}

	if vOpts.withPrefix && !checkPrefix(name) {
		txn.setBuildErr(ErrInvalidName)
		return ErrInvalidName
	}

	keys, err := c.namer.GenerateNames(name)
	if err != nil {
		wrapped := fmt.Errorf("generate names: %w", err)
		txn.setBuildErr(wrapped)

		return wrapped
	}

	start := len(branch.ops)

	for _, key := range keys {
		var keyStr string
		if vOpts.withPrefix {
			keyStr = strings.TrimSuffix(key.Build(), "/") + "/"
		} else {
			keyStr = key.Build()
		}

		branch.ops = append(branch.ops, operation.Delete([]byte(keyStr)))
	}

	branch.spans = append(branch.spans, opSpan{start: start, end: len(branch.ops), handler: nil})

	return nil
}

// TxRange enqueues a range-Get operation for name onto branch b.
// name == "" fetches everything under the object location.
func (c *Codec[T]) TxRange(
	b Branchable,
	name string,
	opts ...options.OptionCallback[getOptions],
) *RangeFuture[T] {
	branch := b.branch()
	txn := branch.parent

	future := &RangeFuture[T]{tx: txn, branch: branch, result: nil, err: nil}

	if txn.buildErr != nil {
		return future
	}

	if !checkRangeName(name) {
		txn.setBuildErr(ErrInvalidName)
		return future
	}

	vOpts := options.ApplyOptions[getOptions](nil, opts)

	start := len(branch.ops)

	switch name {
	case "":
		// Empty name fans out across every key category — value, hash, sig.
		// A single Prefix("", true) only covers the value layer, which makes
		// the validator report missing hash/sig keys for every entry.
		for _, prefix := range c.namer.Prefixes("", true) {
			branch.ops = append(branch.ops, operation.Get([]byte(prefix)))
		}
	default:
		keys, err := c.namer.GenerateNames(name)
		if err != nil {
			txn.setBuildErr(err)
			return future
		}

		for _, key := range keys {
			keyStr := strings.TrimSuffix(key.Build(), "/") + "/"

			branch.ops = append(branch.ops, operation.Get([]byte(keyStr)))
		}
	}

	end := len(branch.ops)

	branch.spans = append(branch.spans, opSpan{
		start: start,
		end:   end,
		handler: func(rrs []storagetx.RequestResponse) error {
			kvs := flattenRRs(rrs)

			results, err := c.val.Validate(kvs)
			if err != nil {
				future.err = err
				return nil //nolint:nilerr // intentional: validation errors go into future.err, not tx commit result.
			}

			var out []ValidatedResult[T]

			for _, validResult := range results {
				if validResult.Error == nil {
					out = append(out, validResult)
					continue
				}

				failedToDecode := validResult.Value.IsZero()
				if vOpts.ignoreVerificationError && !failedToDecode {
					out = append(out, validResult)
				}
			}

			future.result = out

			return nil
		},
	})

	return future
}

// BindPredicate resolves the value-layer key for name and calls p with it,
// returning the concrete predicate.Predicate ready for use in Tx.If.
//
// Returns ErrInvalidName for empty, leading-slash, or trailing-slash names
// — the same rule applied by Put/Delete/Get/Watch — and ErrNoValueKey if
// the namer emits no value-layer key.
func (c *Codec[T]) BindPredicate(name string, pred Predicate) (predicate.Predicate, error) {
	if !checkName(name) {
		return nil, ErrInvalidName
	}

	keys, err := c.namer.GenerateNames(name)
	if err != nil {
		return nil, fmt.Errorf("generate names: %w", err)
	}

	for _, key := range keys {
		if key.Type() == namer.KeyTypeValue {
			return pred([]byte(key.Build())), nil
		}
	}

	return nil, ErrNoValueKey
}

func flattenRRs(rrs []storagetx.RequestResponse) []kv.KeyValue {
	total := 0
	for _, rr := range rrs {
		total += len(rr.Values)
	}

	out := make([]kv.KeyValue, 0, total)
	for _, rr := range rrs {
		out = append(out, rr.Values...)
	}

	return out
}
