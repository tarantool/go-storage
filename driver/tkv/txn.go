package tkv

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

// FailedToDecodeTxnResponseDataSingleError is returned when we failed to decode txnResponseDataSingle.
type FailedToDecodeTxnResponseDataSingleError struct {
	Text string
	Err  error
}

// Error returns the error message.
func (e FailedToDecodeTxnResponseDataSingleError) Error() string {
	return fmt.Sprintf("failed to decode txnResponseDataSingle, %s: %s", e.Text, e.Err)
}

type txnResponseDataSingle struct {
	Response []struct {
		Path        []byte `msgpack:"path"`
		ModRevision int64  `msgpack:"mod_revision"`
		Value       []byte `msgpack:"value"`
	}
}

func (t *txnResponseDataSingle) DecodeMsgpack(decoder *msgpack.Decoder) error {
	err := decoder.Decode(&t.Response)
	if err != nil {
		return FailedToDecodeTxnResponseDataSingleError{Text: "decode response", Err: err}
	}

	return nil
}

type txnResponseData struct {
	IsSuccess bool                    `msgpack:"is_success"`
	Responses []txnResponseDataSingle `msgpack:"responses"`
}

type txnResponse struct {
	Data     txnResponseData `msgpack:"data"`
	Revision int64           `msgpack:"revision"`
}

func (r txnResponse) asTxnResponse() tx.Response {
	results := make([]tx.RequestResponse, 0, len(r.Data.Responses))
	for _, val := range r.Data.Responses {
		keyValues := make([]kv.KeyValue, 0, len(val.Response))
		for _, resp := range val.Response {
			modRevision := resp.ModRevision
			if modRevision == 0 && r.Revision != 0 {
				modRevision = r.Revision
			}

			keyValues = append(keyValues, kv.KeyValue{
				Key:         resp.Path,
				Value:       resp.Value,
				ModRevision: modRevision,
			})
		}

		results = append(results, tx.RequestResponse{
			Values: keyValues,
		})
	}

	return tx.Response{
		Succeeded: r.Data.IsSuccess,
		Results:   results,
	}
}

type txnRequest struct {
	_msgpack struct{} `msgpack:",omitempty"`

	Predicates []tkvPredicate `msgpack:"predicates"`
	OnSuccess  []tkvOperation `msgpack:"on_success"`
	OnFailure  []tkvOperation `msgpack:"on_failure"`
}

func newTxnRequest(
	predicates []predicate.Predicate,
	onSuccess []operation.Operation,
	onFailure []operation.Operation,
) txnRequest {
	return txnRequest{
		_msgpack:   struct{}{},
		Predicates: newTKVPredicates(predicates),
		OnSuccess:  newTKVOperations(onSuccess),
		OnFailure:  newTKVOperations(onFailure),
	}
}
