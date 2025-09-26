package tcs

import (
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-storage/kv"
	goOperation "github.com/tarantool/go-storage/operation"
	goPredicate "github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
)

type txnOpResponse struct {
	Response []struct {
		Path        []byte `msgpack:"path"`
		ModRevision int64  `msgpack:"mod_revision"`
		Value       []byte `msgpack:"value"`
	}
}

func (t *txnOpResponse) DecodeMsgpack(decoder *msgpack.Decoder) error {
	err := decoder.Decode(&t.Response)
	if err != nil {
		return NewTxnOpResponseDecodingError(err)
	}

	return nil
}

type txnResponse struct {
	Data struct {
		IsSuccess bool            `msgpack:"is_success"`
		Responses []txnOpResponse `msgpack:"responses"`
	} `msgpack:"data"`
	Revision int64 `msgpack:"revision"`
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

	Predicates []predicate `msgpack:"predicates"`
	OnSuccess  []operation `msgpack:"on_success"`
	OnFailure  []operation `msgpack:"on_failure"`
}

func newTxnRequest(
	predicates []goPredicate.Predicate,
	onSuccess []goOperation.Operation,
	onFailure []goOperation.Operation,
) txnRequest {
	return txnRequest{
		_msgpack:   struct{}{},
		Predicates: newPredicates(predicates),
		OnSuccess:  newOperations(onSuccess),
		OnFailure:  newOperations(onFailure),
	}
}
