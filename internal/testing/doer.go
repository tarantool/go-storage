package testing

import (
	"bytes"
	"sync"

	"github.com/tarantool/go-tarantool/v2"
)

type doerResponse struct {
	resp *MockResponse
	err  error
}

// MockDoer is an implementation of the Doer interface
// used for testing purposes.
type MockDoer struct {
	mu sync.Mutex
	// Requests is a slice of received requests.
	// It could be used to compare incoming requests with expected.
	Requests  []tarantool.Request
	responses []doerResponse
	t         T
}

// NewMockDoer creates a MockDoer by given responses.
// Each response could be one of two types: MockResponse or error.
func NewMockDoer(t T, responses ...any) *MockDoer {
	t.Helper()

	mockDoer := &MockDoer{
		mu:        sync.Mutex{},
		t:         t,
		Requests:  []tarantool.Request{},
		responses: []doerResponse{},
	}

	for _, response := range responses {
		doerResp := doerResponse{
			resp: nil,
			err:  nil,
		}

		switch resp := response.(type) {
		case *MockResponse:
			doerResp.resp = resp
		case error:
			doerResp.err = resp
		default:
			t.Fatalf("unsupported type: %T", response)
		}

		mockDoer.responses = append(mockDoer.responses, doerResp)
	}

	return mockDoer
}

// Do returns a future with the current response or an error.
// It saves the current request into MockDoer.Requests.
func (d *MockDoer) Do(req tarantool.Request) *tarantool.Future {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.Requests = append(d.Requests, req)

	mockReq := NewMockRequest()
	fut := tarantool.NewFuture(mockReq)

	if len(d.responses) == 0 {
		d.t.Fatalf("list of responses is empty")
	}

	response := d.responses[0]

	if response.err != nil {
		fut.SetError(response.err)
	} else {
		_ = fut.SetResponse(response.resp.header, bytes.NewBuffer(response.resp.data))
	}

	d.responses = d.responses[1:]

	return fut
}
