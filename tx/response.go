package tx

// Response contains the result of a transaction execution.
type Response struct {
	// Succeeded indicates whether the transaction predicates evaluated to true.
	Succeeded bool
	// Results contains the responses for each operation in Then/Else blocks.
	Results []RequestResponse
}
