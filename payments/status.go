package payments

type Status int

var (
	SUCCESS    Status = 0
	PROCESSING Status = 1
	WITHDRAWN  Status = 2
	FAILED     Status = 3 // INTERNAL SERVER ERROR
)
