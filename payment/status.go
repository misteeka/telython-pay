package payment

type Status int

var (
	SUCCESS    Status = 0
	PROCESSING Status = 1
	FAILED     Status = 2 // INTERNAL SERVER ERROR
)
