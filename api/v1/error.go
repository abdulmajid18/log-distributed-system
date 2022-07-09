package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("Offset out of range: %d", e.Offset))

	msg := fmt.Sprintf("The requested offset: %d is out of the log's range", e.Offset)

	d := &errdetails.LocalizedMessage{
		Message: msg,
		Locale:  "en-US",
	}

	statusDetails, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return statusDetails
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
