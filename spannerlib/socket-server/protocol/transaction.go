package protocol

import (
	"bufio"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
)

func ReadTransactionOptions(reader *bufio.Reader) (*spannerpb.TransactionOptions, error) {
	var b []byte
	if err := ReadBytes(reader, &b); err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	options := &spannerpb.TransactionOptions{}
	if err := proto.Unmarshal(b, options); err != nil {
		return nil, err
	}
	return options, nil
}

func WriteTransactionOptions(writer *bufio.Writer, options *spannerpb.TransactionOptions) error {
	if options == nil {
		if err := WriteBytes(writer, []byte{}); err != nil {
			return err
		}
		return nil
	}
	b, err := proto.Marshal(options)
	if err != nil {
		return err
	}
	if err := WriteBytes(writer, b); err != nil {
		return err
	}
	return nil
}

func ReadCommitResponse(reader *bufio.Reader) (*spannerpb.CommitResponse, error) {
	var b []byte
	if err := ReadBytes(reader, &b); err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	resp := &spannerpb.CommitResponse{}
	if err := proto.Unmarshal(b, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func WriteCommitResponse(writer *bufio.Writer, response *spannerpb.CommitResponse) error {
	if response == nil {
		if err := WriteBytes(writer, []byte{}); err != nil {
			return err
		}
		return nil
	}
	b, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	if err := WriteBytes(writer, b); err != nil {
		return err
	}
	return nil
}
