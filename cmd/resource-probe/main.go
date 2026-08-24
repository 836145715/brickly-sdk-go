package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func main() {
	options, err := runtimev1.TakeRuntimeEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	client, err := runtimev1.NewHostResourceClient(options.HostEndpoint, options.RuntimeToHostToken)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer client.Close()
	payload := bytes.Repeat([]byte{7}, 1024*1024)
	ref, err := client.Create(context.Background(), payload, "probe.bin", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	read, err := client.Read(context.Background(), ref.GetResourceId())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !bytes.Equal(read, payload) {
		fmt.Fprintln(os.Stderr, "读取内容与上传不一致")
		os.Exit(1)
	}
	fmt.Println("ok")
}
