package grpc

import "testing"

func TestAnyToBrickValueAcceptsUnsignedIntegers(t *testing.T) {
	t.Parallel()

	cases := []any{
		uint8(7),
		uint16(7),
		uint32(7),
		uint64(7),
		uint(7),
		int8(7),
		int16(7),
		int32(7),
	}
	for _, input := range cases {
		value, err := AnyToBrickValue(input)
		if err != nil {
			t.Fatalf("%T: %v", input, err)
		}
		if got := value.GetSafeIntegerValue(); got != 7 {
			t.Fatalf("%T: got %d", input, got)
		}
	}
}

func TestAnyToBrickValueRejectsUnsafeUint64(t *testing.T) {
	t.Parallel()

	_, err := AnyToBrickValue(uint64(1 << 53))
	if err == nil {
		t.Fatal("expected overflow error")
	}
}
