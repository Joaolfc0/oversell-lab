package bench

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i))
	}
	cases := map[float64]time.Duration{0: 1, 50: 50, 95: 95, 99: 99, 100: 100}
	for p, want := range cases {
		if got := Percentile(d, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("empty slice: got %v", got)
	}
	if got := Percentile([]time.Duration{7}, 99); got != 7 {
		t.Errorf("single element: got %v", got)
	}
}

func TestViolations(t *testing.T) {
	ok := Report{Capacity: 10, Buyers: 20, Purchased: 10, TicketsIssued: 10, Counter: 10}
	if !ok.Correct() {
		t.Fatalf("expected correct, got %v", ok.Violations())
	}
	bad := Report{Capacity: 10, Purchased: 13, TicketsIssued: 13, Counter: 9, Oversold: 3, CounterDrift: -4}
	if got := len(bad.Violations()); got != 2 {
		t.Fatalf("expected 2 violations, got %v", bad.Violations())
	}
}

func TestHumanInt(t *testing.T) {
	for in, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 1234567: "1,234,567", -2999: "-2,999"} {
		if got := humanInt(in); got != want {
			t.Errorf("humanInt(%d) = %q, want %q", in, got, want)
		}
	}
}
