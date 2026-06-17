package delivery

import "testing"

func TestPickerStrictPriority(t *testing.T) {
	p := newPicker(100) // high aging threshold → no forced low
	// all lanes available → always high
	for i := 0; i < 5; i++ {
		if got, ok := p.pick([3]bool{true, true, true}); !ok || got != laneHigh {
			t.Fatalf("iter %d: got lane %d ok %v, want high", i, got, ok)
		}
	}
}

func TestPickerFallsThrough(t *testing.T) {
	p := newPicker(100)
	if got, ok := p.pick([3]bool{false, true, true}); !ok || got != laneNormal {
		t.Errorf("no high: got %d ok %v, want normal", got, ok)
	}
	if got, ok := p.pick([3]bool{false, false, true}); !ok || got != laneLow {
		t.Errorf("only low: got %d ok %v, want low", got, ok)
	}
}

func TestPickerNoneAvailable(t *testing.T) {
	p := newPicker(100)
	if _, ok := p.pick([3]bool{false, false, false}); ok {
		t.Error("expected ok=false when nothing available")
	}
}

func TestPickerAntiStarvation(t *testing.T) {
	p := newPicker(3) // after 3 consecutive high/normal picks, force a low
	avail := [3]bool{true, false, true}
	// first 3 picks are high (high/normal), 4th forced to low
	for i := 0; i < 3; i++ {
		if got, _ := p.pick(avail); got != laneHigh {
			t.Fatalf("pick %d: got %d, want high", i, got)
		}
	}
	if got, _ := p.pick(avail); got != laneLow {
		t.Errorf("4th pick: got %d, want forced low", got)
	}
	// counter reset → high again
	if got, _ := p.pick(avail); got != laneHigh {
		t.Errorf("after forced low: got %d, want high", got)
	}
}

func TestPickerAgingSkipsWhenLowEmpty(t *testing.T) {
	p := newPicker(1)
	// threshold reached but low not available → serve high, do not stall
	if got, ok := p.pick([3]bool{true, false, false}); !ok || got != laneHigh {
		t.Errorf("got %d ok %v, want high when low empty", got, ok)
	}
}
