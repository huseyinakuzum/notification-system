package delivery

type lane int

const (
	laneHigh lane = iota
	laneNormal
	laneLow
)

// picker implements strict high→normal→low selection with anti-starvation:
// after agingThreshold consecutive high/normal picks it forces one low pick
// (when low has work), so the low lane never starves under sustained pressure.
type picker struct {
	agingThreshold int
	sinceLow       int
}

func newPicker(agingThreshold int) *picker {
	return &picker{agingThreshold: agingThreshold}
}

// pick chooses a lane given which lanes currently have work. ok is false when
// nothing is available.
func (p *picker) pick(available [3]bool) (lane, bool) {
	if p.agingThreshold > 0 && p.sinceLow >= p.agingThreshold && available[laneLow] {
		p.sinceLow = 0
		return laneLow, true
	}
	if available[laneHigh] {
		p.sinceLow++
		return laneHigh, true
	}
	if available[laneNormal] {
		p.sinceLow++
		return laneNormal, true
	}
	if available[laneLow] {
		p.sinceLow = 0
		return laneLow, true
	}
	return 0, false
}
