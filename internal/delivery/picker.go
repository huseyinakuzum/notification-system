package delivery

type lane int

const (
	laneHigh lane = iota
	laneNormal
	laneLow
)

// picker selects strict high→normal→low, but forces a low pick after
// agingThreshold consecutive high/normal picks so low never starves.
type picker struct {
	agingThreshold int
	sinceLow       int
}

func newPicker(agingThreshold int) *picker {
	return &picker{agingThreshold: agingThreshold}
}

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
