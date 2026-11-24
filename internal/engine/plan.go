package engine

const MinChunk = 1 << 20

type Range struct {
	Start, End int64
}

func (r Range) Len() int64 { return r.End - r.Start + 1 }

func Plan(info FileInfo, n int) []Range {
	size := info.Size
	if size <= 0 {
		return nil
	}
	if !info.AcceptsRanges || n < 1 {
		n = 1
	}
	if maxN := size / MinChunk; int64(n) > maxN {
		n = int(max(maxN, 1))
	}

	ranges := make([]Range, n)
	base, extra := size/int64(n), size%int64(n)
	var start int64
	for i := range ranges {
		l := base
		if int64(i) < extra {
			l++
		}
		ranges[i] = Range{Start: start, End: start + l - 1}
		start += l
	}
	return ranges
}
