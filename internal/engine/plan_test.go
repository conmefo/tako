package engine

import "testing"

func TestPlan(t *testing.T) {
	const MB = MinChunk
	tests := []struct {
		name  string
		info  FileInfo
		n     int
		wantN int
	}{
		{"100 MB, 4 conns", FileInfo{Size: 100 * MB, AcceptsRanges: true}, 4, 4},
		{"no ranges → 1", FileInfo{Size: 100 * MB}, 4, 1},
		{"small file → fewer", FileInfo{Size: 3*MB + 5, AcceptsRanges: true}, 8, 3},
		{"tiny file → 1", FileInfo{Size: 100, AcceptsRanges: true}, 8, 1},
		{"n=0 → 1", FileInfo{Size: 100 * MB, AcceptsRanges: true}, 0, 1},
		{"empty → none", FileInfo{Size: 0, AcceptsRanges: true}, 4, 0},
		{"unknown size → none", FileInfo{Size: -1}, 4, 0},
		{"odd size, remainder spread", FileInfo{Size: 10*MB + 3, AcceptsRanges: true}, 4, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := Plan(tt.info, tt.n)
			if len(ranges) != tt.wantN {
				t.Fatalf("got %d ranges, want %d: %v", len(ranges), tt.wantN, ranges)
			}
			if tt.wantN == 0 {
				return
			}
			var next int64
			minL, maxL := ranges[0].Len(), ranges[0].Len()
			for i, r := range ranges {
				if r.Start != next {
					t.Fatalf("range %d starts at %d, want %d", i, r.Start, next)
				}
				if r.End < r.Start {
					t.Fatalf("range %d is empty: %+v", i, r)
				}
				next = r.End + 1
				minL, maxL = min(minL, r.Len()), max(maxL, r.Len())
			}
			if next != tt.info.Size {
				t.Fatalf("ranges end at %d, size is %d", next, tt.info.Size)
			}
			if maxL-minL > 1 {
				t.Errorf("uneven ranges: lengths %d..%d", minL, maxL)
			}
		})
	}
}
