package treemap

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// blackHeight returns the black-height of the subtree rooted at n, or -1 if
// the black-height invariant is violated. A nil node has black-height 0.
func blackHeight[K Key, V any](n *node[K, V]) int {
	if n == nil {
		return 0
	}
	lh := blackHeight(n.left)
	rh := blackHeight(n.right)
	if lh == -1 || rh == -1 || lh != rh {
		return -1
	}
	if n.color == black {
		return lh + 1
	}
	return lh
}

// checkRB verifies all four RB invariants. Returns a non-empty error string on
// any violation.
func checkRB[K Key, V any](t *Tree[K, V]) string {
	if t.root == nil {
		return ""
	}
	if t.root.color != black {
		return "root is not black"
	}
	var check func(n *node[K, V]) string
	check = func(n *node[K, V]) string {
		if n == nil {
			return ""
		}
		if n.color == red {
			if isRed(n.left) {
				return fmt.Sprintf("consecutive reds at key %v (left child)", n.key)
			}
			if isRed(n.right) {
				return fmt.Sprintf("consecutive reds at key %v (right child)", n.key)
			}
		}
		if n.left != nil && n.left.parent != n {
			return fmt.Sprintf("parent pointer broken at key %v (left)", n.key)
		}
		if n.right != nil && n.right.parent != n {
			return fmt.Sprintf("parent pointer broken at key %v (right)", n.key)
		}
		if s := check(n.left); s != "" {
			return s
		}
		return check(n.right)
	}
	if s := check(t.root); s != "" {
		return s
	}
	if blackHeight(t.root) == -1 {
		return "black-height invariant violated"
	}
	return ""
}

// mustRB fails the test if the tree violates any RB invariant.
func mustRB[K Key, V any](tb testing.TB, t *Tree[K, V]) {
	tb.Helper()
	if s := checkRB(t); s != "" {
		tb.Fatalf("RB violation: %s", s)
	}
}

// ─── Insert ───────────────────────────────────────────────────────────────────

func TestInsertSingle(t *testing.T) {
	tr := New[int, string]()
	tr.Insert(1, "a")
	mustRB(t, tr)
	if tr.Len() != 1 {
		t.Fatalf("want len 1, got %d", tr.Len())
	}
	v, ok := tr.Get(1)
	if !ok || v != "a" {
		t.Fatalf("want (a, true), got (%v, %v)", v, ok)
	}
}

func TestInsertAscending(t *testing.T) {
	tr := New[int, int]()
	for i := 1; i <= 100; i++ {
		tr.Insert(i, i*10)
		mustRB(t, tr)
	}
	if tr.Len() != 100 {
		t.Fatalf("want 100, got %d", tr.Len())
	}
}

func TestInsertDescending(t *testing.T) {
	tr := New[int, int]()
	for i := 100; i >= 1; i-- {
		tr.Insert(i, i)
		mustRB(t, tr)
	}
}

func TestInsertRandom(t *testing.T) {
	tr := New[int, int]()
	keys := rand.Perm(500)
	for _, k := range keys {
		tr.Insert(k, k)
		mustRB(t, tr)
	}
	if tr.Len() != 500 {
		t.Fatalf("want 500, got %d", tr.Len())
	}
}

func TestInsertDuplicateUpdatesValue(t *testing.T) {
	tr := New[string, int]()
	tr.Insert("x", 1)
	tr.Insert("x", 99)
	if tr.Len() != 1 {
		t.Fatalf("want len 1 after duplicate insert, got %d", tr.Len())
	}
	v, _ := tr.Get("x")
	if v != 99 {
		t.Fatalf("want updated value 99, got %d", v)
	}
	mustRB(t, tr)
}

// ─── Get / Contains ───────────────────────────────────────────────────────────

func TestGetMissing(t *testing.T) {
	tr := New[int, string]()
	tr.Insert(1, "a")
	_, ok := tr.Get(999)
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestContains(t *testing.T) {
	tr := New[int, bool]()
	tr.Insert(42, true)
	if !tr.Contains(42) {
		t.Fatal("expected true")
	}
	if tr.Contains(0) {
		t.Fatal("expected false")
	}
}

// ─── Delete ───────────────────────────────────────────────────────────────────

func TestDeleteRoot(t *testing.T) {
	tr := New[int, int]()
	tr.Insert(1, 1)
	tr.Delete(1)
	mustRB(t, tr)
	if tr.Len() != 0 {
		t.Fatal("want empty after delete")
	}
}

func TestDeleteLeaf(t *testing.T) {
	tr := New[int, int]()
	tr.Insert(2, 2)
	tr.Insert(1, 1)
	tr.Insert(3, 3)
	tr.Delete(1)
	mustRB(t, tr)
	if tr.Contains(1) {
		t.Fatal("1 should be gone")
	}
	if tr.Len() != 2 {
		t.Fatal("wrong size")
	}
}

func TestDeleteWithTwoChildren(t *testing.T) {
	tr := New[int, int]()
	for i := 1; i <= 7; i++ {
		tr.Insert(i, i)
	}
	tr.Delete(4) // root in a balanced 7-node tree
	mustRB(t, tr)
	if tr.Contains(4) {
		t.Fatal("4 should be deleted")
	}
	if tr.Len() != 6 {
		t.Fatalf("want 6, got %d", tr.Len())
	}
}

func TestDeleteAllRandom(t *testing.T) {
	tr := New[int, int]()
	keys := rand.Perm(200)
	for _, k := range keys {
		tr.Insert(k, k)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, k := range keys {
		tr.Delete(k)
		mustRB(t, tr)
	}
	if tr.Len() != 0 {
		t.Fatalf("want 0 after deleting all, got %d", tr.Len())
	}
}

func TestDeleteNoop(t *testing.T) {
	tr := New[int, int]()
	tr.Insert(1, 1)
	tr.Delete(999) // no-op
	mustRB(t, tr)
	if tr.Len() != 1 {
		t.Fatal("size should not change")
	}
}

// ─── Min / Max ────────────────────────────────────────────────────────────────

func TestMinMax(t *testing.T) {
	tr := New[int, int]()
	_, ok := tr.Min()
	if ok {
		t.Fatal("empty tree Min should return false")
	}
	keys := []int{5, 2, 8, 1, 9}
	for _, k := range keys {
		tr.Insert(k, k)
	}
	min, _ := tr.Min()
	max, _ := tr.Max()
	if min != 1 {
		t.Fatalf("want min 1, got %d", min)
	}
	if max != 9 {
		t.Fatalf("want max 9, got %d", max)
	}
}

// ─── Floor / Ceiling ──────────────────────────────────────────────────────────

func TestFloor(t *testing.T) {
	tr := New[int, int]()
	for _, k := range []int{1, 3, 5, 7, 9} {
		tr.Insert(k, k)
	}
	cases := []struct {
		in, want int
		wantOk   bool
	}{
		{4, 3, true},
		{3, 3, true},
		{0, 0, false},
		{10, 9, true},
	}
	for _, c := range cases {
		got, ok := tr.Floor(c.in)
		if ok != c.wantOk || (ok && got != c.want) {
			t.Errorf("Floor(%d): got (%v,%v) want (%v,%v)", c.in, got, ok, c.want, c.wantOk)
		}
	}
}

func TestCeiling(t *testing.T) {
	tr := New[int, int]()
	for _, k := range []int{1, 3, 5, 7, 9} {
		tr.Insert(k, k)
	}
	cases := []struct {
		in, want int
		wantOk   bool
	}{
		{4, 5, true},
		{5, 5, true},
		{10, 0, false},
		{0, 1, true},
	}
	for _, c := range cases {
		got, ok := tr.Ceiling(c.in)
		if ok != c.wantOk || (ok && got != c.want) {
			t.Errorf("Ceiling(%d): got (%v,%v) want (%v,%v)", c.in, got, ok, c.want, c.wantOk)
		}
	}
}

// ─── InOrder / Keys / Values ──────────────────────────────────────────────────

func TestInOrderSorted(t *testing.T) {
	tr := New[int, int]()
	input := []int{5, 3, 7, 1, 4, 6, 8}
	for _, k := range input {
		tr.Insert(k, k)
	}
	got := tr.Keys()
	if !sort.IntsAreSorted(got) {
		t.Fatalf("InOrder not sorted: %v", got)
	}
	if len(got) != len(input) {
		t.Fatalf("wrong length: %v", got)
	}
}

func TestInOrderEarlyStop(t *testing.T) {
	tr := New[int, int]()
	for i := 1; i <= 10; i++ {
		tr.Insert(i, i)
	}
	count := 0
	tr.InOrder(func(k, v int) bool {
		count++
		return count < 5 // stop after 5 visits
	})
	if count != 5 {
		t.Fatalf("want 5 visits, got %d", count)
	}
}

// ─── String keys ──────────────────────────────────────────────────────────────

func TestStringKeys(t *testing.T) {
	tr := New[string, int]()
	words := []string{"banana", "apple", "cherry", "date", "avocado"}
	for i, w := range words {
		tr.Insert(w, i)
	}
	mustRB(t, tr)
	keys := tr.Keys()
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("string keys not sorted: %v", keys)
	}
}

// ─── Clear ────────────────────────────────────────────────────────────────────

func TestClear(t *testing.T) {
	tr := New[int, int]()
	for i := 0; i < 50; i++ {
		tr.Insert(i, i)
	}
	tr.Clear()
	if !tr.IsEmpty() || tr.Len() != 0 {
		t.Fatal("tree should be empty after Clear")
	}
	// should be re-usable after clear
	tr.Insert(1, 1)
	mustRB(t, tr)
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkInsert(b *testing.B) {
	tr := New[int, int]()
	for i := 0; i < b.N; i++ {
		tr.Insert(i, i)
	}
}

func BenchmarkGet(b *testing.B) {
	tr := New[int, int]()
	for i := 0; i < 1_000_000; i++ {
		tr.Insert(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Get(i % 1_000_000)
	}
}

func BenchmarkDelete(b *testing.B) {
	tr := New[int, int]()
	for i := 0; i < b.N; i++ {
		tr.Insert(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Delete(i)
	}
}
