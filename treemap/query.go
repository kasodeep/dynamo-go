package treemap

// ─── Lookup ──────────────────────────────────────────────────────────────────

// Get returns the value associated with key and true, or the zero value and
// false if key is not present. Runs in O(log n).
//
//	t.Insert("go", 2009)
//	year, ok := t.Get("go")  // 2009, true
//	_, ok  = t.Get("rust")   // 0, false
func (t *Tree[K, V]) Get(key K) (V, bool) {
	n := t.find(key)
	if n == nil {
		var zero V
		return zero, false
	}
	return n.value, true
}

// Contains reports whether key is present in the tree. Runs in O(log n).
//
//	t.Insert(42, "hello")
//	t.Contains(42) // true
//	t.Contains(7)  // false
func (t *Tree[K, V]) Contains(key K) bool {
	return t.find(key) != nil
}

// find is the internal O(log n) BST search. Returns nil if not found.
func (t *Tree[K, V]) find(key K) *node[K, V] {
	cur := t.root
	for cur != nil {
		switch {
		case key < cur.key:
			cur = cur.left
		case key > cur.key:
			cur = cur.right
		default:
			return cur
		}
	}
	return nil
}

// ─── Order statistics ─────────────────────────────────────────────────────────

// Min returns the smallest key in the tree and true, or the zero key and false
// if the tree is empty. Runs in O(log n).
//
//	t.Insert(3, "c"); t.Insert(1, "a"); t.Insert(2, "b")
//	k, ok := t.Min() // 1, true
func (t *Tree[K, V]) Min() (K, bool) {
	if t.root == nil {
		var zero K
		return zero, false
	}
	m := minimum(t.root)
	return m.key, true
}

// Max returns the largest key in the tree and true, or the zero key and false
// if the tree is empty. Runs in O(log n).
//
//	t.Insert(3, "c"); t.Insert(1, "a"); t.Insert(2, "b")
//	k, ok := t.Max() // 3, true
func (t *Tree[K, V]) Max() (K, bool) {
	if t.root == nil {
		var zero K
		return zero, false
	}
	m := maximum(t.root)
	return m.key, true
}

// minimum returns the leftmost (smallest) node in the subtree rooted at n.
// n must be non-nil.
func minimum[K Key, V any](n *node[K, V]) *node[K, V] {
	for n.left != nil {
		n = n.left
	}
	return n
}

// maximum returns the rightmost (largest) node in the subtree rooted at n.
// n must be non-nil.
func maximum[K Key, V any](n *node[K, V]) *node[K, V] {
	for n.right != nil {
		n = n.right
	}
	return n
}

// Floor returns the largest key ≤ the given key, and true. Returns zero and
// false if no such key exists (i.e., all keys are strictly greater). O(log n).
//
//	t.Insert(1, _); t.Insert(3, _); t.Insert(5, _)
//	k, ok := t.Floor(4)  // 3, true  (largest key ≤ 4)
//	k, ok  = t.Floor(0)  // 0, false (no key ≤ 0)
func (t *Tree[K, V]) Floor(key K) (K, bool) {
	n := t.floor(t.root, key)
	if n == nil {
		var zero K
		return zero, false
	}
	return n.key, true
}

func (t *Tree[K, V]) floor(n *node[K, V], key K) *node[K, V] {
	if n == nil {
		return nil
	}
	switch {
	case key == n.key:
		return n
	case key < n.key:
		return t.floor(n.left, key)
	default:
		// n.key < key: n is a candidate; check if something larger exists in right subtree
		if r := t.floor(n.right, key); r != nil {
			return r
		}
		return n
	}
}

// Ceiling returns the smallest key ≥ the given key, and true. Returns zero and
// false if no such key exists (i.e., all keys are strictly smaller). O(log n).
//
//	t.Insert(1, _); t.Insert(3, _); t.Insert(5, _)
//	k, ok := t.Ceiling(2) // 3, true  (smallest key ≥ 2)
//	k, ok  = t.Ceiling(6) // 0, false (no key ≥ 6)
func (t *Tree[K, V]) Ceiling(key K) (K, bool) {
	n := t.ceiling(t.root, key)
	if n == nil {
		var zero K
		return zero, false
	}
	return n.key, true
}

func (t *Tree[K, V]) ceiling(n *node[K, V], key K) *node[K, V] {
	if n == nil {
		return nil
	}
	switch {
	case key == n.key:
		return n
	case key > n.key:
		return t.ceiling(n.right, key)
	default:
		// key < n.key: n is a candidate; check if something smaller exists in left subtree
		if l := t.ceiling(n.left, key); l != nil {
			return l
		}
		return n
	}
}

// ─── Size ─────────────────────────────────────────────────────────────────────

// Len returns the number of key-value pairs in the tree. O(1).
//
//	t.Insert(1, "a"); t.Insert(2, "b")
//	t.Len() // 2
func (t *Tree[K, V]) Len() int {
	return t.size
}

// IsEmpty reports whether the tree contains no keys. O(1).
//
//	t := New[int, string]()
//	t.IsEmpty() // true
//	t.Insert(1, "a")
//	t.IsEmpty() // false
func (t *Tree[K, V]) IsEmpty() bool {
	return t.size == 0
}

// ─── Iteration ────────────────────────────────────────────────────────────────

// InOrder calls fn for each key-value pair in ascending key order.
// Iteration stops early if fn returns false. Runs in O(n).
//
//	t.Insert(3, "c"); t.Insert(1, "a"); t.Insert(2, "b")
//	t.InOrder(func(k int, v string) bool {
//	    fmt.Println(k, v) // prints: 1 a, 2 b, 3 c
//	    return true
//	})
func (t *Tree[K, V]) InOrder(fn func(key K, value V) bool) {
	inOrder(t.root, fn)
}

func inOrder[K Key, V any](n *node[K, V], fn func(K, V) bool) bool {
	if n == nil {
		return true
	}
	if !inOrder(n.left, fn) {
		return false
	}
	if !fn(n.key, n.value) {
		return false
	}
	return inOrder(n.right, fn)
}

// Keys returns all keys in ascending sorted order. O(n).
//
//	t.Insert(3, _); t.Insert(1, _); t.Insert(2, _)
//	t.Keys() // [1, 2, 3]
func (t *Tree[K, V]) Keys() []K {
	out := make([]K, 0, t.size)
	t.InOrder(func(k K, _ V) bool {
		out = append(out, k)
		return true
	})
	return out
}

// Values returns all values in the order of their ascending keys. O(n).
//
//	t.Insert(3, "c"); t.Insert(1, "a"); t.Insert(2, "b")
//	t.Values() // ["a", "b", "c"]
func (t *Tree[K, V]) Values() []V {
	out := make([]V, 0, t.size)
	t.InOrder(func(_ K, v V) bool {
		out = append(out, v)
		return true
	})
	return out
}

// ─── Mutation helpers ─────────────────────────────────────────────────────────

// Clear removes all entries from the tree. O(1).
//
//	t.Insert(1, "a")
//	t.Clear()
//	t.Len()     // 0
//	t.IsEmpty() // true
func (t *Tree[K, V]) Clear() {
	t.root = nil
	t.size = 0
}
