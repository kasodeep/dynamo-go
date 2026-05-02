// Package treemap provides a generic, ordered key-value map backed by a
// left-leaning Red-Black BST. All mutating operations run in O(log n)
// worst-case time; iteration is O(n).
//
// Supported key types are any type whose underlying kind is int, string, or
// float64 — captured by the [Key] constraint.
//
// # Red-Black Tree invariants
//
//  1. Every node is red or black.
//  2. The root is black.
//  3. Red nodes have only black children (no two consecutive reds).
//  4. Every root→nil path has the same number of black nodes (black-height).
//
// These four invariants guarantee height ≤ 2·log₂(n+1), so all operations
// are O(log n) in the worst case.
//
// # Example
//
//	t := treemap.New[string, int]()
//	t.Insert("banana", 2)
//	t.Insert("apple",  1)
//	t.Insert("cherry", 3)
//
//	v, ok := t.Get("apple")  // 1, true
//	t.Delete("banana")
//	fmt.Println(t.Len())     // 2
package treemap

// Key constrains which types may be used as map keys.
// Any named type whose underlying kind is int, string, or float64 qualifies.
type Key interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		~string
}

// Tree is a generic sorted map from keys of type K to values of type V.
// The zero value is ready to use — call [New] to get an empty tree.
//
// Tree is NOT safe for concurrent use. Wrap with a sync.RWMutex if you need
// concurrent reads or writes.
type Tree[K Key, V any] struct {
	root *node[K, V]
	size int
}

// New returns an empty Tree.
//
//	t := treemap.New[int, string]()
func New[K Key, V any]() *Tree[K, V] {
	return &Tree[K, V]{}
}
