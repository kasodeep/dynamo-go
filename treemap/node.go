package treemap

// color represents the color of a red-black tree node.
// Red nodes are "flexible" links — they can temporarily violate structure
// and are fixed via rotations/recoloring. Black nodes are "structural anchors"
// that define the black-height invariant.
type color bool

const (
	red   color = true
	black color = false
)

// node is an internal tree node. It holds a key-value pair, pointers to its
// left child, right child, and parent, plus its current color.
//
// Invariants maintained at all times:
//
//  1. Every node is either red or black.
//  2. The root is always black.
//  3. No two consecutive red nodes exist on any root→leaf path (red nodes
//     may not have red children).
//  4. Every root→nil path passes through the same number of black nodes
//     (the "black-height" of the tree).
//
// These four invariants guarantee height ≤ 2·⌊log₂(n+1)⌋.
type node[K Key, V any] struct {
	key    K
	value  V
	left   *node[K, V]
	right  *node[K, V]
	parent *node[K, V]
	color  color
}

// newNode allocates a fresh node. All new insertions start red (so we never
// break the black-height invariant on arrival); the root is immediately
// recoloured black by the caller.
func newNode[K Key, V any](key K, value V) *node[K, V] {
	return &node[K, V]{
		key:   key,
		value: value,
		color: red, // always inserted red; root recolour happens in Tree.Insert
	}
}

// isRed is a nil-safe colour check. A nil node is considered black (sentinel).
func isRed[K Key, V any](n *node[K, V]) bool {
	return n != nil && n.color == red
}

// sibling returns the other child of n's parent, or nil if n has no parent.
func (n *node[K, V]) sibling() *node[K, V] {
	if n.parent == nil {
		return nil
	}
	if n == n.parent.left {
		return n.parent.right
	}
	return n.parent.left
}

// uncle returns the sibling of n's parent (i.e., grandparent's other child),
// or nil if n has no grandparent.
func (n *node[K, V]) uncle() *node[K, V] {
	if n.parent == nil {
		return nil
	}
	return n.parent.sibling()
}

// grandparent returns n's grandparent or nil.
func (n *node[K, V]) grandparent() *node[K, V] {
	if n.parent == nil {
		return nil
	}
	return n.parent.parent
}
