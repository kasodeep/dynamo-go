package treemap

// rotateLeft performs a left rotation around pivot x.
//
//	  x                y
//	 / \              / \
//	A   y    -->    x   C
//	   / \         / \
//	  B   C       A   B
//
// After rotation: y takes x's position in the tree, x becomes y's left child,
// and y's former left child B becomes x's right child.
// Colors are NOT changed here — the caller is responsible.
func (t *Tree[K, V]) rotateLeft(x *node[K, V]) {
	y := x.right // y must be non-nil; callers guarantee this
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	t.transplant(x, y)
	y.left = x
	x.parent = y
}

// rotateRight performs a right rotation around pivot y.
//
//	    y              x
//	   / \            / \
//	  x   C  -->    A   y
//	 / \               / \
//	A   B             B   C
//
// After rotation: x takes y's position, y becomes x's right child,
// and x's former right child B becomes y's left child.
// Colors are NOT changed here — the caller is responsible.
func (t *Tree[K, V]) rotateRight(y *node[K, V]) {
	x := y.left // x must be non-nil; callers guarantee this
	y.left = x.right
	if x.right != nil {
		x.right.parent = y
	}
	x.parent = y.parent
	t.transplant(y, x)
	x.right = y
	y.parent = x
}

// transplant replaces the subtree rooted at u with the subtree rooted at v.
// It only rewires the parent pointer — child pointers of u or v are not
// touched, so callers must fix those themselves.
func (t *Tree[K, V]) transplant(u, v *node[K, V]) {
	switch {
	case u.parent == nil:
		t.root = v
	case u == u.parent.left:
		u.parent.left = v
	default:
		u.parent.right = v
	}
	if v != nil {
		v.parent = u.parent
	}
}
