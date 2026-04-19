package treemap

// Delete removes the node with the given key from the tree. It is a no-op if
// the key does not exist. Runs in O(log n).
//
//	t.Insert("a", 1)
//	t.Insert("b", 2)
//	t.Delete("a")
//	_, ok := t.Get("a") // false
func (t *Tree[K, V]) Delete(key K) {
	z := t.find(key)
	if z == nil {
		return
	}
	t.delete(z)
	t.size--
}

// delete removes node z from the tree and restores RB invariants.
//
// The algorithm follows CLRS §13.4 closely.
//
//   - y is the node that is physically removed (either z itself, or z's
//     in-order successor when z has two children).
//   - x is the node that moves into y's original position; it may be nil
//     (representing a "double-black" nil sentinel).
//   - yOrigColor records y's colour before any changes so we know whether
//     fixDelete is needed.
func (t *Tree[K, V]) delete(z *node[K, V]) {
	y := z
	yOrigColor := y.color

	var x *node[K, V]       // node moving into y's old spot
	var xParent *node[K, V] // x's parent (needed when x is nil)

	switch {
	case z.left == nil:
		// z has at most a right child → splice out z directly.
		x = z.right
		xParent = z.parent
		t.transplant(z, z.right)

	case z.right == nil:
		// z has only a left child → splice out z directly.
		x = z.left
		xParent = z.parent
		t.transplant(z, z.left)

	default:
		// z has two children → replace z's key/value with its in-order
		// successor (minimum of z's right subtree), then delete the successor.
		y = minimum(z.right)
		yOrigColor = y.color
		x = y.right

		if y.parent == z {
			// y is z's direct right child.
			xParent = y
		} else {
			xParent = y.parent
			t.transplant(y, y.right)
			y.right = z.right
			y.right.parent = y
		}
		t.transplant(z, y)
		y.left = z.left
		y.left.parent = y
		y.color = z.color
	}

	// If the physically removed node was black, a black-height violation may
	// exist at x's position — fix it.
	if yOrigColor == black {
		t.fixDelete(x, xParent)
	}
}

// fixDelete restores the RB invariants after a black node was removed.
// x is the node at the "problem" position (may be nil = double-black nil);
// xParent is x's actual parent (needed because x can be nil).
//
// There are four cases (plus their left/right mirrors). CLRS calls them cases
// 1–4. In all cases, we track a "double-black" token that must be absorbed:
//
//   - Case 1 — sibling w is red:
//     Rotate parent toward x; recolour parent red and w black.
//     This converts to Case 2, 3, or 4 (sibling is now black).
//
//   - Case 2 — sibling w is black with two black children:
//     Recolour w red; move double-black up to parent.
//     If parent was red, it absorbs the token (loop ends); otherwise repeat.
//
//   - Case 3 — sibling w is black, near child red, far child black:
//     Rotate sibling away from x; swap w and near-child colours.
//     Now converts to Case 4.
//
//   - Case 4 — sibling w is black, far child red:
//     Rotate parent toward x; recolour to absorb the double-black.
//     Loop terminates.
func (t *Tree[K, V]) fixDelete(x *node[K, V], xParent *node[K, V]) {
	for x != t.root && !isRed[K, V](x) {
		if xParent == nil {
			break
		}

		if x == xParent.left {
			w := xParent.right // sibling of x

			if isRed[K, V](w) {
				// ── Case 1 ────────────────────────────────────────────────────
				w.color = black
				xParent.color = red
				t.rotateLeft(xParent)
				w = xParent.right // new sibling after rotation
			}

			if !isRed[K, V](w.left) && !isRed[K, V](w.right) {
				// ── Case 2 ────────────────────────────────────────────────────
				w.color = red
				x = xParent
				xParent = x.parent

			} else {
				if !isRed[K, V](w.right) {
					// ── Case 3 ────────────────────────────────────────────────
					if w.left != nil {
						w.left.color = black
					}
					w.color = red
					t.rotateRight(w)
					w = xParent.right
				}
				// ── Case 4 ────────────────────────────────────────────────────
				w.color = xParent.color
				xParent.color = black
				if w.right != nil {
					w.right.color = black
				}
				t.rotateLeft(xParent)
				x = t.root // done
			}

		} else { // mirror: x is xParent.right
			w := xParent.left

			if isRed[K, V](w) {
				// ── Case 1 (mirror) ───────────────────────────────────────────
				w.color = black
				xParent.color = red
				t.rotateRight(xParent)
				w = xParent.left
			}

			if !isRed[K, V](w.right) && !isRed[K, V](w.left) {
				// ── Case 2 (mirror) ───────────────────────────────────────────
				w.color = red
				x = xParent
				xParent = x.parent

			} else {
				if !isRed[K, V](w.left) {
					// ── Case 3 (mirror) ───────────────────────────────────────
					if w.right != nil {
						w.right.color = black
					}
					w.color = red
					t.rotateLeft(w)
					w = xParent.left
				}
				// ── Case 4 (mirror) ───────────────────────────────────────────
				w.color = xParent.color
				xParent.color = black
				if w.left != nil {
					w.left.color = black
				}
				t.rotateRight(xParent)
				x = t.root
			}
		}
	}

	// Absorb any remaining double-black at x.
	if x != nil {
		x.color = black
	}
}
