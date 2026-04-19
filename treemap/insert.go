package treemap

// Insert adds the key-value pair to the tree. If key already exists, its
// value is updated in place (no duplicate keys). Runs in O(log n).
//
//	t := treemap.New[string, int]()
//	t.Insert("a", 1)
//	t.Insert("b", 2)
//	t.Insert("a", 99) // updates existing key "a" to 99
func (t *Tree[K, V]) Insert(key K, value V) {
	// --- Phase 1: plain BST insert ---
	n := newNode(key, value)

	if t.root == nil {
		n.color = black
		t.root = n
		t.size++
		return
	}

	cur := t.root
	for {
		switch {
		case key < cur.key:
			if cur.left == nil {
				cur.left = n
				n.parent = cur
				t.size++
				t.fixInsert(n)
				return
			}
			cur = cur.left

		case key > cur.key:
			if cur.right == nil {
				cur.right = n
				n.parent = cur
				t.size++
				t.fixInsert(n)
				return
			}
			cur = cur.right

		default: // key == cur.key → update value, no structural change needed
			cur.value = value
			return
		}
	}
}

// fixInsert restores red-black invariants after inserting node z (which starts
// red). There are three cases, each with a mirror image for left/right:
//
//   - Case 1 — uncle is red:
//     Recolour parent + uncle black, grandparent red. Move z up to grandparent
//     and repeat. This "pushes" the red violation upward.
//
//   - Case 2 — uncle is black, z is an "inner" grandchild (triangle shape):
//     Rotate parent in the direction that straightens the triangle into a line.
//     Fall through to Case 3.
//
//   - Case 3 — uncle is black, z is an "outer" grandchild (line shape):
//     Rotate grandparent and swap colours of parent and grandparent.
//     This terminates the loop.
//
// CLRS Theorem 13.1 guarantees this runs in O(log n) with O(1) rotations.
func (t *Tree[K, V]) fixInsert(z *node[K, V]) {
	for isRed(z.parent) {
		parent := z.parent
		gp := parent.parent // grandparent; non-nil because parent is red → not root

		if parent == gp.left {
			uncle := gp.right

			if isRed(uncle) {
				// ── Case 1: uncle red ──────────────────────────────────────────
				// Recolour and move the violation up to grandparent.
				parent.color = black
				uncle.color = black
				gp.color = red
				z = gp // continue from grandparent

			} else {
				if z == parent.right {
					// ── Case 2: triangle (left-right) ─────────────────────────
					// Left-rotate parent to convert to Case 3.
					z = parent
					t.rotateLeft(z)
					parent = z.parent
				}
				// ── Case 3: line (left-left) ──────────────────────────────────
				parent.color = black
				parent.parent.color = red
				t.rotateRight(parent.parent)
			}

		} else { // mirror: parent is gp.right
			uncle := gp.left

			if isRed(uncle) {
				// ── Case 1 (mirror) ───────────────────────────────────────────
				parent.color = black
				uncle.color = black
				gp.color = red
				z = gp

			} else {
				if z == parent.left {
					// ── Case 2 (mirror): triangle (right-left) ────────────────
					z = parent
					t.rotateRight(z)
					parent = z.parent
				}
				// ── Case 3 (mirror): line (right-right) ───────────────────────
				parent.color = black
				parent.parent.color = red
				t.rotateLeft(parent.parent)
			}
		}
	}
	// Invariant 2: root must always be black.
	t.root.color = black
}
