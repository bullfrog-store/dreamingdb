package bptree

import (
	"bytes"
	"errors"
)

const (
	defaultOrder = 4
)

type Option func(bpt *BPlusTree) error

// SetOrder sets the BPlusTree's order
func SetOrder(order int) Option {
	return func(bpt *BPlusTree) error {
		if order < 3 {
			return errors.New("order can'bpt be less than 3")
		}
		bpt.order = order
		return nil
	}
}

type BPlusTree struct {
	// root of the b plus tree
	root *node

	// the most left node
	mostLeftNode *node

	// the order of branching factor of b plus tree,
	// that is, the capacity for internal nodes.
	order int

	// the number of keys
	size int

	// the min of number of keys allowed
	minKeyNum int
}

func NewBPlusTree(options ...Option) (*BPlusTree, error) {
	bpt := &BPlusTree{order: defaultOrder}
	for _, opt := range options {
		if err := opt(bpt); err != nil {
			return nil, err
		}
	}
	bpt.minKeyNum = ceil(bpt.order, 2) - 1
	return bpt, nil
}

// Init inits a bpt whose root is nil
func (bpt *BPlusTree) init(key, value []byte) {
	keys := make([][]byte, bpt.order-1)
	copy(keys[0], key)
	pointers := make([]*pointer, bpt.order-1)
	pointers[0] = &pointer{data: value}
	bpt.root = &node{
		leaf:     true,
		parent:   nil,
		keys:     keys,
		keyNums:  1,
		pointers: pointers,
	}
	bpt.mostLeftNode = bpt.root
	bpt.size++
}

// Get returns the value and true if the given key exists,
// otherwise nil and false
func (bpt *BPlusTree) Get(key []byte) ([]byte, bool) {
	if bpt.root == nil {
		return nil, false
	}
	targetLeaf := bpt.findLeafByKey(key)
	for i := 0; i < targetLeaf.keyNums; i++ {
		if bytes.Compare(key, targetLeaf.keys[i]) == 0 {
			return targetLeaf.pointers[i].convertToValue(), true
		}
	}
	return nil, false
}

// findLeafByKey finds the leaf which stores the given key
func (bpt *BPlusTree) findLeafByKey(key []byte) *node {
	current := bpt.root
	for !current.leaf {
		position := 0
		// find the target leaf node level by level
		for position < current.keyNums {
			if bytes.Compare(key, current.keys[position]) < 0 {
				break
			}
			position++
		}
		current = current.pointers[position].convertToNode()
	}
	return current
}

// Put insert a pair of kv into bpt, if the given key exists,
// the given value will override its value.
// Return old value and true if the given key exists, otherwise
// nil and false.
func (bpt *BPlusTree) Put(key, value []byte) ([]byte, bool) {
	if bpt.root == nil {
		bpt.init(key, value)
		return nil, false
	}
	targetLeaf := bpt.findLeafByKey(key)

	return bpt.putIntoLeaf(targetLeaf, key, value)
}

// putIntoLeaf puts a pair of kv into the given leaf node
// putIntoLeaf puts key and value into the node.
func (bpt *BPlusTree) putIntoLeaf(n *node, k, v []byte) ([]byte, bool) {
	insertPos := 0
	for insertPos < n.keyNums {
		cmp := bytes.Compare(k, n.keys[insertPos])
		if cmp == 0 {
			// found the exact match
			oldValue := n.pointers[insertPos].overrideValue(v)

			return oldValue, true
		} else if cmp < 0 {
			// found the insert position,
			// can break the loop
			break
		}
		insertPos++
	}

	// if we did not find the same key, we continue to insert
	if n.keyNums < len(n.keys) {
		// if the node is not full

		// shift the keys and pointers
		for j := n.keyNums; j > insertPos; j-- {
			n.keys[j] = n.keys[j-1]
			n.pointers[j] = n.pointers[j-1]
		}

		// insert
		n.keys[insertPos] = k
		n.pointers[insertPos] = &pointer{v}
		// and update key num
		n.keyNums++
	} else {
		// if the node is full
		parent := n.parent
		left, right := bpt.putIntoLeafAndSplit(n, insertPos, k, v)
		insertKey := right.keys[0]

		for left != nil && right != nil {
			if parent == nil {
				bpt.putIntoNewRoot(insertKey, left, right)
				break
			} else {
				if parent.keyNums < len(parent.keys) {
					// if the parent is not full
					bpt.putIntoParent(parent, insertKey, left, right)
					break
				} else {
					// if the parent is full
					// split parent, insert into the new parent and continue
					insertKey, left, right = bpt.putIntoParentAndSplit(parent, insertKey, left, right)
				}
			}

			parent = parent.parent
		}
	}
	bpt.size++
	return nil, false
}

// putIntoParent puts the node into the parent and update the left and the right
// pointers.
func (bpt *BPlusTree) putIntoParent(parent *node, k []byte, l, r *node) {
	insertPos := 0
	for insertPos < parent.keyNums {
		if bytes.Compare(k, parent.keys[insertPos]) < 0 {
			// found the insert position,
			// can break the loop
			break
		}

		insertPos++
	}

	// shift the keys and pointers
	parent.pointers[parent.keyNums+1] = parent.pointers[parent.keyNums]
	for j := parent.keyNums; j > insertPos; j-- {
		parent.keys[j] = parent.keys[j-1]
		parent.pointers[j] = parent.pointers[j-1]
	}

	// insert
	parent.keys[insertPos] = k
	parent.pointers[insertPos] = &pointer{l}
	parent.pointers[insertPos+1] = &pointer{r}
	// and update key num
	parent.keyNums++

	l.parent = parent
	r.parent = parent
}

// putIntoNewRoot creates new root, inserts left and right entries
// and updates the tree.
func (bpt *BPlusTree) putIntoNewRoot(key []byte, l, r *node) {
	// new root
	newRoot := &node{
		leaf:     false,
		keys:     make([][]byte, bpt.order-1),
		pointers: make([]*pointer, bpt.order),
		parent:   nil,
		keyNums:  1, // we are going to put just one key
	}

	newRoot.keys[0] = key
	newRoot.pointers[0] = &pointer{l}
	newRoot.pointers[1] = &pointer{r}

	l.parent = newRoot
	r.parent = newRoot

	bpt.root = newRoot
}

// putIntoParentAndSplit puts key in the parent, splits the node and returns the splitten
// nodes with all fixed pointers.
func (bpt *BPlusTree) putIntoParentAndSplit(parent *node, k []byte, l, r *node) ([]byte, *node, *node) {
	insertPos := 0
	for insertPos < parent.keyNums {
		if bytes.Compare(k, parent.keys[insertPos]) < 0 {
			// found the insert position,
			// can break the loop
			break
		}

		insertPos++
	}

	right := &node{
		leaf:     false,
		keys:     make([][]byte, bpt.order-1),
		keyNums:  0,
		pointers: make([]*pointer, bpt.order),
		parent:   nil,
	}

	middlePos := ceil(len(parent.keys), 2)
	copyFrom := middlePos
	if insertPos < middlePos {
		// since the elements will be shifted
		copyFrom -= 1
	}

	copy(right.keys, parent.keys[copyFrom:])
	copy(right.pointers, parent.pointers[copyFrom:])
	// copy the pointer to the next node
	right.keyNums = len(right.keys) - copyFrom

	// the given node becomes the left node
	left := parent
	left.keyNums = copyFrom
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= copyFrom; i-- {
		left.keys[i] = nil
		left.pointers[i+1] = nil
	}

	insertNode := left
	if insertPos >= middlePos {
		insertNode = right
		insertPos -= middlePos
	}

	// insert into the node
	insertNode.pointers[insertNode.keyNums+1] = insertNode.pointers[insertNode.keyNums]
	for j := insertNode.keyNums; j > insertPos; j-- {
		insertNode.keys[j] = insertNode.keys[j-1]
		insertNode.pointers[j] = insertNode.pointers[j-1]
	}

	insertNode.keys[insertPos] = k
	insertNode.pointers[insertPos] = &pointer{l}
	insertNode.pointers[insertPos+1] = &pointer{r}
	insertNode.keyNums++

	l.parent = insertNode
	r.parent = insertNode

	middleKey := right.keys[0]

	// clean up the right node
	for i := 1; i < right.keyNums; i++ {
		right.keys[i-1] = right.keys[i]
		right.pointers[i-1] = right.pointers[i]
	}
	right.pointers[right.keyNums-1] = right.pointers[right.keyNums]
	right.pointers[right.keyNums] = nil
	right.keys[right.keyNums-1] = nil
	right.keyNums--

	// update the pointers
	for _, p := range left.pointers {
		if p != nil {
			p.convertToNode().parent = left
		}
	}
	for _, p := range right.pointers {
		if p != nil {
			p.convertToNode().parent = right
		}
	}

	return middleKey, left, right
}

// putIntoLeafAndSplit puts the new key and splits the node into the left and right nodes
// and returns the left and the right nodes.
// The given node becomes left node.
// The tree is right-biased, so the first element in
// the right node is the "middle" key.
func (bpt *BPlusTree) putIntoLeafAndSplit(n *node, insertPos int, k, v []byte) (*node, *node) {
	right := &node{
		leaf:     true,
		keys:     make([][]byte, bpt.order-1),
		keyNums:  0,
		pointers: make([]*pointer, bpt.order),
		parent:   nil,
	}

	middlePos := ceil(len(n.keys), 2)
	copyFrom := middlePos
	if insertPos < middlePos {
		// since the elements will be shifted
		copyFrom -= 1
	}

	copy(right.keys, n.keys[copyFrom:])
	copy(right.pointers, n.pointers[copyFrom:len(n.pointers)-1])

	// copy the pointer to the next node
	if err := right.setLastPointer(n.pointerToNextLeafNode()); err != nil {
		panic(err)
	}
	right.keyNums = len(right.keys) - copyFrom

	// the given node becomes the left node
	left := n
	left.parent = nil
	left.keyNums = copyFrom
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= copyFrom; i-- {
		left.keys[i] = nil
		left.pointers[i] = nil
	}
	if err := left.setLastPointer(&pointer{right}); err != nil {
		panic(err)
	}

	insertNode := left
	if insertPos >= middlePos {
		insertNode = right
		// normalize insert position
		insertPos -= middlePos
	}

	// insert into the node
	insertNode.insertAt(insertPos, insertPos, k, &pointer{v})

	return left, right
}

// Delete deletes the key from the tree. Returns deleted value and true
// if the key exists, otherwise nil and false.
func (bpt *BPlusTree) Delete(key []byte) ([]byte, bool) {
	if bpt.root == nil {
		return nil, false
	}

	leaf := bpt.findLeafByKey(key)

	value, deleted := bpt.deleteAtLeafAndRebalance(leaf, key)
	if !deleted {
		return nil, false
	}

	bpt.size--

	return value, true
}

// deleteAtLeafAndRebalance deletes the key from the given node and rebalances it.
func (bpt *BPlusTree) deleteAtLeafAndRebalance(n *node, key []byte) ([]byte, bool) {
	keyPos := n.keyPosition(key)
	if keyPos == -1 {
		return nil, false
	}

	value := n.pointers[keyPos].convertToValue()
	n.deleteAt(keyPos, keyPos)

	if n.parent == nil {
		// deletion from the root
		if n.keyNums == 0 {
			// remove the root
			bpt.root = nil
		}

		return value, true
	}

	if n.keyNums < bpt.minKeyNum {
		bpt.rebalancedFromLeafNode(n)
	}

	bpt.removeFromIndex(key)

	return value, true
}

// removeFromIndex searches the key in the index (internal nodes and if finds it changes to
// the leftmost key in the right subtree.
func (bpt *BPlusTree) removeFromIndex(key []byte) {
	current := bpt.root
	for !current.leaf {
		// until the leaf is reached

		position := 0
		for position < current.keyNums {
			cmp := bytes.Compare(key, current.keys[position])
			if cmp < 0 {
				break
			} else if cmp > 0 {
				position += 1
			} else if cmp == 0 {
				// the key is found in the index
				// take the right sub-tree and find the leftmost key
				// and update the key
				current.keys[position] = findLeftmostKey(current.pointers[position+1].convertToNode())
			}
		}

		current = current.pointers[position].convertToNode()
	}
}

// findLeftmostKey returns the leftmost key for the node.
func findLeftmostKey(n *node) []byte {
	current := n
	for !current.leaf {
		current = current.pointers[0].convertToNode()
	}

	return current.keys[0]
}

// rebalancedFromLeafNode starts rebalancing the tree from the leaf node.
func (bpt *BPlusTree) rebalancedFromLeafNode(n *node) {
	parent := n.parent

	pointerPositionInParent := parent.getPointerPositionOfNode(n)
	keyPositionInParent := pointerPositionInParent - 1
	if keyPositionInParent < 0 {
		keyPositionInParent = 0
	}

	// trying to borrow for the leaf from any sibling

	// check left sibling
	leftSiblingPosition := pointerPositionInParent - 1
	var leftSibling *node
	if leftSiblingPosition >= 0 {
		// if left sibling exists
		leftSibling = parent.pointers[leftSiblingPosition].convertToNode()

		if leftSibling.keyNums > bpt.minKeyNum {
			// borrow from the left sibling
			n.insertAt(0, 0, leftSibling.keys[leftSibling.keyNums-1], leftSibling.pointers[leftSibling.keyNums-1])
			leftSibling.deleteAt(leftSibling.keyNums-1, leftSibling.keyNums-1)
			parent.keys[keyPositionInParent] = n.keys[0]
			return
		}
	}

	rightSiblingPosition := pointerPositionInParent + 1
	var rightSibling *node
	if rightSiblingPosition < parent.keyNums+1 {
		// if right sibling exists
		rightSibling = parent.pointers[rightSiblingPosition].convertToNode()

		if rightSibling.keyNums > bpt.minKeyNum {
			// borrow from the right sibling
			n.append(rightSibling.keys[0], rightSibling.pointers[0])
			rightSibling.deleteAt(0, 0)
			parent.keys[rightSiblingPosition-1] = rightSibling.keys[0]
			return
		}
	}

	// if we could borrow, we would borrow
	// so, we just take the first available sibling and merge with it
	// and the remove the navigator key and appropriate pointer

	// merge nodes and remove the "navigator" key and appropriate
	if leftSibling != nil {
		leftSibling.copyFromRight(n)
		parent.deleteAt(keyPositionInParent, pointerPositionInParent)
	} else if rightSibling != nil {
		n.copyFromRight(rightSibling)
		parent.deleteAt(keyPositionInParent, rightSiblingPosition)
	}

	bpt.rebalanceParentNode(parent)
}

// rebalanceInternalNode rebalances the tree from the internal node. It expects that
func (bpt *BPlusTree) rebalanceParentNode(n *node) {
	if n.parent == nil {
		if n.keyNums == 0 {
			bpt.root = n.pointers[0].convertToNode()
			bpt.root.parent = nil
		}

		return
	}

	if n.keyNums >= bpt.minKeyNum {
		// balanced
		return
	}

	parent := n.parent

	pointerPositionInParent := n.parent.getPointerPositionOfNode(n)
	keyPositionInParent := pointerPositionInParent - 1
	if keyPositionInParent < 0 {
		keyPositionInParent = 0
	}

	// trying to borrow for the internal node from any sibling

	// check left sibling
	leftSiblingPosition := pointerPositionInParent - 1
	var leftSibling *node
	if leftSiblingPosition >= 0 {
		// if left sibling exists
		leftSibling = parent.pointers[leftSiblingPosition].convertToNode()

		if leftSibling.keyNums > bpt.minKeyNum {
			splitKey := parent.keys[keyPositionInParent]

			// borrow from the left sibling
			leftSibling.pointers[leftSibling.keyNums].convertToNode().parent = n
			n.insertAt(0, 0, splitKey, leftSibling.pointers[leftSibling.keyNums])

			parent.keys[keyPositionInParent] = leftSibling.keys[leftSibling.keyNums-1]
			leftSibling.deleteAt(leftSibling.keyNums-1, leftSibling.keyNums)

			return
		}
	}

	rightSiblingPosition := pointerPositionInParent + 1
	var rightSibling *node
	if rightSiblingPosition < parent.keyNums+1 {
		// if right sibling exists
		rightSibling = parent.pointers[rightSiblingPosition].convertToNode()

		if rightSibling.keyNums > bpt.minKeyNum {
			splitKeyPosition := rightSiblingPosition - 1
			splitKey := parent.keys[splitKeyPosition]

			// borrow from the right sibling
			n.append(splitKey, rightSibling.pointers[0])

			parent.keys[splitKeyPosition] = rightSibling.keys[0]
			rightSibling.deleteAt(0, 0)
			return
		}
	}

	// if we could borrow, we would borrow
	// so, we just take the first available sibling and merge with it
	if leftSibling != nil {
		splitKey := parent.keys[keyPositionInParent]

		// incorporate the split key from parent for the merging
		leftSibling.keys[leftSibling.keyNums] = splitKey
		leftSibling.keyNums++

		leftSibling.copyFromRight(n)

		parent.deleteAt(keyPositionInParent, pointerPositionInParent)
	} else if rightSibling != nil {
		splitKey := parent.keys[keyPositionInParent]

		n.keys[n.keyNums] = splitKey
		n.keyNums++

		n.copyFromRight(rightSibling)
		parent.deleteAt(keyPositionInParent, rightSiblingPosition)
	}

	bpt.rebalanceParentNode(parent)
}

// ForEach traverses tree in ascending key order.
func (bpt *BPlusTree) ForEach(action func(key []byte, value []byte)) {
	for it := bpt.Iterator(); it.HasNext(); {
		key, value := it.Next()
		action(key, value)
	}
}

// Size returns the size of the tree.
func (bpt *BPlusTree) Size() int {
	return bpt.size
}
