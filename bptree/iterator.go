package bptree

// Iterator returns a stateful Iterator for traversing the tree
// in ascending key order.
type Iterator struct {
	next *node
	i    int
}

// Iterator returns a stateful iterator that traverses the tree
// in ascending key order.
func (bpt *BPlusTree) Iterator() *Iterator {
	return &Iterator{bpt.mostLeftNode, 0}
}

// HasNext returns true if there is a next element.
func (it *Iterator) HasNext() bool {
	return it.next != nil && it.i < it.next.keyNums
}

// Next returns a key and a value at the current position of the iteration
// and advances the iterator.
func (it *Iterator) Next() ([]byte, []byte) {
	if !it.HasNext() {
		// to sleep well
		panic("there is no next node")
	}

	key, value := it.next.keys[it.i], it.next.pointers[it.i].convertToValue()

	it.i++
	if it.i == it.next.keyNums {
		lastPointer := it.next.pointerToNextLeafNode()
		if lastPointer != nil {
			it.next = lastPointer.convertToNode()
		} else {
			it.next = nil
		}

		it.i = 0
	}

	return key, value
}
