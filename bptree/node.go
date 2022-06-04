package bptree

import (
	"bytes"
	"errors"
)

type node struct {
	// true for leaf node and false for internal node
	leaf   bool
	parent *node

	// all the keys stored in this node
	keys [][]byte
	// the real key numbers
	keyNums int

	// abstract pointer pointed to something.
	// for internal node, pointer pointed to a node,
	// for leaf node, pointer pointed to a value.
	// The size of pointers equals to the size of key + 1,
	// in leaf node, the last pointer pointed to the next leaf node.
	pointers []*pointer
}

// newNode returns a new node
func newNode(leaf bool, parent *node, order int) *node {
	return &node{
		leaf:     leaf,
		parent:   parent,
		keys:     make([][]byte, order-1),
		keyNums:  0,
		pointers: make([]*pointer, order-1),
	}
}

// append appends the key and pointer to node
func (n *node) append(key []byte, p *pointer) {
	keyPosition, pointerPosition := n.keyNums, n.keyNums
	if !n.leaf && n.pointers[pointerPosition] != nil {
		pointerPosition++
	}
	n.keys[keyPosition] = key
	n.pointers[pointerPosition] = p
	n.keyNums++
	if !n.leaf {
		p.convertToNode().parent = n
	}
}

// insertAt inserts the given key and pointer to the specified position
func (n *node) insertAt(keyPosition, pointerPosition int, key []byte, p *pointer) {
	// shift all the keys after keyPosition
	for i := n.keyNums; i > keyPosition; i-- {
		n.keys[i] = n.keys[i-1]
	}
	pointerNums := n.keyNums
	if !n.leaf {
		pointerNums++
		p.convertToNode().parent = n
	}
	// shift all the pointers after pointerPosition
	for i := pointerNums; i > pointerPosition; i-- {
		n.pointers[i] = n.pointers[i-1]
	}
	n.keyNums++
	n.keys[keyPosition] = key
	n.pointers[pointerPosition] = p
}

func (n *node) deleteAt(keyPosition, pointerPosition int) {
	// shift all the keys before keyPosition
	for i := keyPosition; i < n.keyNums-1; i++ {
		n.keys[i] = n.keys[i+1]
	}
	n.keys[n.keyNums-1] = nil
	pointerNums := n.keyNums
	if !n.leaf {
		pointerNums++
	}
	// shift all the pointers before pointPosition
	for i := pointerPosition; i < n.keyNums-1; i++ {
		n.pointers[i] = n.pointers[i+1]
	}
	n.pointers[pointerNums-1] = nil

	n.keyNums--
}

// keyPosition returns key position of the given key
// if it exists, otherwise -1
func (n *node) keyPosition(key []byte) int {
	for keyPosition := 0; keyPosition < n.keyNums; keyPosition++ {
		if bytes.Compare(key, n.keys[keyPosition]) == 0 {
			return keyPosition
		}
	}
	return -1
}

// getPointerPositionOfNode returns the pointer position of
// the given node, but -1 if not found.
func (n *node) getPointerPositionOfNode(target *node) int {
	for position, pointer := range n.pointers {
		if pointer == nil {
			break
		}
		if pointer.convertToNode() == target {
			return position
		}
	}
	return -1
}

// setLastPointer sets the last pointer,
// **Only works for leaf node**
func (n *node) setLastPointer(p *pointer) error {
	if !n.leaf {
		return errors.New("only works for leaf node")
	}
	n.pointers[len(n.pointers)-1] = p
	return nil
}

// nextLeafNode returns the next leaf node,
// it only works for leaf node.
func (n *node) nextLeafNode() (*node, error) {
	if !n.leaf {
		return nil, errors.New("only works for leaf node")
	}
	return n.pointers[len(n.pointers)-1].convertToNode(), nil
}

// pointerToNextLeafNode returns the pointer to next leaf node, it actually
// returns the last pointer, so it only works for leaf node.
func (n *node) pointerToNextLeafNode() *pointer {
	return n.pointers[len(n.pointers)-1]
}

// copyFromRight copies the keys and the pointer from the given node.
func (n *node) copyFromRight(from *node) {
	for i := 0; i < from.keyNums; i++ {
		n.append(from.keys[i], from.pointers[i])
	}

	if n.leaf {
		n.setLastPointer(from.pointerToNextLeafNode())
	} else {
		n.pointers[n.keyNums] = from.pointers[from.keyNums]
		n.pointers[n.keyNums].convertToNode().parent = n
	}
}
