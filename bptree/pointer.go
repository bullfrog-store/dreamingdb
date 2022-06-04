package bptree

// pointer is either a node or an instance of value
type pointer struct {
	data interface{}
}

// convertToNode convert pointer.data to node
func (p *pointer) convertToNode() *node {
	return p.data.(*node)
}

// convertToValue convert pointer.data to value
func (p *pointer) convertToValue() []byte {
	return p.data.([]byte)
}

// overrideValue override the old value and return it
func (p *pointer) overrideValue(value []byte) []byte {
	oldValue := p.convertToValue()
	p.data = value
	return oldValue
}
