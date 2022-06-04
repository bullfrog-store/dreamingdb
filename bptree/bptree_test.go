package bptree

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

var testDatas = []struct {
	key   []byte
	value []byte
}{
	{[]byte("11"), []byte("11")},
	{[]byte("18"), []byte("18")},
	{[]byte("7"), []byte("7")},
	{[]byte("15"), []byte("15")},
	{[]byte("0"), []byte("0")},
	{[]byte("16"), []byte("16")},
	{[]byte("14"), []byte("14")},
	{[]byte("33"), []byte("33")},
	{[]byte("25"), []byte("25")},
	{[]byte("42"), []byte("42")},
	{[]byte("60"), []byte("60")},
	{[]byte("2"), []byte("2")},
	{[]byte("1"), []byte("1")},
	{[]byte("74"), []byte("74")},
}

func TestPutAndGet(t *testing.T) {
	for order := 3; order <= 7; order++ {
		bpt, _ := NewBPlusTree(SetOrder(order))

		// put some pairs of kv
		for _, testData := range testDatas {
			oldValue, existed := bpt.Put(testData.key, testData.value)
			assert.False(t, existed)
			assert.Nil(t, oldValue)
		}

		// get value by key
		for _, testData := range testDatas {
			value, ok := bpt.Get(testData.key)
			assert.True(t, ok)
			assert.Equal(t, testData.value, value)
		}
	}
}

func TestSize(t *testing.T) {
	bpt, _ := NewBPlusTree()

	expected := 0
	for _, testData := range testDatas {
		assert.Equal(t, expected, bpt.size)
		bpt.Put(testData.key, testData.value)
		expected++
	}
}

func TestPutNilKey(t *testing.T) {
	bpt, _ := NewBPlusTree()

	bpt.Put(nil, []byte{1})

	value, ok := bpt.Get(nil)
	assert.False(t, ok)
	assert.Nil(t, value)
}

func TestPutOverrides(t *testing.T) {
	bpt, _ := NewBPlusTree()

	oldValue, existed := bpt.Put([]byte("1"), []byte("1"))
	assert.False(t, existed)
	assert.Nil(t, oldValue)

	oldValue, existed = bpt.Put([]byte("1"), []byte("2"))
	assert.True(t, existed)
	assert.Equal(t, "1", string(oldValue))

	value, ok := bpt.Get([]byte("1"))
	assert.True(t, ok)
	assert.Equal(t, "2", string(value))
}

func TestGetForNonExistentValue(t *testing.T) {
	bpt, _ := NewBPlusTree()

	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	value, ok := bpt.Get([]byte("nonExistentKey"))
	assert.False(t, ok)
	assert.Nil(t, value)
}

func TestForEach(t *testing.T) {
	bpt, _ := NewBPlusTree()
	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	actual := make([][]byte, 0)
	bpt.ForEach(func(key []byte, value []byte) {
		actual = append(actual, key)
	})

	isSorted := sort.SliceIsSorted(actual, func(i, j int) bool {
		return string(actual[i]) < string(actual[j])
	})
	assert.True(t, isSorted)

	expected := make([][]byte, 0)
	for _, testData := range testDatas {
		expected = append(expected, testData.key)
	}
	sort.Slice(expected, func(i, j int) bool {
		return string(expected[i]) < string(expected[j])
	})
	assert.True(t, reflect.DeepEqual(expected, actual))
}

func TestKeySetOrder(t *testing.T) {
	bpt, _ := NewBPlusTree()
	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	keys := make([][]byte, len(testDatas))
	bpt.ForEach(func(key, value []byte) {
		keys = append(keys, key)
	})
	assert.False(t, len(keys) == 0)

	isSorted := sort.SliceIsSorted(keys, func(i, j int) bool {
		return string(keys[i]) < string(keys[j])
	})
	assert.True(t, isSorted)
}

func TestPutAndGetRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 10000
	keys := r.Perm(size)

	for order := 3; order <= 7; order++ {
		bpt, _ := NewBPlusTree(SetOrder(order))

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			oldValue, existed := bpt.Put(key, value)
			assert.False(t, existed)
			assert.Nil(t, oldValue)
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok := bpt.Get(key)
			assert.True(t, ok)

			actualValue := binary.LittleEndian.Uint32(v)
			assert.Equal(t, expectedValue, actualValue)
		}
	}
}

func TestPutAndDeleteRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 10000
	keys := r.Perm(size)

	for order := 3; order <= 7; order++ {
		bpt, _ := NewBPlusTree(SetOrder(order))

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			oldValue, existed := bpt.Put(key, value)
			assert.False(t, existed)
			assert.Nil(t, oldValue)
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok := bpt.Delete(key)
			assert.True(t, ok)

			actualValue := binary.LittleEndian.Uint32(v)
			assert.Equal(t, expectedValue, actualValue)
		}
	}
}

func TestDeleteNonExistentElement(t *testing.T) {
	bpt, _ := NewBPlusTree(SetOrder(3))

	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	value, deleted := bpt.Delete([]byte("nonExistentKey"))
	assert.False(t, deleted)
	assert.Nil(t, value)
}

func TestDeleteMergingThreeTimes(t *testing.T) {
	bpt, _ := NewBPlusTree(SetOrder(3))

	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	for _, testData := range testDatas {
		value, deleted := bpt.Delete(testData.key)
		assert.True(t, deleted)
		assert.Equal(t, testData.value, value)
	}
}

func TestDelete(t *testing.T) {
	for order := 3; order <= 7; order++ {
		bpt, _ := NewBPlusTree(SetOrder(order))
		for _, testData := range testDatas {
			bpt.Put(testData.key, testData.value)
		}

		expectedSize := len(testDatas)
		for _, testData := range testDatas {
			value, deleted := bpt.Delete(testData.key)
			assert.True(t, deleted)
			assert.Equal(t, testData.value, value)

			expectedSize--
			assert.Equal(t, expectedSize, bpt.Size())
		}
	}
}

func TestForEachAfterDeletion(t *testing.T) {
	bpt, _ := NewBPlusTree(SetOrder(3))

	keys := make([][]byte, 0)
	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
		keys = append(keys, testData.key)
	}

	for i, key := range keys {
		value, deleted := bpt.Delete(key)
		assert.True(t, deleted)
		assert.NotNil(t, value)

		actual := make([][]byte, 0)
		bpt.ForEach(func(key []byte, value []byte) {
			actual = append(actual, key)
		})

		isSorted := sort.SliceIsSorted(actual, func(i, j int) bool {
			return string(actual[i]) < string(actual[j])
		})
		assert.True(t, isSorted)

		expected := make([][]byte, 0)
		for j, k := range keys {
			if j > i {
				expected = append(expected, k)
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			return string(expected[i]) < string(expected[j])
		})
		assert.True(t, reflect.DeepEqual(expected, actual))
	}
}

func TestNonExistentPointerPositionOf(t *testing.T) {
	bpt, _ := NewBPlusTree(SetOrder(3))

	for _, testData := range testDatas {
		bpt.Put(testData.key, testData.value)
	}

	actual := bpt.root.getPointerPositionOfNode(bpt.root)
	assert.Equal(t, -1, actual)
}
