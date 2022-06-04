package bptree

func ceil(a, b int) int {
	if a%b == 0 {
		return a / b
	}
	return a/b + 1
}
