package memtable

type AVLTree struct {
	root *avlNode
}

func NewAvlTree() *AVLTree {
	return &AVLTree{
		root: nil,
	}
}

func (t *AVLTree) Insert(key string, data []byte) {

}

func (t *AVLTree) Delete(key string, data []byte) {

}

func (t *AVLTree) Find(key string) []byte {
	return nil
}

type avlNode struct {
	key  string
	data []byte

	height int
	left   *avlNode
	right  *avlNode
}
