package entity

type OrderQueue struct {
	Orders []*Order
}

// Less -> Valor 1 (i) é menor que valor 2 (j)
func (oq *OrderQueue) Less(i, j int) bool {
	return oq.Orders[i].Price < oq.Orders[j].Price
}

// Swap -> Inversor de valores i <-> j
func (oq *OrderQueue) Swap(i, j int) {
	oq.Orders[i], oq.Orders[j] = oq.Orders[j], oq.Orders[i]
}

// Len -> Saber o tamanho dos dados
func (oq *OrderQueue) Len() int {
	return len(oq.Orders)
}

// Push -> Adicionar dados (Append)
// Inteface vazia é igual any
func (oq *OrderQueue) Push(x interface{}) {
	oq.Orders = append(oq.Orders, x.(*Order))
}

// Pop -> Remover uma posição da fila
func (oq *OrderQueue) Pop() interface{} {
	old := oq.Orders
	n := len(old)
	item := old[n-1]
	oq.Orders = old[0 : n-1]
	return item
}

func NewOrderQueue() *OrderQueue {
	return &OrderQueue{}
}
