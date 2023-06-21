package entity

import (
	"container/heap"
	"sync"
)

type Book struct {
	Order         []*Order
	Transaction   []*Transaction
	OrdersChanIn  chan *Order
	OrdersChanOut chan *Order
	Wg            *sync.WaitGroup
}

func NewBook(orderChanIn chan *Order, orderChanOut chan *Order, wg *sync.WaitGroup) *Book {
	return &Book{
		Order:         []*Order{},
		Transaction:   []*Transaction{},
		OrdersChanIn:  orderChanIn,
		OrdersChanOut: orderChanOut,
		Wg:            wg,
	}
}

func (b *Book) Trade() {
	//fila de ordens de compra
	buyOrders := NewOrderQueue()

	//fila de ordens de venda
	sellOrders := NewOrderQueue()

	heap.Init(buyOrders)
	heap.Init(sellOrders)

	//percorre todas as ordens de entrada
	for order := range b.OrdersChanIn {
		// verifica se a orden é de compra
		if order.OrderType == "BUY" {
			//adiciona a ordens na fila de compra
			buyOrders.Push(order)
			// verifica se existe ordem de venda na fila de ordens de venda e
			// se o preço da ordem de venda é menor ou igual ao preço da ordem de venda
			if sellOrders.Len() > 0 && sellOrders.Orders[0].Price <= order.Price {
				//se existir remove essa ordem da fila de ordens de venda
				sellOrder := sellOrders.Pop().(*Order)
				//verifica de existem cotas pendentes para venda.
				if sellOrder.PendingShares > 0 {
					//cria uma nova transação de venda de cotas
					transaction := NewTransaction(sellOrder, order, order.Shares, sellOrder.Price)
					// Adiciona uma nova transação ao Book para registrar a transação realizada
					b.AddTransaction(transaction, b.Wg)
					//adicionar as transações nas ordens de compra e de venda
					sellOrder.Transactions = append(order.Transactions, transaction)
					order.Transactions = append(order.Transactions, transaction)

					//adiona as ordens de compra e de venda para o canal para disponibilizar para o Kafka
					b.OrdersChanOut <- sellOrder
					b.OrdersChanOut <- order

					//verificar se ainda existem cotas pendentes, se tiver adiciona a ordens novamente na fila de ordens de venda
					if sellOrder.PendingShares > 0 {
						sellOrders.Push(sellOrders)
					}
				}
			}
		} else if order.OrderType == "SELL" {
			sellOrders.Push(order)
			if buyOrders.Len() > 0 && buyOrders.Orders[0].Price >= order.Price {
				buyOrder := buyOrders.Pop().(*Order)
				if buyOrder.PendingShares > 0 {
					transaction := NewTransaction(order, buyOrder, order.Shares, buyOrder.Price)
					b.AddTransaction(transaction, b.Wg)
					buyOrder.Transactions = append(buyOrder.Transactions, transaction)
					order.Transactions = append(order.Transactions, transaction)
					b.OrdersChanOut <- buyOrder
					b.OrdersChanOut <- order
					if buyOrder.PendingShares > 0 {
						buyOrders.Push(buyOrder)

					}
				}

			}

		}
	}
}

func (b *Book) AddTransaction(transaction *Transaction, wg *sync.WaitGroup) {
	//comando defer é utilizado para quando finalizar tudo que está abaixo executar essa linha.
	defer wg.Done()

	sellingShares := transaction.SellingOrder.PendingShares
	buyingShares := transaction.BuyingOrder.PendingShares

	minShares := sellingShares
	if buyingShares < minShares {
		minShares = buyingShares
	}

	transaction.SellingOrder.Investor.UpdateAssetPosition(transaction.SellingOrder.Asset.ID, -minShares)
	transaction.UpdateSellOrderPendingShares(-minShares)

	transaction.BuyingOrder.Investor.UpdateAssetPosition(transaction.BuyingOrder.Asset.ID, minShares)
	transaction.UpdateBuyOrderPendingShares(-minShares)

	transaction.CalculateTotal(transaction.Shares, transaction.BuyingOrder.Price)

	transaction.CloserBuyOrder()
	transaction.CloserSellOrder()

	b.Transaction = append(b.Transaction, transaction)

}
