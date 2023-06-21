package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/carloskenny/imersao13fullcycle/internal/infra/kafka"
	"github.com/carloskenny/imersao13fullcycle/internal/market/dto"
	"github.com/carloskenny/imersao13fullcycle/internal/market/entity"
	"github.com/carloskenny/imersao13fullcycle/internal/market/transformer"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	//canal de entrada para receber as ordens de entrada
	ordersIn := make(chan *entity.Order)

	//canal de saída para enviar as ordens de saída
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "myGroup",
		"auto.offset.reset": "earlist",
	}

	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	//quando executar a próxima linha o programa ficara travado, pois ficará rodando o loop infinito.
	//para isso é criada uma nova thread colocando "go" na frente. (go routing)
	go kafka.Consume(kafkaMsgChan) //T2

	//receber do canal do kafka, joga no input, processa joga no output e depois publica no kafka
	book := entity.NewBook(ordersIn, ordersOut, wg)

	go book.Trade() //T3

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(tradeInput)
			ordersIn <- order
		}

	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "   ")
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outputJson, []byte("orders"), "output")
	}

}