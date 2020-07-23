package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const TopicName = "test"

func main() {
	if len(os.Args) > 1 &&os.Args[1] == "-r" {
		readMessage()
	} else {
		emitMessages()
	}
}

func readMessage() {
	config:= &kafka.ConfigMap{"bootstrap.servers": "localhost",
		"isolation.level": "read_committed",
		"enable.idempotence": true,
		"enable.auto.commit": false,
		"enable.auto.offset.store": false,
		"group.id": "testTransactionalGroup",
		"broker.address.family": "v4",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	topic := "test"
	err = consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: 0, Offset: kafka.OffsetBeginning}})
	if err != nil {
		panic(err)
	}
	msg, err := consumer.ReadMessage(-1)
	if err != nil {
		panic(err)
	}
	fmt.Println(msg)
}

func emitMessages() {
	config:= &kafka.ConfigMap{"bootstrap.servers": "localhost",
		"isolation.level": "read_committed",
		"enable.idempotence": true,
		"enable.auto.commit": false,
		"enable.auto.offset.store": false,
		"transactional.id": "testTransactionId",
		"broker.address.family": "v4",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic("Failed to make producer")
	}
	defer producer.Close()

	topic:= TopicName

	err = createTopicIfNecessary(config, topic)
	if err != nil {
		panic(err)
	}

	ctx, cxl := context.WithTimeout(context.Background(), 3000)
	defer cxl()
	err = producer.InitTransactions(ctx)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			evt, ok := <-producer.Events()
			if !ok{
				return
			}
			msg, ok := evt.(*kafka.Message)
			if !ok {
				fmt.Println(ok)

			}
			if msg.TopicPartition.Error != nil {
				fmt.Println(msg.TopicPartition)
			}
		}
	}()

	err = producer.BeginTransaction()
	if err != nil {
		panic(err)
	}


	i := int64(0)
	for i< 40 *1000 *1000 {
		i++
		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(buf, i)
		msg := &kafka.Message{Value: buf, Key: buf, TopicPartition: kafka.TopicPartition{Partition: 0, Topic: &topic, Offset: kafka.OffsetBeginning}}
		err = producer.Produce(msg, nil)
		if err != nil {
			panic(err)
		}
		if i%1000000 == 0 {
			fmt.Println("progress update emitted", i)
		}
	}
	fmt.Println("Emitted 40 million events but did not commit them")
}

func createTopicIfNecessary(config *kafka.ConfigMap, topic string) error {
	admin, err := kafka.NewAdminClient(config)
	if err != nil {
		return err
	}
	defer admin.Close()
	md, err := admin.GetMetadata(nil, true, 5000)
	if err != nil {
		return err
	}

	if _, ok := md.Topics[topic]; !ok {
		topics, err := admin.CreateTopics(context.Background(), []kafka.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
				Config:            make(map[string]string),
			},
		})
		if err != nil {
			return  err
		}
		if topics[0].Error.Code() != kafka.ErrNoError {
			return topics[0].Error
		}
	}
	return nil
}