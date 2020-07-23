# MinimumReproduceableLibrdkafkaTransactionBug
This repo recreates a librdkafka bug relating to failed transactions
Steps to reproduce

1. Have kafka running on your localhost (or change connection settings in main.go)
2. `go run main.go`. This will create a topic called test and send 40 million messages to it and then exit without commiting them.
3. `go run main.go -r` This will eventually output an error like `% ERROR: Topic test [0] error: Message at offset 37942000 might be too large to fetch, try increasing receive.message.max.bytes`
4. `kafkacat -C -b 127.0.0.1:9092 -t test -o 0`. This will eventually output the same error
