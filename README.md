# kafka-tool

## Installation
### Install from source
```bash
go get github.com/rickb777/kafka-tool
```

## Usage
```bash
Usage of ./kafka-tool:
  -begin
        consume from begin
  -cmd command
        - consumer (default): runs a consumer that prints each message
        - copy: copy messages from host to remote
        - offset
        - resetOffset
  -filter string
        the filter on keys (optional)
  -group string
        the consumer group (optional)
  -host string
        brokerlist/topic
  -partition int
        required partition number
  -print
        printflag
  -remote string
        brokerlist/topic, on using command "copy"
```

### Example
```bash
## consumer
kafka-tool -host 127.0.0.1:9092/mytopic

## consumer with filter
kafka-tool -host 127.0.0.1:9092/mytopic -filter aaaaaaaa

## consumer from begining
kafka-tool -host 127.0.0.1:9092/mytopic -begin

## copy topic to another topic
./kafka-tool -cmd copy -host 127.0.0.1:9092/mytopic1  -remote 127.0.0.1:9092/mytopic2

## dump the newest offset
./kafka-tool -cmd offset -host 127.0.0.1:9092/mytopic1

```
