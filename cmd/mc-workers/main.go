package main

import (
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"
)

func main() {
	cfg := rpc.NewConsumerConfig().WithName("test")
	rpc.Consumer(cfg, func(ch rpc.DeliveryChannel) {
		forever := make(chan bool)

		go func() {
			for d := range ch {
				log.Infof(" [x] %s", d.Body)
			}
		}()

		log.Infof(" [*] Waiting for logs. To exit press CTRL+C")
		<-forever
	})
}
