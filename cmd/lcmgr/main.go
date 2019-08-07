package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/vanstee/lcmgr"
	"golang.org/x/sync/errgroup"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	service           = kingpin.Flag("service", "Name of systemd unit to monitor").Required().Short('s').String()
	spotInterval      = kingpin.Flag("spot-interval", "Interval to wait between checking for a spot notice").Default("30s").Short('i').Duration()
	heartbeatInterval = kingpin.Flag("heartbeat-interval", "Interval to wait between sending heartbeats").Default("1m").Short('t').Duration()
)

func main() {
	kingpin.Parse()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	notices := make(chan lcmgr.Notice)

	client := lcmgr.NewAWSClient()

	queues, err := client.GetLifecycleNoticeQueues(context.Background())
	if err != nil {
		log.Fatalf("failed to get lifecycle hooks: %v", err)
	}

	listeners := make([]lcmgr.Listener, 0, len(queues)+1)
	listeners = append(listeners, lcmgr.NewSpotListener(notices, *spotInterval, client))
	for _, queue := range queues {
		listeners = append(listeners, lcmgr.NewLifecycleListener(notices, queue, client))
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)
	for _, listener := range listeners {
		listener := listener
		group.Go(func() error {
			return listener.Listen(ctx)
		})
	}

	handler := lcmgr.NewServiceHandler(*service, *heartbeatInterval, client)

	for ctx.Err() != nil {
		var notice lcmgr.Notice
		select {
		case notice = <-notices:
			if err := handler.Handle(ctx, notice); err != nil {
				log.Printf("failed to handle %v notice: %v", notice.Type(), err)
			}
		case <-signals:
			log.Printf("received signal, shutting down")
			cancel()
		}
	}

	if err := group.Wait(); err != nil {
		log.Fatalf("failed while listening: %v", err)
	}
}
