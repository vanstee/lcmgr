package lcmgr

import (
	"context"
	"log"
	"time"
)

type Listener interface {
	Type() string
	Listen(context.Context) error
}

type SpotListener struct {
	Notices  chan Notice
	Interval time.Duration
	Client   AWSClient
}

type LifecycleListener struct {
	Notices chan Notice
	Queue   *Queue
	Client  AWSClient
}

type LaunchListener struct {
	*LifecycleListener
}

type TerminationListener struct {
	*LifecycleListener
}

type ErrorListener struct{}

func NewSpotListener(notices chan Notice, interval time.Duration, client AWSClient) Listener {
	return &SpotListener{
		Notices:  notices,
		Interval: interval,
		Client:   client,
	}
}

func NewLifecycleListener(notices chan Notice, queue *Queue, client AWSClient) Listener {
	listener := &LifecycleListener{
		Notices: notices,
		Queue:   queue,
		Client:  client,
	}

	switch queue.Action {
	case LaunchLifecycleAction:
		return &LaunchListener{listener}
	case TerminationLifecycleAction:
		return &TerminationListener{listener}
	default:
		return &TerminationListener{listener}
	}
}

func (listener *SpotListener) Listen(ctx context.Context) error {
	ticker := time.NewTicker(listener.Interval)
	defer ticker.Stop()

	var notice Notice
	var notices chan Notice
	for {
		if notice != nil {
			notices = listener.Notices
		}

		select {
		case notices <- notice:
			notices = nil
		case <-ticker.C:
			var err error
			notice, err = listener.Client.GetSpotNotice()
			if err != nil {
				log.Printf("failed to get spot notice: %v", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (listener *SpotListener) Type() string {
	return "spot"
}

func (listener *LifecycleListener) Listen(ctx context.Context) error {
	var notice Notice
	var notices chan Notice
	for {
		if notice != nil {
			notices = listener.Notices
		}

		select {
		case notices <- notice:
			notices = nil
		case <-ctx.Done():
			return nil
		default:
			var err error
			notice, err = listener.Client.GetLifecycleNotice(ctx, listener.Queue)
			if err != nil {
				log.Printf("failed to get lifecycle notice from queue %v: %v", listener.Queue.Name, err)
			}
		}
	}
}

func (listener *LaunchListener) Type() string {
	return "launch"
}

func (listener *TerminationListener) Type() string {
	return "termination"
}
