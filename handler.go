package lcmgr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coreos/go-systemd/dbus"
)

type HandlerFunc func(context.Context, Notice) error

type Handler interface {
	Handle(context.Context, Notice) error
}

type ServiceHandler struct {
	Service           string
	HeartbeatInterval time.Duration
	Client            AWSClient
}

func NewServiceHandler(service string, heartbeatInterval time.Duration, client AWSClient) Handler {
	return &ServiceHandler{
		Service:           service,
		HeartbeatInterval: heartbeatInterval,
		Client:            client,
	}
}

func (handler *ServiceHandler) Handle(ctx context.Context, notice Notice) error {
	switch notice.(type) {
	case *SpotNotice:
		return handler.WaitForServiceStop(ctx, notice)
	case *LaunchNotice:
		return handler.ForLifecycleAction(ctx, notice, handler.WaitForServiceStart)
	case *TerminationNotice:
		return handler.ForLifecycleAction(ctx, notice, handler.WaitForServiceStop)
	default:
		return errors.New("failed to handle unexpected notice type")
	}
}

func (handler *ServiceHandler) WaitForServiceStart(ctx context.Context, notice Notice) error {
	conn, err := dbus.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	units, err := conn.ListUnitsByNames([]string{handler.Service})
	if err != nil {
		return err
	}
	if len(units) != 1 {
		return fmt.Errorf("failed to list status of systemd unit %s: %v", handler.Service, err)
	}

	results := make(chan string)
	n, err := conn.StartUnit(handler.Service, "fail", results)
	if err != nil {
		return err
	} else if n == 0 {
		fmt.Errorf("failed to start systemd unit %s due to unknown error", handler.Service)
	}

	result := <-results
	if result != "done" {
		fmt.Errorf("failed to start systemd unit %s, job returned %v result", handler.Service, result)
	}

	return nil
}

func (handler *ServiceHandler) WaitForServiceStop(ctx context.Context, notice Notice) error {
	conn, err := dbus.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	units, err := conn.ListUnitsByNames([]string{handler.Service})
	if err != nil {
		return err
	}
	if len(units) != 1 {
		return fmt.Errorf("failed to list status of systemd unit %s: %v", handler.Service, err)
	}

	results := make(chan string)
	n, err := conn.StopUnit(handler.Service, "fail", results)
	if err != nil {
		return err
	} else if n == 0 {
		fmt.Errorf("failed to start systemd unit %s due to unknown error", handler.Service)
	}

	result := <-results
	if result != "done" {
		fmt.Errorf("failed to start systemd unit %s, job returned %v result", handler.Service, result)
	}

	return nil
}

func (handler *ServiceHandler) ForLifecycleAction(ctx context.Context, notice Notice, f HandlerFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(handler.HeartbeatInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				handler.Client.SendHeartbeat(ctx, notice)
			case <-ctx.Done():
				return
			}
		}
	}()

	err := f(ctx, notice)
	if err != nil {
		log.Printf("failed to run %s handler: %v", notice.Type(), err)
	}

	if err := handler.Client.CompleteLifecycleAction(ctx, notice); err != nil {
		log.Printf("failed to complete %s lifecycle action: %v", notice.Type(), err)
	}

	cancel() // Stop sending heartbeats

	return err
}
