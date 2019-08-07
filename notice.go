package lcmgr

import "time"

type Notice interface {
	Type() string
}

type SpotNotice struct {
	TerminationTime time.Time
}

type LifecycleNotice struct {
	LifecycleHookName    string
	LifecycleActionToken string
}

type LaunchNotice struct {
	*LifecycleNotice
}

type TerminationNotice struct {
	*LifecycleNotice
}

func NewSpotNotice(terminationTime time.Time) *SpotNotice {
	return &SpotNotice{
		TerminationTime: terminationTime,
	}
}

func NewLaunchNotice(hook, token string) *LaunchNotice {
	return &LaunchNotice{
		&LifecycleNotice{
			LifecycleHookName:    hook,
			LifecycleActionToken: token,
		},
	}
}

func NewTerminationNotice(hook, token string) *TerminationNotice {
	return &TerminationNotice{
		&LifecycleNotice{
			LifecycleHookName:    hook,
			LifecycleActionToken: token,
		},
	}
}

func (notice *SpotNotice) Type() string {
	return "spot"
}

func (notice *LaunchNotice) Type() string {
	return "launch"
}

func (notice *TerminationNotice) Type() string {
	return "termination"
}
