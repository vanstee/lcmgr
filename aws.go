package lcmgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	LaunchLifecycleAction      = "autoscaling:EC2_INSTANCE_LAUNCHING"
	TerminationLifecycleAction = "autoscaling:EC2_INSTANCE_TERMINATING"
)

type AWSClient interface {
	GetInstanceID() (string, error)
	GetAutoScalingGroupName(context.Context) (string, error)
	GetLifecycleNoticeQueues(context.Context) ([]*Queue, error)
	GetSpotNotice() (Notice, error)
	GetLifecycleNotice(context.Context, *Queue) (Notice, error)
	SendHeartbeat(context.Context, Notice) error
	CompleteLifecycleAction(context.Context, Notice) error
}

type awsClient struct {
	Session     *session.Session
	AutoScaling *autoscaling.AutoScaling
	EC2Metadata *ec2metadata.EC2Metadata
	SQS         *sqs.SQS

	AutoScalingGroupName string
	InstanceID           string
}

type Queue struct {
	Action string
	Name   string
	URL    string
}

type Message struct {
	EC2InstanceID        string `json:"EC2InstanceID"`
	LifecycleHookName    string `json:"LifecycleHookName"`
	LifecycleActionToken string `json:"LifecycleActionToken"`
	LifecycleTransition  string `json:"LifecycleTransition"`
}

func NewAWSClient() AWSClient {
	sess := session.Must(session.NewSession())

	return &awsClient{
		Session:     sess,
		AutoScaling: autoscaling.New(sess),
		EC2Metadata: ec2metadata.New(sess),
		SQS:         sqs.New(sess),
	}
}

func (client *awsClient) GetInstanceID() (string, error) {
	if client.InstanceID != "" {
		return client.InstanceID, nil
	}

	if !client.EC2Metadata.Available() {
		return "", errors.New("unable to access ec2 metadata api")
	}

	instanceID, err := client.EC2Metadata.GetMetadata("instance-id")
	if err != nil {
		return "", err
	}

	client.InstanceID = instanceID
	return instanceID, nil
}

func (client *awsClient) GetAutoScalingGroupName(ctx context.Context) (string, error) {
	if client.AutoScalingGroupName != "" {
		return client.AutoScalingGroupName, nil
	}

	instanceID, err := client.EC2Metadata.GetMetadata("instance-id")
	if err != nil {
		return "", err
	}

	input := &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceID),
		},
	}
	output, err := client.AutoScaling.DescribeAutoScalingInstancesWithContext(ctx, input)
	if err != nil {
		return "", err
	}
	if len(output.AutoScalingInstances) != 1 {
		return "", errors.New("instance is not controlled by an auto scaling group")
	}

	autoScalingGroupName := *output.AutoScalingInstances[0].AutoScalingGroupName
	client.AutoScalingGroupName = autoScalingGroupName
	return autoScalingGroupName, nil
}

// TODO: Include heartbeat in queue
func (client *awsClient) GetLifecycleNoticeQueues(ctx context.Context) ([]*Queue, error) {
	autoScalingGroupName, err := client.GetAutoScalingGroupName(ctx)
	if err != nil {
		return nil, err
	}

	input := &autoscaling.DescribeLifecycleHooksInput{
		AutoScalingGroupName: aws.String(autoScalingGroupName),
	}
	output, err := client.AutoScaling.DescribeLifecycleHooksWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	var queues map[string]*Queue
	for _, hook := range output.LifecycleHooks {
		if _, ok := queues[*hook.NotificationTargetARN]; ok {
			continue
		}

		parsed, err := arn.Parse(*hook.NotificationTargetARN)
		if err != nil {
			return nil, err
		}
		if parsed.Service != "sqs" {
			continue
		}

		input := &sqs.GetQueueUrlInput{
			QueueName:              aws.String(parsed.Resource),
			QueueOwnerAWSAccountId: aws.String(parsed.AccountID),
		}
		output, err := client.SQS.GetQueueUrlWithContext(ctx, input)
		if err != nil {
			return nil, err
		}

		queues[*hook.NotificationTargetARN] = &Queue{
			Action: *hook.LifecycleTransition,
			Name:   parsed.Resource,
			URL:    *output.QueueUrl,
		}
	}

	unique := make([]*Queue, 0, len(queues))
	for _, queue := range queues {
		unique = append(unique, queue)
	}

	return unique, nil
}

func (client *awsClient) GetSpotNotice() (Notice, error) {
	output, err := client.EC2Metadata.GetMetadata("spot/termination-time")
	if err != nil {
		if e, ok := err.(awserr.Error); ok && strings.Contains(e.OrigErr().Error(), "404") {
			return nil, nil
		}
		return nil, err
	}

	terminationTime, err := time.Parse(time.RFC3339, output)
	if err != nil {
		return nil, err
	}

	return NewSpotNotice(terminationTime), nil
}

func (client *awsClient) GetLifecycleNotice(ctx context.Context, queue *Queue) (Notice, error) {
	instanceID, err := client.GetInstanceID()
	if err != nil {
		return nil, err
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queue.URL),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
		VisibilityTimeout:   aws.Int64(0),
	}
	output, err := client.SQS.ReceiveMessageWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	for _, message := range output.Messages {
		var m Message

		if err := json.Unmarshal([]byte(*message.Body), &m); err != nil {
			continue
		}
		if m.EC2InstanceID != instanceID {
			continue
		}

		input := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queue.URL),
			ReceiptHandle: message.ReceiptHandle,
		}
		if _, err := client.SQS.DeleteMessageWithContext(ctx, input); err != nil {
			return nil, err
		}

		var notice Notice
		switch m.LifecycleTransition {
		case LaunchLifecycleAction:
			notice = NewLaunchNotice(m.LifecycleHookName, m.LifecycleActionToken)
		case TerminationLifecycleAction:
			notice = NewTerminationNotice(m.LifecycleHookName, m.LifecycleActionToken)
		}

		return notice, nil
	}

	return nil, nil
}

func (client *awsClient) SendHeartbeat(ctx context.Context, notice Notice) error {
	var lifecycleNotice *LifecycleNotice
	switch n := notice.(type) {
	case *LaunchNotice:
		lifecycleNotice = n.LifecycleNotice
	case *TerminationNotice:
		lifecycleNotice = n.LifecycleNotice
	default:
		return fmt.Errorf("cannot send heartbeat for %s notice", notice.Type())
	}

	instanceID, err := client.GetInstanceID()
	if err != nil {
		return err
	}

	autoScalingGroupName, err := client.GetAutoScalingGroupName(ctx)
	if err != nil {
		return err
	}

	input := &autoscaling.RecordLifecycleActionHeartbeatInput{
		InstanceId:           aws.String(instanceID),
		AutoScalingGroupName: aws.String(autoScalingGroupName),
		LifecycleHookName:    aws.String(lifecycleNotice.LifecycleHookName),
		LifecycleActionToken: aws.String(lifecycleNotice.LifecycleActionToken),
	}
	if _, err := client.AutoScaling.RecordLifecycleActionHeartbeat(input); err != nil {
		return err
	}
	return nil
}

func (client *awsClient) CompleteLifecycleAction(ctx context.Context, notice Notice) error {
	var lifecycleNotice *LifecycleNotice
	switch n := notice.(type) {
	case *LaunchNotice:
		lifecycleNotice = n.LifecycleNotice
	case *TerminationNotice:
		lifecycleNotice = n.LifecycleNotice
	default:
		return fmt.Errorf("cannot continue lifecycle action for %s notice", notice.Type())
	}

	instanceID, err := client.GetInstanceID()
	if err != nil {
		return err
	}

	autoScalingGroupName, err := client.GetAutoScalingGroupName(ctx)
	if err != nil {
		return err
	}

	input := &autoscaling.CompleteLifecycleActionInput{
		InstanceId:            aws.String(instanceID),
		AutoScalingGroupName:  aws.String(autoScalingGroupName),
		LifecycleHookName:     aws.String(lifecycleNotice.LifecycleHookName),
		LifecycleActionToken:  aws.String(lifecycleNotice.LifecycleActionToken),
		LifecycleActionResult: aws.String("CONTINUE"),
	}
	if _, err := client.AutoScaling.CompleteLifecycleAction(input); err != nil {
		return err
	}
	return nil
}
