// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api/resetworkflow"
)

func (s *FunctionalSuite) TestResetWorkflow() {
	id := "functional-reset-workflow-test"
	wt := "functional-reset-workflow-test-type"
	tq := "functional-reset-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	activityData := int32(1)
	activityCount := 3
	isFirstTaskProcessed := false
	isSecondTaskProcessed := false
	var firstActivityCompletionEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !isFirstTaskProcessed {
			// Schedule 3 activities on first workflow task
			isFirstTaskProcessed = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			var scheduleActivityCommands []*commandpb.Command
			for i := 1; i <= activityCount; i++ {
				scheduleActivityCommands = append(scheduleActivityCommands, &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             strconv.Itoa(i),
						ActivityType:           &commonpb.ActivityType{Name: "ResetActivity"},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(100 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(5 * time.Second),
					}},
				})
			}

			return scheduleActivityCommands, nil
		} else if !isSecondTaskProcessed {
			// Confirm one activity completion on second workflow task
			isSecondTaskProcessed = true
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
					firstActivityCompletionEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil

	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Process first workflow task to schedule activities
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// Reset workflow execution
	_, err = s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process second activity", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process third activity", tag.Error(err))
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process final workflow task", tag.Error(err))
	s.NoError(err)

	s.NotNil(firstActivityCompletionEvent)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplyAll() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplySignal() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplyNone() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplyAll() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplySignal() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplyNone() {
	t := resetTest{
		FunctionalSuite:     s,
		tv:                  testvars.New(s.T().Name()),
		reapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:         enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

type resetTest struct {
	*FunctionalSuite
	tv                  *testvars.TestVars
	reapplyExcludeTypes []enumspb.ResetReapplyExcludeType
	reapplyType         enumspb.ResetReapplyType
	totalSignals        int
	totalUpdates        int
	wftCounter          int
	commandsCompleted   bool
	messagesCompleted   bool
}

func (t resetTest) sendSignalAndProcessWFT(poller *TaskPoller) {
	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		RequestId:         uuid.New(),
		Namespace:         t.namespace,
		WorkflowExecution: t.tv.WorkflowExecution(),
		SignalName:        t.tv.HandlerName(),
		Input:             t.tv.Any().Payloads(),
		Identity:          t.tv.WorkerIdentity(),
	}
	_, err := t.engine.SignalWorkflowExecution(NewContext(), signalRequest)
	t.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	t.NoError(err)
}

func (t resetTest) sendUpdateAndProcessWFT(updateId string, poller *TaskPoller) {
	updateResponse := make(chan error)
	pollResponse := make(chan error)
	go func() {
		_, err := t.FunctionalSuite.sendUpdateWaitPolicyAccepted(t.tv, updateId)
		updateResponse <- err
	}()
	go func() {
		// Blocks until the update request causes a WFT to be dispatched; then sends the update acceptance message
		// required for the update request to return.
		_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
		pollResponse <- err
	}()
	t.NoError(<-updateResponse)
	t.NoError(<-pollResponse)
}

func (t *resetTest) messageHandler(_ *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {

	// Increment WFT counter here; messageHandler is invoked prior to wftHandler
	t.wftCounter++

	// There's an initial empty WFT; then come `totalUpdates` updates, followed by `totalSignals` signals, each in a
	// separate WFT. We must accept the updates, but otherwise respond with empty messages.
	if t.wftCounter == t.totalUpdates+t.totalSignals+1 {
		t.messagesCompleted = true
	}
	if t.wftCounter > t.totalSignals+1 {
		updateId := fmt.Sprint(t.wftCounter - t.totalSignals - 1)
		return []*protocolpb.Message{
			{
				Id:                 "accept-" + updateId,
				ProtocolInstanceId: t.tv.UpdateID(updateId),
				Body: protoutils.MarshalAny(t.T(), &updatepb.Acceptance{
					AcceptedRequestMessageId:         "fake-request-message-id",
					AcceptedRequestSequencingEventId: int64(-1),
				}),
			},
		}, nil
	}
	return []*protocolpb.Message{}, nil

}

func (t *resetTest) wftHandler(_ *commonpb.WorkflowExecution, _ *commonpb.WorkflowType, _ int64, _ int64, _ *historypb.History) ([]*commandpb.Command, error) {

	commands := []*commandpb.Command{}

	// There's an initial empty WFT; then come `totalSignals` signals, followed by `totalUpdates` updates, each in
	// a separate WFT. We must send COMPLETE_WORKFLOW_EXECUTION in the final WFT.
	if t.wftCounter > t.totalSignals+1 {
		updateId := fmt.Sprint(t.wftCounter - t.totalSignals - 1)
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: "accept-" + updateId,
			}},
		})
	}
	if t.wftCounter == t.totalSignals+t.totalUpdates+1 {
		t.commandsCompleted = true
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: t.tv.Any().Payloads(),
				},
			},
		})
	}
	return commands, nil
}

func (t resetTest) reset(eventId int64) string {
	resp, err := t.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 t.namespace,
		WorkflowExecution:         t.tv.WorkflowExecution(),
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: eventId,
		RequestId:                 uuid.New(),
		ResetReapplyType:          t.reapplyType,
		ResetReapplyExcludeTypes:  t.reapplyExcludeTypes,
	})
	t.NoError(err)
	return resp.RunId
}

func (t *resetTest) run() {
	t.totalSignals = 2
	t.totalUpdates = 2
	t.tv = t.FunctionalSuite.startWorkflow(t.tv)

	poller := &TaskPoller{
		Engine:              t.engine,
		Namespace:           t.namespace,
		TaskQueue:           t.tv.TaskQueue(),
		Identity:            t.tv.WorkerIdentity(),
		WorkflowTaskHandler: t.wftHandler,
		MessageHandler:      t.messageHandler,
		Logger:              t.Logger,
		T:                   t.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	t.NoError(err)

	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
`, t.getHistory(t.namespace, t.tv.WorkflowExecution()))

	for i := 1; i <= t.totalSignals; i++ {
		t.sendSignalAndProcessWFT(poller)
	}
	for i := 1; i <= t.totalUpdates; i++ {
		t.sendUpdateAndProcessWFT(fmt.Sprint(i), poller)
	}
	t.True(t.commandsCompleted)
	t.True(t.messagesCompleted)

	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionSignaled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionUpdateAccepted
 17 WorkflowTaskScheduled
 18 WorkflowTaskStarted
 19 WorkflowTaskCompleted
 20 WorkflowExecutionUpdateAccepted
 21 WorkflowExecutionCompleted
`, t.getHistory(t.namespace, t.tv.WorkflowExecution()))

	resetToEventId := int64(4)
	newRunId := t.reset(resetToEventId)
	t.tv = t.tv.WithRunID(newRunId)
	events := t.getHistory(t.namespace, t.tv.WorkflowExecution())

	resetReapplyExcludeTypes := resetworkflow.GetResetReapplyExcludeTypes(t.reapplyExcludeTypes, t.reapplyType)
	signals := !resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL]
	updates := !resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE]

	if !signals && !updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
`, events)
	} else if !signals && updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionUpdateAdmitted
  6 WorkflowExecutionUpdateAdmitted
  7 WorkflowTaskScheduled
`, events)
	} else if signals && !updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
`, events)
	} else {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionUpdateAdmitted
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowTaskScheduled
`, events)
		resetToEventId := int64(4)
		newRunId := t.reset(resetToEventId)
		t.tv = t.tv.WithRunID(newRunId)
		events = t.getHistory(t.namespace, t.tv.WorkflowExecution())
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionUpdateAdmitted
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowTaskScheduled
`, events)
	}
}

func (s *FunctionalSuite) TestResetWorkflow_ReapplyBufferAll() {
	workflowID := "functional-reset-workflow-test-reapply-buffer-all"
	workflowTypeName := "functional-reset-workflow-test-reapply-buffer-all-type"
	taskQueueName := "functional-reset-workflow-test-reapply-buffer-all-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *FunctionalSuite) TestResetWorkflow_ReapplyBufferNone() {
	workflowID := "functional-reset-workflow-test-reapply-buffer-none"
	workflowTypeName := "functional-reset-workflow-test-reapply-buffer-none-type"
	taskQueueName := "functional-reset-workflow-test-reapply-buffer-none-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *FunctionalSuite) testResetWorkflowReapplyBuffer(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	reapplyType enumspb.ResetReapplyType,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	runID := we.RunId

	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Data: []byte("random data"),
		}}},
		Identity: identity,
	}

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	resetRunID := ""
	workflowComplete := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if len(resetRunID) == 0 {
			signalRequest.RequestId = uuid.New()
			_, err := s.engine.SignalWorkflowExecution(NewContext(), signalRequest)
			s.NoError(err)

			// events layout
			//  1. WorkflowExecutionStarted
			//  2. WorkflowTaskScheduled
			//  3. WorkflowTaskStarted
			//  x. WorkflowExecutionSignaled

			resp, err := s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Reason:                    "reset execution from test",
				WorkflowTaskFinishEventId: 3,
				RequestId:                 uuid.New(),
				ResetReapplyType:          reapplyType,
			})
			s.NoError(err)
			resetRunID = resp.RunId

			return []*commandpb.Command{}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err) // due to workflow termination (reset)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resetRunID,
	})
	signalCount := 0
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			signalCount++
		}
	}

	switch reapplyType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		s.Equal(1, signalCount)
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		s.Equal(0, signalCount)
	default:
		panic(fmt.Sprintf("unknown reset reapply type: %v", reapplyType))
	}

}

func (s *FunctionalSuite) TestUpdateMessageInLastWFT() {
	tv := testvars.New(s.T().Name())
	workflowId := "wfid"
	workflowTypeName := "wftype"
	taskQueueName := "tq"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	updateId := tv.UpdateID("update-id")

	we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowId,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	wtHandler := func(*commonpb.WorkflowExecution, *commonpb.WorkflowType, int64, int64, *historypb.History) ([]*commandpb.Command, error) {
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
				Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
					MessageId: tv.MessageID("accept-msg"),
				}},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Done"),
					},
				},
			}}, nil
	}

	messageHandler := func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		return []*protocolpb.Message{
			{
				Id:                 tv.MessageID("accept-msg"),
				ProtocolInstanceId: updateId,
				Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
					AcceptedRequestMessageId:         "request-msg",
					AcceptedRequestSequencingEventId: int64(1),
				}),
			},
		}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      messageHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResponse := make(chan error)
	pollResponse := make(chan error)
	request := &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      we.RunId,
		},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{UpdateId: updateId, Identity: identity},
			Input: &updatepb.Input{
				Name: tv.HandlerName(),
				Args: payloads.EncodeString("args-value-of-" + updateId),
			},
		},
	}
	go func() {
		_, err := s.engine.UpdateWorkflowExecution(NewContext(), request)
		updateResponse <- err
	}()
	go func() {
		// Blocks until the update request causes a WFT to be dispatched; then sends the update complete message
		// required for the update request to return.
		_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
		pollResponse <- err
	}()
	s.NoError(<-updateResponse)
	s.NoError(<-pollResponse)

	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: workflowId,
		RunId:      we.RunId,
	})
	updateCount := 0
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			updateCount++
		}
	}
	s.Equal(1, updateCount)
}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_Schedule() {
	workflowID := "functional-reset-workflow-test-schedule"
	workflowTypeName := "functional-reset-workflow-test-schedule-type"
	taskQueueName := "functional-reset-workflow-test-schedule-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 3)
}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_ScheduleToStart() {
	workflowID := "functional-reset-workflow-test-schedule-to-start"
	workflowTypeName := "functional-reset-workflow-test-schedule-to-start-type"
	taskQueueName := "functional-reset-workflow-test-schedule-to-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 4)
}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_Start() {
	workflowID := "functional-reset-workflow-test-start"
	workflowTypeName := "functional-reset-workflow-test-start-type"
	taskQueueName := "functional-reset-workflow-test-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 5)
}

func (s *FunctionalSuite) testResetWorkflowRangeScheduleToStart(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	resetToEventID int64,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
			{Data: []byte("random signal payload")},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// workflow logic
	workflowComplete := false
	isWorkflowTaskProcessed := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !isWorkflowTaskProcessed {
			isWorkflowTaskProcessed = true
			return []*commandpb.Command{}, nil
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil

	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	_, err = s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
}
