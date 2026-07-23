package main

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	policy := &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(time.Second),
		BackoffCoefficient: 2.0,
		MaximumInterval:    durationpb.New(100 * time.Second),
	}
	for attempt := int32(1); attempt <= 40; attempt++ {
		raw := backoff.ExponentialBackoffAlgorithm(policy.InitialInterval, policy.BackoffCoefficient, attempt)
		capped := backoff.CalculateExponentialRetryInterval(policy, attempt)
		fmt.Printf("%d\t%s\t%s\n", attempt, raw, capped)
	}
}
