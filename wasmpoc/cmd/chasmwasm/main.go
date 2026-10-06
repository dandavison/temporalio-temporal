package main

import (
	"fmt"

	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/activity"
	"go.temporal.io/server/wasmpoc/common/log"
)

func main() {
	r := chasm.NewRegistry(log.NewNoopLogger())
	fmt.Println(r.Register(&chasm.CoreLibrary{}), r.Register(activity.NewLibrary(nil, &activity.Config{})))
}
