package main

import (
	"fmt"

	"go.temporal.io/server/wasmpoc/chasm"
)

func main() {
	fmt.Println(chasm.NewRegistry != nil)
}
