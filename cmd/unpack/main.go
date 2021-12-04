package main

import (
	"log"

	"github.com/joelanford/kuberpak/cmd/unpack/internal/unpacker"
)

func main() {
	cmd := unpacker.UnpackCommand()
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
