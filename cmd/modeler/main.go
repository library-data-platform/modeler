package main

import (
	"fmt"
	"os"

	"github.com/library-data-platform/modeler"
)

func main() {
	var err error
	var m *modeler.Model
	if m, err = modeler.NewModel(os.Args[1], progress); err != nil {
		panic(err)
	}
	// fmt.Println(m.EncodeDOT())
	fmt.Println(m.EncodeSQL())
}

func progress(m string) {
	fmt.Fprintf(os.Stderr, "modeler: %s\n", m)
}
