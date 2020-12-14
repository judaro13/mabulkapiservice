package utils

import (
	"log"
	"os"
	"runtime"
)

// Err var to add Err message to log
var Err = log.New(os.Stderr,
	"ERROR: ",
	log.Ldate|log.Ltime)

// Error func for print errors
func Error(err error) {
	pc, fn, line, _ := runtime.Caller(1)
	if err != nil {
		Err.Printf("optiapi - in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), fn, line, err)
	}
}
