package impl

import (
	"gopkg.in/bblfsh/sdk.v2/driver/native"
	"gopkg.in/bblfsh/sdk.v2/driver/server"
)

func init() {
	// Can be overridden to link a native driver into a Go driver server.
	server.DefaultDriver = native.NewDriver(native.UTF8)
}
