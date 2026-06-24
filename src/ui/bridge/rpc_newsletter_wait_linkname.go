package bridge

import (
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"

	_ "unsafe"
)

//go:linkname whatsmeowWaitResponse go.mau.fi/whatsmeow.(*Client).waitResponse
func whatsmeowWaitResponse(*whatsmeow.Client, string) chan *waBinary.Node

//go:linkname whatsmeowCancelResponse go.mau.fi/whatsmeow.(*Client).cancelResponse
func whatsmeowCancelResponse(*whatsmeow.Client, string, chan *waBinary.Node)
