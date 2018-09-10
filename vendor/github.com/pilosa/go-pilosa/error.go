// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"fmt"
)

// Error contains a Pilosa specific error.
type Error struct {
	Message string
}

// NewError creates a Pilosa error.
func NewError(message string) *Error {
	return &Error{Message: message}
}

func (e Error) Error() string {
	return fmt.Sprintf("Error: %s", e.Message)
}

// Predefined Pilosa errors.
var (
	ErrEmptyCluster           = NewError("No usable addresses in the cluster")
	ErrIndexExists            = NewError("Index exists")
	ErrFieldExists            = NewError("Field exists")
	ErrInvalidIndexName       = NewError("Invalid index name")
	ErrInvalidFieldName       = NewError("Invalid field name")
	ErrInvalidLabel           = NewError("Invalid label")
	ErrInvalidKey             = NewError("Invalid key")
	ErrTriedMaxHosts          = NewError("Tried max hosts, still failing")
	ErrAddrURIClusterExpected = NewError("Addresses, URIs or a cluster is expected")
	ErrInvalidQueryOption     = NewError("Invalid query option")
	ErrInvalidIndexOption     = NewError("Invalid index option")
	ErrInvalidFieldOption     = NewError("Invalid field option")
	ErrNoFragmentNodes        = NewError("No fragment nodes")
	ErrNoShard                = NewError("Index has no shards")
	ErrUnknownType            = NewError("Unknown type")
)
