// Copyright (c) 2019 Uber Technologies, Inc.
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

package config

import (
	"errors"

	"github.com/temporalio/temporal/common"
)

// Validate validates the archival config
func (a *Archival) Validate(namespaceDefaults *ArchivalNamespaceDefaults) error {
	if !isArchivalConfigValid(a.History.Status, a.History.EnableRead, namespaceDefaults.History.Status, namespaceDefaults.History.URI, a.History.Provider != nil) {
		return errors.New("Invalid history archival config")
	}

	if !isArchivalConfigValid(a.Visibility.Status, a.Visibility.EnableRead, namespaceDefaults.Visibility.Status, namespaceDefaults.Visibility.URI, a.Visibility.Provider != nil) {
		return errors.New("Invalid visibility archival config")
	}

	return nil
}

func isArchivalConfigValid(
	clusterStatus string,
	enableRead bool,
	namespaceDefaultStatus string,
	domianDefaultURI string,
	specifiedProvider bool,
) bool {
	archivalEnabled := clusterStatus == common.ArchivalEnabled
	URISet := len(domianDefaultURI) != 0

	validEnable := archivalEnabled && URISet && specifiedProvider
	validDisabled := !archivalEnabled && !enableRead && namespaceDefaultStatus != common.ArchivalEnabled && !URISet && !specifiedProvider
	return validEnable || validDisabled
}
