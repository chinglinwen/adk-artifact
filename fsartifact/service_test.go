// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fsartifact_test

import (
	"testing"

	"github.com/chinglinwen/adk-artifact/fsartifact"
	"github.com/chinglinwen/adk-artifact/tests"
	"google.golang.org/adk/artifact"
)

func TestFSArtifactService(t *testing.T) {
	factory := func(t *testing.T) (artifact.Service, error) {
		dir := t.TempDir()
		return fsartifact.NewService(dir)
	}
	tests.TestArtifactService(t, "FSArtifact", factory)
}
