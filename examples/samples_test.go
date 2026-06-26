// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package examples

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunSamples(t *testing.T) {
	items, _ := os.ReadDir(".")
	for _, item := range items {
		if item.IsDir() {
			mainFile, err := os.Stat(filepath.Join(item.Name(), "main.go"))
			if err != nil {
				t.Fatalf("failed to check for main.go file in %v: %v", item.Name(), err)
			}
			if !mainFile.IsDir() {
				t.Run(item.Name(), func(t *testing.T) {
					// Verify that we can run the sample.
					timeout := time.Minute
					if item.Name() == "emulator" {
						timeout = 3 * time.Minute
						if !isDockerAvailable() {
							t.Skip("Docker is not available; skipping self-starting emulator test")
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					cmd := exec.CommandContext(ctx, "go", "run", "-race", ".")
					cmd.Dir = item.Name()
					if item.Name() == "emulator" {
						var env []string
						for _, envVar := range os.Environ() {
							if !strings.HasPrefix(envVar, "SPANNER_EMULATOR_HOST=") {
								env = append(env, envVar)
							}
						}
						cmd.Env = env
					}
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					if err := cmd.Run(); err != nil {
						cancel()
						t.Fatalf("failed to run sample %v: %v", item.Name(), stderr.String())
					}
					cancel()
				})
			}
		}
	}
}

func isDockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}
