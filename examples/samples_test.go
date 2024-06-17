package examples

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
				// Verify that we can run the sample.
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				cmd := exec.CommandContext(ctx, "go", "run", "main.go")
				cmd.Dir = item.Name()
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				if err := cmd.Run(); err != nil {
					cancel()
					t.Fatalf("failed to run sample %v: %v", item.Name(), stderr.String())
				}
				cancel()
			}
		}
	}
}
