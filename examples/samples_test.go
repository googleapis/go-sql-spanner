package examples

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"
)

func TestRunSamples(t *testing.T) {
	items, _ := os.ReadDir(".")
	for _, item := range items {
		if item.IsDir() {
			fmt.Printf("Running sample %v\n", item.Name())
			mainFile, err := os.Stat(path.Join(item.Name(), "main.go"))
			if err == nil && !mainFile.IsDir() {
				// Verify that we can run the sample.
				if err := os.Chdir(item.Name()); err != nil {
					t.Fatalf("failed to change current directory to %v: %v", item.Name(), err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				cmd := exec.CommandContext(ctx, "go", "run", "main.go")
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				if err := cmd.Run(); err != nil {
					cancel()
					t.Fatalf("failed to run sample %v: %v", item.Name(), stderr.String())
				}
				cancel()
				if err = os.Chdir("./.."); err != nil {
					t.Fatalf("failed to change current directory to back to main sample directory: %v", err)
				}
			}
		}
	}
}
