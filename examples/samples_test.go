package examples

import (
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
				if err := cmd.Start(); err != nil {
					cancel()
					t.Fatalf("failed to start sample %v: %v", item.Name(), err)
				}
				if err := cmd.Wait(); err != nil {
					cancel()
					t.Fatalf("failed to wait for sample %v: %v (%v)", item.Name(), err, cmd.Err)
				}
				cancel()
				if err = os.Chdir("./.."); err != nil {
					t.Fatalf("failed to change current directory to back to main sample directory: %v", err)
				}
			}
		}
	}
}
