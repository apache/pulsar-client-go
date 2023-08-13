package integration

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	fileRoot = "pulsar/internal/integration_tests/files"
)

var repoRoot string

func repoPath(path ...string) string {
	return filepath.Join(repoRoot, filepath.Join(path...))
}

func MustFilePath(path string) string {
	fPath, err := repoFilePath(path)
	if err != nil {
		panic(err.Error())
	}
	return fPath
}

func MustList(dir string) []string {
	dPath, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	files := []string{}
	for _, file := range dPath {
		files = append(files, file.Name())
	}
	return files
}

func FilePath(t *testing.T, path string) string {
	fPath, err := repoFilePath(path)
	if err != nil {
		t.Fatal(err.Error())
	}
	return fPath
}

func repoFilePath(path string) (string, error) {
	path = repoPath(fileRoot, path)
	_, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	return path, nil
}

func init() {
	rootPath, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("unable to determine repository path for integration testing: %v", err))
	}
	repoRoot = string(bytes.TrimSpace(rootPath))
}
