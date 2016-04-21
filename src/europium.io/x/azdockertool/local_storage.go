package azdockertool

import (
	"fmt"
	"io/ioutil"
	"errors"
	"os"
	"strings"
	"path/filepath"
)

var (
	ErrCouldNotCreateDir error = errors.New("Could not create a directory for this operation")
)

type LocalStorage struct {
	basedir string
}

// creates a new staging area
func NewLocalStorage() (*LocalStorage, error) {
	basedir, err := ioutil.TempDir("", "azdockertool")
	if err != nil {
		return nil, ErrCouldNotCreateDir
	}

	return &LocalStorage{basedir}, nil
}

// dispose the stage, removing all temporary directories
func (s *LocalStorage) Dispose() {
	if err := os.RemoveAll(s.basedir); err != nil {
		fmt.Println(err)
	}
}

// creates a new temporary directory within the staging area that will be cleaned up on exit
func (s *LocalStorage) TempDir(suffix string) (string, error) {
	suffix = strings.Replace(suffix, ":", "_", -1)
	path := filepath.Join(s.basedir, suffix)

	fmt.Printf("WorkDir: %v\n", path)

	if err := os.MkdirAll(path, os.ModeDir|0700); err != nil {
		return "", ErrCouldNotCreateDir
	}

	return path, nil
}

