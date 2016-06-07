package azdockertool

import (
	"time"
)

type ImageInfo struct {
	Repository   string
	Tag          string
	LastModified time.Time
	Root         string
	Id           ID
}

type PullResult struct {
	Id         ID
	Repository string
	Tag        string
	Src        string
}

type PushResult struct {
}

type Remote interface {
	Images() ([]*ImageInfo, error)
	Pull(query string, known func(id ID) (bool, error), localStorage *LocalStorage) (*PullResult, error)
	// Graph() (*LayerGraph, error)
	Push(query string, exporter func(dir, repository string) error, localStorage *LocalStorage) (*PushResult, error)
}
