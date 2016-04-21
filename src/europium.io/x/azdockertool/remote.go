package azdockertool

import (
	"time"
)

type ImageInfo struct {
	Repository string
	Tag string
	LastModified time.Time
	Root string
}

type PullResult struct {
	Id ID
	Repository string
	Tag string
	Src string
}

type Remote interface {
	Images() ([]*ImageInfo, error)
	Pull(query string, known func(id ID) (bool, error), localStorage *LocalStorage) (*PullResult, error)
	// Graph() (*LayerGraph, error)
}
