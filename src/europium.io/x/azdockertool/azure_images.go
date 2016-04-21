package azdockertool

import (
	"fmt"
	sdk "github.com/Azure/azure-sdk-for-go/storage"
	"sort"
	"strings"
	"time"
)

const (
	imageSearchPrefix string = "repositories/"
	azureDateLayout string = time.RFC1123
)

func (ar *absremote) Images() ([]*ImageInfo, error) {
	var coll []*ImageInfo

	// call azure
	res, err := ar.blobStorage.ListBlobs(ar.config.Container, sdk.ListBlobsParameters{Prefix: imageSearchPrefix})
	if err != nil {
		return nil, fmt.Errorf("remote unavailable: %s", err)
	}

	if ar.config.Verbose {
	 	fmt.Printf("found %d matching blobs\n", len(res.Blobs))
	}

	// process the response
	for _, item := range res.Blobs {

		// too verbose
		// if ar.config.Verbose {
		// 	fmt.Printf("parsing '%s'\n", item.Name)
		// }

		s := strings.TrimPrefix(item.Name, imageSearchPrefix)
		parts := strings.Split(s, "/")

		n := len(parts)
		if n < 2 {
			fmt.Printf("skipping due to malformed image path: '%s'\n", item.Name)
			continue
		}

		tag := parts[n - 1]
		img := strings.Join(parts[:(n - 1)], "/")

		modified, err := time.Parse(azureDateLayout, item.Properties.LastModified)
		if err != nil {
			fmt.Printf("skipping due to malformed last modified: '%s' at '%s'\n", item.Properties.LastModified, item.Name)
			continue
		}

		// too verbose
		// if ar.config.Verbose {
		// 	fmt.Printf("found image '%s:%s'\n", img, tag)
		// }

		coll = append(coll, &ImageInfo{img, tag, modified, item.Name})
	}

	// sort, because we aren't monsters
	sort.Sort(ByRepositoryThenTag(coll))

	return coll, nil
}

// ByRepositoryThenTag implements sort.Interface for []*ImageInfo based on the Name, Tag fields
type ByRepositoryThenTag []*ImageInfo

func (a ByRepositoryThenTag) Len() int           { return len(a) }
func (a ByRepositoryThenTag) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByRepositoryThenTag) Less(i, j int) bool { 
	n := strings.Compare(a[i].Repository, a[j].Repository)
	if n == 0 {
		n = strings.Compare(a[i].Tag, a[j].Tag)
	}

	return n <= 0
}