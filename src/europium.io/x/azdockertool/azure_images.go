package azdockertool

import (
	sdk "github.com/Azure/azure-sdk-for-go/storage"
	log "github.com/Sirupsen/logrus"
	"sort"
	"strings"
	"time"
)

const (
	imageSearchPrefix string = "refs/"
	azureDateLayout   string = time.RFC1123
)

func (ar *absremote) Images() ([]*ImageInfo, error) {
	var coll []*ImageInfo

	// call azure
	res, err := ar.blobStorage.ListBlobs(ar.config.Container, sdk.ListBlobsParameters{Prefix: imageSearchPrefix})
	if err != nil {
		return nil, err
	}

	if ar.config.Verbose {
		log.WithFields(log.Fields{
			"count": len(res.Blobs),
		}).Info("found matching images")
	}

	// process the response
	for _, item := range res.Blobs {

		id, err := ar.GetBlobAsString(item.Name)
		if err != nil {
			log.WithFields(log.Fields{
				"path": item.Name,
			}).Warn("skipping due to missing image pointer")
			continue
		}

		s := strings.TrimPrefix(item.Name, imageSearchPrefix)
		parts := strings.Split(s, "/")

		n := len(parts)
		if n < 2 {
			log.WithFields(log.Fields{
				"path": item.Name,
				"id":   id,
			}).Warn("skipping due to malformed image path")
			continue
		}

		tag := parts[n-1]
		img := strings.Join(parts[:(n-1)], "/")

		modified, err := time.Parse(azureDateLayout, item.Properties.LastModified)
		if err != nil {
			log.WithFields(log.Fields{
				"path":         item.Name,
				"id":           id,
				"LastModified": item.Properties.LastModified,
			}).Warn("skipping due to malformed last modified")
			continue
		}

		coll = append(coll, &ImageInfo{img, tag, modified, item.Name, ID(id)})
	}

	// sort, because we aren't monsters
	sort.Sort(ByRepositoryThenTag(coll))

	return coll, nil
}

// ByRepositoryThenTag implements sort.Interface for []*ImageInfo based on the Name, Tag fields
type ByRepositoryThenTag []*ImageInfo

func (a ByRepositoryThenTag) Len() int      { return len(a) }
func (a ByRepositoryThenTag) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByRepositoryThenTag) Less(i, j int) bool {
	n := strings.Compare(a[i].Repository, a[j].Repository)
	if n == 0 {
		n = strings.Compare(a[i].Tag, a[j].Tag)
	}

	return n <= 0
}
