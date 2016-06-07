package azdockertool

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

// Sends a Docker image to Azure Blob Storage
func (ar *absremote) Push(query string, exporter func(dir, repository string) error, localStorage *LocalStorage) (*PushResult, error) {
	workdir, err := localStorage.TempDir(fmt.Sprintf("azdockertool_%08d", rand.Int31()))
	if err != nil {
		return nil, err
	}

	// export the image to disk (uncompressed)
	err = exporter(workdir, query)
	if err != nil {
		return nil, err
	}

	if ar.config.Verbose {
		log.WithFields(log.Fields{
			"repository":        query,
			"working directory": workdir,
		}).Info("exported image")
	}

	// open the image manifest (docker 1.10+)
	m, err := openManifest(workdir)
	if err != nil {
		return nil, err
	}

	// TODO: check if we're overwriting an existing tag

	// determine which layers we need to upload
	missing, err := ar.findMissingLayers(m.LayerIds())
	if err != nil {
		return nil, err
	}

	// TODO: report status of each layer (found, missing, sending, completed)
	if ar.config.Verbose {
		log.WithFields(log.Fields{
			"found":   len(m.Layers) - len(missing),
			"missing": len(missing),
		}).Info("completed layer discovery")
	} else {
		log.WithFields(log.Fields{
			"count": len(missing),
		}).Info("sending missing layers")
	}

	// TODO: concurrent layer uploads (controllable by command-line option)

	// upload any missing layers
	for _, id := range missing {
		if ar.config.Verbose {
			log.WithFields(log.Fields{
				"layer id": string(id),
			}).Info("uploading layer")
		}

		err := ar.putImageLayer(workdir, id)
		if err != nil {
			log.WithFields(log.Fields{
				"layer id": string(id),
				"rollback": false,
			}).Error("failed to upload missing layer")
			return nil, err
		}
	}

	// now upload image metadata
	err = ar.putImageMetadata(m, workdir)
	if err != nil {
		return nil, err
	}

	err = ar.putImageRefs(m, workdir)
	if err != nil {
		return nil, err
	}

	return &PushResult{}, nil
}

func (ar *absremote) putImageMetadata(m *manifest, dir string) error {
	const (
		ImagesFormat = "images/%s/%s"
	)

	id := m.ImageId()

	parts := make(map[string]string)
	parts[filepath.Join(dir, "manifest.json")] = fmt.Sprintf(ImagesFormat, id, "manifest.json")
	parts[filepath.Join(dir, "repositories")] = fmt.Sprintf(ImagesFormat, id, "repositories")
	parts[filepath.Join(dir, m.Config)] = fmt.Sprintf(ImagesFormat, id, "json")

	err := ar.putBlockBlobsFromFiles(parts)
	if err != nil {
		log.WithFields(log.Fields{
			"image id": string(id),
			"rollback": false,
		}).Error("failed to upload image metadata")
		return err
	}

	log.WithFields(log.Fields{
		"image id": m.ImageId(),
	}).Info("published image")

	return nil
}

func (ar *absremote) putImageRefs(m *manifest, dir string) error {
	const (
		RefsFormat = "refs/%s/%s"
	)

	id := m.ImageId()

	for _, item := range m.RepoTags {
		repo, tag := toRepositoryAndTag(item)

		dst := fmt.Sprintf(RefsFormat, repo, tag)
		chunk := []byte(id)

		err := putSingleBlockBlob(ar.blobStorage, ar.config.Container, dst, chunk)
		if err != nil {
			log.WithFields(log.Fields{
				"image id":   string(id),
				"repository": repo,
				"tag":        tag,
				"rollback":   false,
			}).Error("failed to set tag")
			return err
		}

		log.WithFields(log.Fields{
			"image id":   m.ImageId(),
			"repository": repo,
			"tag":        tag,
		}).Info("published tag")
	}

	return nil
}

func (ar *absremote) putImageLayer(dir string, layerId ID) error {
	const DstFormat = "layers/%s/%s"

	id := layerId.String()

	parts := make(map[string]string)
	parts[filepath.Join(dir, id, "VERSION")] = fmt.Sprintf(DstFormat, id, "VERSION")
	parts[filepath.Join(dir, id, "json")] = fmt.Sprintf(DstFormat, id, "json")
	parts[filepath.Join(dir, id, "layer.tar")] = fmt.Sprintf(DstFormat, id, "layer.tar")

	return ar.putBlockBlobsFromFiles(parts)
}

func (ar *absremote) putBlockBlobsFromFiles(spec map[string]string) error {
	for src, dst := range spec {
		err := PutBlockBlobFromFile(ar.blobStorage, ar.config.Container, dst, src)
		if err != nil {
			return err // todo(politician): retries
		}
	}

	return nil
}

func (ar *absremote) findMissingLayers(ids []ID) ([]ID, error) {
	var missing []ID
	for _, id := range ids {
		if ok, err := ar.HasLayer(id); !ok && err != nil {
			fmt.Printf("not exists %s\n", id.String())
			missing = append(missing, id)
		} else if err != nil {
			return nil, err
		} else {
			fmt.Printf("exists %s\n", id.String())
		}
	}
	return missing, nil
}

type manifest struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

func openManifest(dir string) (*manifest, error) {
	where := filepath.Join(dir, "manifest.json")

	f, err := os.Open(where)
	if err != nil {
		return nil, err
	}

	var arr []manifest
	err = json.NewDecoder(f).Decode(&arr)
	if err != nil {
		return nil, err
	}

	if len(arr) != 1 {
		return nil, errors.New("idk wtf")
	}

	return &arr[0], nil
}

func (m *manifest) ImageId() string {
	return strings.TrimSuffix(m.Config, filepath.Ext(m.Config))
}

func (m *manifest) LayerIds() []ID {
	var pile []ID

	for _, v := range m.Layers {
		dir, _ := filepath.Split(v)
		pile = append(pile, ID(filepath.Clean(dir)))
	}

	return pile
}
