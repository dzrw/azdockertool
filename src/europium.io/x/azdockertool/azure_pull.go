package azdockertool

import (
	"encoding/json"
	"fmt"
	sdk "github.com/Azure/azure-sdk-for-go/storage"
	"os"
	"path/filepath"
	"strings"
)

func (ar *absremote) Pull(query string, known func(id ID) (bool, error), localStorage *LocalStorage) (*PullResult, error) {

	// resolve the query to a layer
	var root ID
	repo, tag := toRepositoryAndTag(query)
	root, err := ar.findLayerByImageAndTag(repo, tag)
	if err != nil {
		root, err = ar.findLayerByHash(query)
		if err != nil {
			return nil, err
		}
	}

	fmt.Printf("Image '%s' resolved to ID '%s'\n", query, root.Short())

	// figure out what to download
	fmt.Println("Determining which images need to be downloaded from remote...")
	todo, err := ar.discoverLayers(root, known)
	if err != nil {
		return nil, err
	}

	workdir, err := localStorage.TempDir(string(root.Short()))
	if err != nil {
		return nil, err
	}

	// download all the things
	fmt.Println("Downloading images from remote...")
	for _, id := range todo {
		srcDir := strings.Join([]string{"images", id.String()}, "/")
		dstDir := filepath.Join(workdir, id.String())

		fmt.Printf("Pulling image id '%s' to: %v\n", id.Short(), dstDir)
		_, err := ar.fetchAll(srcDir, dstDir)
		if err != nil {
			return nil, err
		}
	}

	// write the image manifest
	fmt.Println("Generating repositories JSON file...")
	err = emitImageManifest(root, repo, tag, workdir)
	if err != nil {
		return nil, err
	}

	return &PullResult{root, repo, tag, workdir}, nil
}

// Returns a repository and a tag from an docker image ID
func toRepositoryAndTag(image string) (repository string, tag string) {
	s := strings.TrimPrefix(image, "sha256:")
	a := strings.Split(s, ":")
	if len(a) == 1 {
		return a[0], "latest"
	} else {
		return a[0], a[1]
	}
}

// finds ancestors of the given id until cancelled or the chain ends
func (ar *absremote) discoverLayers(root ID, known func(id ID) (bool, error)) ([]ID, error) {

	coll := []ID{}

	for curr := root; curr != ""; {
		fmt.Printf("Examining id '%s' in remote storage...\n", curr.Short())
		node, err := ar.getLayerDescriptor(curr)
		if err != nil {
			return nil, err
		}

		done, err := known(ID(node.Id))
		if err != nil {
			return nil, err
		} else if done {
			break
		}

		coll = append(coll, ID(node.Id))
		curr = ID(node.Parent)
	}

	return coll, nil
}

// Queries the root layer index to find a corresponding layer identifier, if any
func (ar *absremote) findLayerByImageAndTag(image, tag string) (ID, error) {
	query := fmt.Sprintf("repositories/%s/%s", image, tag)
	body, err := ar.GetBlobAsString(query)
	if err != nil {
		return "", err
	}

	return ID(body), nil
}

// Queries the full image store to locate a layer by its (partial) identifier
func (ar *absremote) findLayerByHash(hash string) (ID, error) {
	// clean up what could be a completely unsafe mess
	coll := strings.Split(hash, "/")
	hash = strings.TrimPrefix(coll[0], "sha256:")
	query := fmt.Sprintf("images/%s", hash)

	// using ListBlobs because we may have been given a partial hash
	res, err := ar.blobStorage.ListBlobs(ar.config.Container, sdk.ListBlobsParameters{Prefix: query})
	if err != nil {
		return "", fmt.Errorf("remote unavailable: %s", err)
	}

	if len(res.Blobs) == 0 {
		return "", ErrNoSuchImage
	} else if len(res.Blobs) > 1 {
		return "", ErrMultipleResults
	} else {
		coll = strings.Split(res.Blobs[0].Name, "/")
		return ID(coll[1]), nil
	}
}

type layer struct {
	Id     string `json:"id"`
	Parent string `json:"parent"`
}

// Download the json file at images/{id}/json
func (ar *absremote) getLayerDescriptor(id ID) (*layer, error) {
	path := strings.Join([]string{"images", id.String(), "json"}, "/")

	body, err := ar.GetBlobAsString(path)
	if err != nil {
		return nil, err
	}

	l := &layer{}
	if err := json.Unmarshal([]byte(body), l); err != nil {
		return nil, err
	}

	return l, nil
}

func emitImageManifest(id ID, repo, tag, workdir string) error {
	path := filepath.Join(workdir, "repositories")
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	type Repository map[string]string

	repositories := map[string]Repository{}
	repositories[repo] = Repository{}
	repositories[repo][tag] = id.String()

	return json.NewEncoder(file).Encode(&repositories)
}
