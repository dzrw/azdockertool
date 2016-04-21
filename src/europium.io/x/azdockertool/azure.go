package azdockertool

import (
	"fmt"
	sdk "github.com/Azure/azure-sdk-for-go/storage"
	"strings"
	"errors"
	"io"
	"bufio"
	"path/filepath"
	"os"
)

var (
	ErrNoSuchImage error = errors.New("image not found")
	ErrMultipleResults error = errors.New("multiple results found; try to narrow down your query")
)

type absremote struct {
	config *Config
	client sdk.Client
	blobStorage sdk.BlobStorageClient
}

// Returns an Azure Blob Storage backend
func NewAzureBlobStorageRemote(config *Config) (Remote, error) {
	client, err := sdk.NewBasicClient(config.AccountName, config.AccountKey)
	if err != nil {
		return nil, err
	}

	remote := &absremote{
		config: config, 
		client: client, 
		blobStorage: client.GetBlobService(),
	}

	return remote, nil
}

// Retrieves a file from Azure Blob Storage and interprets it as a string
func (ar *absremote) GetBlobAsString(path string) (string, error) {
	f, err := ar.blobStorage.GetBlob(ar.config.Container, path)
	if err != nil {
		return "", err
	}

	defer f.Close()

	// Read until null terminator
	buf := bufio.NewReader(f)
	ys, err := buf.ReadBytes(0)
	if err != nil && err != io.EOF {
		return "", err
	}

	if err == io.EOF {
		return string(ys), nil
	}

	// Don't return the null terminator
	return string(ys[:len(ys)-1]), nil
}

// Downloads all blobs sharing a given prefix to the given dir
func (ar *absremote) fetchAll(srcDir, dstDir string) (n int, err error) {
	res, err := ar.blobStorage.ListBlobs(ar.config.Container, sdk.ListBlobsParameters{Prefix: srcDir})
	if err != nil {
		return 0, fmt.Errorf("remote unavailable: %s", err)
	}

	if err := os.MkdirAll(dstDir, os.ModeDir|0700); err != nil {
		return 0, err
	}

	if ar.config.Verbose {
		fmt.Printf("fetching %d blobs to '%s'...\n", len(res.Blobs), dstDir)
	}

	n = 0
	for _, item := range res.Blobs {
		err = ar.fetch(item.Name, dstDir)
		if err != nil {
			return n, err
		}

		n++
	}

	return n, nil
}

// Downloads a single blob to the given dir
func (ar *absremote) fetch(srcPath, dstDir string) error {
	src, err := ar.blobStorage.GetBlob(ar.config.Container, srcPath)
	if err != nil {
		return fmt.Errorf("could not download '%s': %v\n", srcPath, err) 
	}

	defer src.Close()

	ns := strings.Split(srcPath, "/")
	name := ns[len(ns)-1]

	dst, err := os.Create(filepath.Join(dstDir, name))
	if err != nil {
		return err
	}

	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	return nil
}

