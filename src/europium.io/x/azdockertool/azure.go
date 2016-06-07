package azdockertool

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	sdk "github.com/Azure/azure-sdk-for-go/storage"
	log "github.com/Sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrNoSuchImage      error = errors.New("image not found")
	ErrMultipleResults  error = errors.New("multiple results found; try to narrow down your query")
	ErrTooLargeToCommit error = errors.New("did not commit upload because it is too large (> 1 TiB)")
	ErrFileNotFound     error = errors.New("file not found")
)

const (
	MaxBlobBlockSize = 4 * 1024 * 1024 // 4 MiB
	MaxBlobBlockId   = 262144          // 262144 * 4 MiB = 1 TiB
)

type absremote struct {
	config      *Config
	client      sdk.Client
	blobStorage sdk.BlobStorageClient
}

// Returns an Azure Blob Storage backend
func NewAzureBlobStorageRemote(config *Config) (Remote, error) {
	client, err := sdk.NewBasicClient(config.AccountName, config.AccountKey)
	if err != nil {
		return nil, err
	}

	remote := &absremote{
		config:      config,
		client:      client,
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

// Sends a file to Azure Blob Storage
func PutBlockBlobFromFile(client sdk.BlobStorageClient, container, name, path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer f.Close()

	return putBlockBlob(client, container, name, f, MaxBlobBlockSize)
}

func putBlockBlob(client sdk.BlobStorageClient, container, name string, blob io.Reader, chunkSize int) error {
	if chunkSize <= 0 || chunkSize > MaxBlobBlockSize {
		chunkSize = MaxBlobBlockSize
	}

	chunk := make([]byte, chunkSize)
	n, err := blob.Read(chunk)
	if err != nil && err != io.EOF {
		return err
	}

	if err == io.EOF {
		// Fits into one block
		return putSingleBlockBlob(client, container, name, chunk[:n])
	} else {
		// Does not fit into one block. Upload block by block then commit the block list
		blockList := []sdk.Block{}

		// Put blocks
		for blockNum := 0; blockNum < MaxBlobBlockId; blockNum++ {
			id := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%07d", blockNum)))
			data := chunk[:n]
			err = client.PutBlock(container, name, id, data)
			if err != nil {
				return err // todo(politician): retries
			}

			blockList = append(blockList, sdk.Block{id, sdk.BlockStatusLatest})

			// Read next block
			n, err = blob.Read(chunk)
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}

			if blockNum%10 == 0 {
				log.WithFields(log.Fields{
					"blocks": blockNum + 1,
					"MiB":    uint(blockNum+1) * uint(MaxBlobBlockSize),
				}).Info("progress")
			}
		}

		if _, err := blob.Read(chunk); err != io.EOF {
			return ErrTooLargeToCommit // max block id exceeded
		}

		log.WithFields(log.Fields{
			"name":   name,
			"blocks": len(blockList),
		}).Info("committing block list")

		// Commit block list
		return client.PutBlockList(container, name, blockList)
	}
}

func putSingleBlockBlob(client sdk.BlobStorageClient, container, name string, chunk []byte) error {
	if len(chunk) > MaxBlobBlockSize {
		return fmt.Errorf("storage: provided chunk (%d bytes) cannot fit into single-block blob (max %d bytes)", len(chunk), MaxBlobBlockSize)
	}

	size := uint64(len(chunk))
	r := bytes.NewReader(chunk)
	extraHeaders := make(map[string]string)

	return client.CreateBlockBlobFromReader(container, name, size, r, extraHeaders)
}

// Returns whether or not the remote contains a particular layer
func (ar *absremote) HasLayer(id ID) (bool, error) {
	path := fmt.Sprintf("layers/%s", id.String())

	// using ListBlobs because each layer should contain 3 blobs
	res, err := ar.blobStorage.ListBlobs(ar.config.Container, sdk.ListBlobsParameters{Prefix: path})
	if err != nil {
		return false, fmt.Errorf("remote unavailable: %s", err)
	}

	if len(res.Blobs) == 3 {
		return true, nil
	} else if len(res.Blobs) == 0 {
		return false, nil
	} else {
		return false, errors.New("corrupt or incomplete layer encountered")
	}
}
