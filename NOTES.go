// ---------------------------------
// toplevel
// ---------------------------------

	fmt.Printf("Using docker endpoint for push: %v\n", cli.DockerHost)
	fmt.Printf("Remote: %v\n", r.Desc())

	if err = cli.exportToFiles(image, r, imageRoot); err != nil {
		return err
	}

	if err := r.Push(image, imageRoot); err != nil {
		fmt.Printf(`{"Status":"error", "Message": "%v"}`+"\n", err.Error())
		return err
	}

	fmt.Println(`{"Status":"ok"}`)


// ---------------------------------
// exportToFiles
// ---------------------------------

// Ok, I think this thing is doing the following:
// 1. Building the set of all local layers starting from the root ID
// 2. Building the set of all remote layers and taking the set difference (LOCAL - REMOTE)
// 3. "Exporting" the missing layers to disk
// 4. "Exporting" the image metadata (repositories json?) to disk

func (cli *DogestryCli) exportToFiles(image string, r remote.Remote, imageRoot string) error {
	imageHistory, err := cli.Client.ImageHistory(image)
	if err != nil {
		fmt.Printf("Error getting image history: %v\n", err)
		return err
	}

	fmt.Println("Checking layers on remote")

	imageID := remote.ID(imageHistory[0].ID)
	repoName, repoTag := remote.NormaliseImageName(image)

	// Check the remote to see what layers are missing. Only missing Ids will
	// need to be saved to disk when exporting the docker image.

	missingIds := make(set)

	for _, i := range imageHistory {
		id := remote.ID(i.ID)
		_, err = r.ImageMetadata(id)
		if err == nil {
			fmt.Printf("  exists   : %v\n", id)
		} else {
			fmt.Printf("  not found: %v\n", id)
			missingIds[id] = empty
		}
	}

	if len(missingIds) > 0 {
		if err := cli.exportImageToFiles(image, imageRoot, missingIds); err != nil {
			return err
		}
	}

	if err := cli.exportMetaDataToFiles(repoName, repoTag, imageID, imageRoot); err != nil {
		return err
	}

	return nil
}


// Stream the tarball from docker and translate it into the portable repo format
// Note that its easier to handle as a stream on the way out.
func (cli *DogestryCli) exportImageToFiles(image, root string, saveIds set) error {
	fmt.Printf("Exporting image: %v to: %v\n", image, root)

	reader, writer := io.Pipe()
	defer writer.Close()
	defer reader.Close()

	tarball := tar.NewReader(reader)

	errch := make(chan error)

	go func() {
		defer close(errch)
		for {
			header, err := tarball.Next()

			if err == io.EOF {
				break
			}

			if err != nil {
				errch <- err
				return
			}

			parts := strings.Split(header.Name, "/")
			idFromFile := remote.ID(parts[0])

			if _, ok := saveIds[idFromFile]; ok {
				if err := cli.createFileFromTar(root, header, tarball); err != nil {
					errch <- err
					return
				}
			} else {
				// Drain the reader. Is this necessary?
				if _, err := io.Copy(ioutil.Discard, tarball); err != nil {
					errch <- err
					return
				}

			}
		}

		errch <- nil
	}()

	if err := cli.Client.ExportImage(docker.ExportImageOptions{image, writer}); err != nil {
		return err
	}

	// wait for the tar reader
	if err := <-errch; err != nil {
		return err
	}

	return nil
}


func (cli *DogestryCli) createFileFromTar(root string, header *tar.Header, tarball io.Reader) error {
	// only handle files (directories are implicit)
	if header.Typeflag == tar.TypeReg {
		fmt.Printf("  tar: extracting file: %s\n", header.Name)

		// special case - repositories file
		if filepath.Base(header.Name) == "repositories" {
			if err := createRepositoriesJsonFile(root, tarball); err != nil {
				return err
			}

		} else {
			barename := strings.TrimPrefix(header.Name, "./")

			dest := filepath.Join(root, "images", barename)
			if err := os.MkdirAll(filepath.Dir(dest), os.ModeDir|0700); err != nil {
				return err
			}

			destFile, err := os.Create(dest)
			if err != nil {
				return err
			}

			if wrote, err := io.Copy(destFile, tarball); err != nil {
				return err
			} else {
				fmt.Printf("  tar: file created. Size: %s\n", utils.HumanSize(wrote))
			}

			destFile.Close()
		}
	}

	return nil
}

func (cli *DogestryCli) exportMetaDataToFiles(repoName string, repoTag string, id remote.ID, root string) error {
	fmt.Printf("Exporting metadata for: %v to: %v\n", repoName, root)
	dest := filepath.Join(root, "repositories", repoName, repoTag)

	if err := os.MkdirAll(filepath.Dir(dest), os.ModeDir|0700); err != nil {
		return err
	}

	if err := ioutil.WriteFile(dest, []byte(id), 0600); err != nil {
		return err
	}
	return nil
}

// ---------------------------------
// AzureRemote.Push
// ---------------------------------

func (remote *AzureRemote) Push(image, imageRoot string) error {
	var err error

	keysToPush, err := remote.localKeys(imageRoot)
	if err != nil {
		return fmt.Errorf("error calculating keys to push: %v", err)
	}

	if len(keysToPush) == 0 {
		log.Println("There are no files to push")
		return nil
	}

	type putFileResult struct {
		host string
		err  error
	}

	putFileErrChan := make(chan putFileResult)
	putFilesChan := remote.makeAzFilesChan(keysToPush)

	defer close(putFileErrChan)

	numGoroutines := 25
	goroutineQuitChans := make([]chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		goroutineQuitChans[i] = make(chan bool)
	}

	println("Pushing files to Azure remote:")
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			select {
			case <-goroutineQuitChans[i]:
				return
			default:
				for putFile := range putFilesChan {
					putFileErr := remote.putFile(putFile.KeyDef.fullPath, &putFile.KeyDef)

					if (putFileErr != nil) && ((putFileErr != io.EOF) && (!strings.Contains(putFileErr.Error(), "EOF"))) {
						putFileErrChan <- putFileResult{putFile.Key, putFileErr}
						return
					}

					putFileErrChan <- putFileResult{}
				}
			}
		}(i)
	}

	for i := 0; i < len(keysToPush); i++ {
		p := <-putFileErrChan
		if p.err != nil {
			// Close all running goroutines
			for i := 0; i < numGoroutines; i++ {
				select {
				case goroutineQuitChans[i] <- true:
				default:
				}
			}

			log.Printf("error when uploading to Azure: %v", p.err)
			return fmt.Errorf("Error when uploading to Azure: %v", p.err)
		}
	}

	return nil
}

type azPutFileTuple struct {
	Key    string
	KeyDef azKeyDef
}

func (remote *AzureRemote) makeAzFilesChan(keysToPush azKeys) <-chan azPutFileTuple {
	putFilesChan := make(chan azPutFileTuple, len(keysToPush))
	go func() {
		defer close(putFilesChan)
		for key, localKey := range keysToPush {
			keyDefClone := *localKey
			putFilesChan <- azPutFileTuple{key, keyDefClone}
		}
	}()
	return putFilesChan
}


// put a file with key from imageRoot to the s3 bucket
func (remote *AzureRemote) putFile(src string, key *azKeyDef) error {
	dstKey := key.key

	blob := remote.config.Azure.Blob

	if blob.PathPresent {
		dstKey = fmt.Sprintf("%s/%s", blob.Path, dstKey)
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}

	defer f.Close()

	service, err := remote.azureBlobClient()
	if err != nil {
		return err
	}

	// Create the block, if it doesn't exist
	// Copy to Azure Blob Storage
	blocks, err := remote.putAzureBlocks(service, f, blob, dstKey)
	if err != nil {
		return err
	}

	if len(blocks) > 0 {
		err = service.PutBlockList(blob.Container, dstKey, blocks)
		if err != nil {
			return err
		}
	}

	return nil
}

const maxBlockSize int64 = 4000000

func (remote *AzureRemote) putAzureBlocks(svc *storage.BlobStorageClient, f *os.File, blob *config.BlobSpec, dst string) ([]storage.Block, error) {
	arr := make([]byte, maxBlockSize)

	firstId, nBlocks := remote.firstBlockId(f)
	id := firstId
	blocks := make([]storage.Block, nBlocks)

	var err error = nil

	for n, e := f.Read(arr); n > 0 && (e == nil || e == io.EOF); n, e = f.Read(arr) {
		strId := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(id)))

		putErr := svc.PutBlock(blob.Container, dst, strId, arr[:n])

		if putErr != nil {
			// We could re-try the block under certain conditions
			// Note that, because we haven't committed any blocks,
			// we don't have to worry about partial uploads
			return nil, putErr
		}

		blocks[id-firstId] = storage.Block{strId, storage.BlockStatusUncommitted}
		err = e

		id++
	}

	if err != nil && err != io.EOF {
		return nil, err
	}

	if nBlocks == 1 && id == 10 {
		// Create an empty one and break
		if createErr := svc.CreateBlockBlob(blob.Container, dst); createErr != nil {
			return nil, createErr
		}

		return blocks, nil
	}

	return blocks, nil
}

// Return a number n with 1 in the greatest place value, such that
// numBlocks < n < 10 * numBlocks
// This allows us to use IDs in the range [n, n+numBlocks)
// without the number of digits changing.  This is a requirement
// for azure block blob uploads
func (remote *AzureRemote) firstBlockId(f *os.File) (firstId, numBlocks int) {
	fi, _ := f.Stat()

	numBlocks = int((fi.Size() / maxBlockSize) + 1)

	s := strconv.Itoa(numBlocks * 10)

	var buf bytes.Buffer

	buf.WriteString("1")

	for i := 1; i < len(s); i++ {
		buf.WriteString("0")
	}

	id, _ := strconv.ParseInt(buf.String(), 10, 0)

	firstId = int(id)
	return
}