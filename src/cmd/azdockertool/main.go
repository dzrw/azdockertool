package main

import (
	lib "europium.io/x/azdockertool"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/docopt/docopt-go"
	"math/rand"
	"os"
	"text/tabwriter"
	"time"
)

const (
	ProgramName    string = "azdockertool"
	ProgramVersion string = "azdockertool version 0.0.1"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	err := doit()
	if err != nil {
		panic(err)
	}
}

func doit() (err error) {
	res, err := usage(os.Args[1:])
	if err != nil {
		usage([]string{ProgramName, "--help"})
		os.Exit(1)
	}

	// load config
	environment := res["-e"].(string)
	verbose := res["-v"].(bool)

	conf, err := lib.GetConfig(environment, verbose)
	if err != nil {
		return err
	}

	if conf.Verbose {
		fmt.Printf("---\n")
		fmt.Printf("loaded environment '%s'\n", environment)
		fmt.Printf("using account %s\n", conf.AccountName)
		fmt.Printf("using container %s\n", conf.Container)
	}

	// dispatch images
	if res["images"].(bool) {
		if conf.Verbose {
			fmt.Printf("enumerating images\n")
		}

		images(conf)
		return nil
	}

	// dispatch push
	if res["push"].(bool) {
		image := res["<image>"].(string)

		if conf.Verbose {
			fmt.Printf("pushing image '%s'\n", image)
		}

		push(conf, image)
		return nil
	}

	// dispatch pull
	if res["pull"].(bool) {

		image := res["<image>"].(string)

		if conf.Verbose {
			fmt.Printf("pulling image '%s'\n", image)
		}

		pull(conf, image)
		return nil
	}

	// dispatch layers
	if res["layers"].(bool) {
		fmt.Println("layers is not yet implemented")
		os.Exit(2)
	}

	// dispatch rmi
	if res["rmi"].(bool) {
		fmt.Println("rmi is not yet implemented")
		os.Exit(2)
	}

	// dispatch tree
	// if res["tree"].(bool) {
	// 	cmd := &azb.SimpleCommand{
	// 		Config:     conf,
	// 		Command:    "tree",
	// 		OutputMode: mode,
	// 	}

	// 	src, err := blobSpec(res, "<container>", false)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	cmd.Source = src

	// 	err = cmd.Dispatch()
	// 	if err == azb.ErrContainerNotFound {
	// 		fmt.Println("azb tree: No such container")
	// 		os.Exit(1)
	// 	} else if err == azb.ErrUnrecognizedCommand {
	// 		fmt.Println("azb tree: unexpected arguments")
	// 		os.Exit(1)
	// 	} else if err != nil {
	// 		return err
	// 	}
	// }

	// // dispatch get
	// if res["get"].(bool) {
	// 	cmd := &azb.SimpleCommand{
	// 		Config:     conf,
	// 		Command:    "get",
	// 		OutputMode: mode,
	// 	}

	// 	src, err := blobSpec(res, "<blobpath>", true)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	cmd.Source = src

	// 	if dst, ok := res["<dst>"].(string); ok {
	// 		cmd.LocalPath = dst
	// 	} else {
	// 		cmd.LocalPath = ""
	// 	}

	// 	err = cmd.Dispatch()
	// 	if err == azb.ErrBlobNotFound {
	// 		fmt.Println("azb get: blob not found")
	// 		os.Exit(1)
	// 	} else if err == azb.ErrContainerNotFound {
	// 		fmt.Println("azb get: container not found")
	// 		os.Exit(2)
	// 	} else if err != nil {
	// 		return err
	// 	}
	// }

	// // dispatch rm
	// if res["rm"].(bool) {
	// 	cmd := &azb.SimpleCommand{
	// 		Config:     conf,
	// 		Command:    "rm",
	// 		OutputMode: mode,
	// 	}

	// 	if res["-f"].(bool) {
	// 		cmd.Destructive = true
	// 	}

	// 	src, err := blobSpec(res, "<blobpath>", true)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	cmd.Source = src

	// 	err = cmd.Dispatch()
	// 	if err == azb.ErrContainerOrBlobNotFound {
	// 		fmt.Println("azb rm: No such container or blob")
	// 		os.Exit(1)
	// 	} else if err != nil {
	// 		return err
	// 	}
	// }

	// if res["put"].(bool) {
	// 	cmd := &azb.SimpleCommand{
	// 		Config:     conf,
	// 		Command:    "put",
	// 		OutputMode: mode,
	// 	}

	// 	dst, err := blobSpec(res, "<blobpath>", false)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	cmd.Destination = dst

	// 	if path, ok := res["<src>"].(string); ok {
	// 		cmd.LocalPath = path
	// 	} else {
	// 		cmd.LocalPath = fmt.Sprintf("%s/%s", dst.Container, dst.Path)
	// 	}

	// 	err = cmd.Dispatch()
	// 	if err == azb.ErrContainerOrBlobNotFound {
	// 		fmt.Println("azb put: No such container or blob")
	// 		os.Exit(1)
	// 	} else if err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func usage(argv []string) (map[string]interface{}, error) {
	usage := `azdockertool - reads and writes Docker images to Azure Blob Storage

Usage:
  azdockertool [ -v ] [ -e environment ] images
  azdockertool [ -v ] [ -e environment ] push <image>
  azdockertool [ -v ] [ -e environment ] pull <image>
  azdockertool [ -v ] [ -e environment ] layers [ --graphviz ]
  azdockertool [ -v ] [ -e environment ] rmi <image>
  azdockertool -h | --help
  azdockertool --version

Arguments:
  image 			The name of a Docker image; optionally may specify a tag (e.g. docker/helloworld:1.0)

Options:
  -e environment    Specifies the Azure Storage Services account to use [default: default]
  -h, --help     	Show this screen.
  --version     	Show version.

The most commonly used commands are:
   images      	Lists remote images
   pull			Retrieves an image from storage
   push			Publishes an image to storage

Environment configurations are loaded from ~/.azdockertool.toml.
`

	dict, err := docopt.Parse(usage, argv, true, ProgramVersion, false)
	if err != nil {
		return nil, err
	}

	fmt.Println("cmdRoot says:")
	fmt.Printf("dict= %v\n", dict)

	return dict, err
}

// lists remote images
func images(config *lib.Config) {
	remote, err := lib.NewAzureBlobStorageRemote(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	images, err := remote.Images()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if config.Verbose {
		fmt.Println("---\n")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintf(w, "REPOSITORY\tTAG\tIMAGE ID\tLAST MODIFIED\n")

	for _, i := range images {
		line := fmt.Sprintf("%s\t%s\t%s\t%s", i.Repository, i.Tag, i.Id.Short(), i.LastModified.Format(time.RFC822))
		fmt.Fprintln(w, line)
	}
}

// exports a local image:tag to Azure Blob Storage
func push(config *lib.Config, image string) {
	client, err := lib.NewDockerClient(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	exporter := func(dir, repository string) error {
		return lib.DockerSave(client, repository, dir)
	}

	remote, err := lib.NewAzureBlobStorageRemote(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// figure out where to stage the uploads
	localStorage, err := lib.NewLocalStorage()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// FIXME: Uncomment before using a lot.
	//defer localStorage.Dispose()

	// upload the individual layers to Azure blob Storage
	_, err = remote.Push(image, exporter, localStorage)
	if err != nil {
		log.WithFields(log.Fields{
			"image":  image,
			"reason": err.Error(),
		}).Error("could not publish image")
		os.Exit(1)
		return
	}
}

// pulls a remote image:tag into the local Docker daemon
func pull(config *lib.Config, image string) {
	client, err := lib.NewDockerClient(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// nautical!
	skipper := func(id lib.ID) (bool, error) {
		ok, err := lib.DockerImageExists(client, id)
		if ok && err != nil {
			fmt.Printf("Docker host already has id '%s', stop scanning.\n", id.Short())
		}

		return ok, err
	}

	remote, err := lib.NewAzureBlobStorageRemote(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// figure out where to put the downloads
	localStorage, err := lib.NewLocalStorage()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// FIXME: Uncomment before using a lot.
	//defer localStorage.Dispose()

	res, err := remote.Pull(image, skipper, localStorage)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//fmt.Println("Go debug this in bash and see if it works first")

	fmt.Printf("Importing image(%s) TAR file to docker host\n", res.Id.Short())

	// Seriously; the original code has a "placebo" progress bar.
	fmt.Println("Please be patient, this may take a while.  Instead of treating you like a child and showing a fake progress bar, we're just asking you to chill.")

	err = lib.DockerLoad(client, res.Src)
	if err != nil {
		fmt.Println(err)
		return
	}
}
