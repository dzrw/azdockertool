package azdockertool

import (
	//log "github.com/Sirupsen/logrus"
	docker "github.com/fsouza/go-dockerclient"
	"os"
	"os/exec"
	"path/filepath"
)

func NewDockerClient(config *Config) (client *docker.Client, err error) {
	if config.Docker.UseTLS {
		d := config.Docker
		client, err = docker.NewTLSClient(d.Host, d.CertPath, d.PrivateKeyPath, d.CaCertPath)
	} else {
		client, err = docker.NewClient(config.Docker.Host)
	}

	return client, err
}

func DockerImageExists(client *docker.Client, id ID) (ok bool, err error) {
	_, err = client.InspectImage(id.String())
	if err == docker.ErrNoSuchImage {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

// loads an image from a tarball
func DockerLoad(client *docker.Client, srcdir string) error {
	cmd := exec.Command("tar", "cvf", "-", "-C", srcdir, ".")
	cmd.Env = os.Environ()
	cmd.Dir = srcdir
	defer cmd.Wait()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	// loads a tarball from stdout into the Docker host
	err = client.LoadImage(docker.LoadImageOptions{InputStream: stdout})
	if err != nil {
		return err
	}

	return nil
}

// exports an (uncompressed) image to disk
func DockerSave(client *docker.Client, repository, dstdir string) error {
	// writes the image to disk
	tarfile := filepath.Join(dstdir, "image.tar")
	cmd0 := exec.Command("docker", "save", "-o", tarfile, repository)
	cmd0.Env = os.Environ()
	cmd0.Dir = dstdir

	if err := cmd0.Run(); err != nil {
		return ErrNoSuchImage
	}

	// expand the tar file
	cmd1 := exec.Command("tar", "xvf", "image.tar", "-C", dstdir)
	cmd1.Env = os.Environ()
	cmd1.Dir = dstdir

	if err := cmd1.Run(); err != nil {
		return err
	}

	return nil
}
