package azdockertool

import (
	docker "github.com/fsouza/go-dockerclient"
	"os"
	"os/exec"
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

// streams exported tarball into remote docker host
func DockerLoad(client *docker.Client, src string) error {
	cmd := exec.Command("tar", "cvf", "-", "-C", src, ".")
	cmd.Env = os.Environ()
	cmd.Dir = src
	defer cmd.Wait()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	err = client.LoadImage(docker.LoadImageOptions{InputStream: stdout})
	if err != nil {
		return err
	}

	return nil
}


