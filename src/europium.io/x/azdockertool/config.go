package azdockertool

import (
	"errors"
	"github.com/BurntSushi/toml"
	"os"
	"os/user"
	"path/filepath"
	"fmt"
)

var (
	ErrEnvironmentNotFound = errors.New("undefined environment; check your configuration")
	ErrCannotAccessConfigFile = errors.New("configuration unavailable")
)

const (
	prefab string = `[default]
storage_account_name = "YOUR_STORAGE_ACCOUNT"
storage_account_access_key = "WU9VUl9TVE9SQUdFX0FDQ09VTlRfS0VZCg=="
container = "YOUR_CONTAINER"
`
)

type Config struct {
	AccountName string
	AccountKey string
	Container string
	Verbose bool
	HomeDir string
	Docker *DockerConfig
}

type DockerConfig struct {
	Host string
	UseTLS bool
	CaCertPath string
	CertPath string
	PrivateKeyPath string
}

func GetConfig(environment string, verbose bool) (*Config, error) {

	usr, err := user.Current()
	if err != nil {
		return nil, errors.New("cannot get homedir")
	}

	configFile, err := ensureConfigFileExists(usr.HomeDir, verbose)
	if err != nil {
		return nil, err
	}

	type envInfo struct {
		AccountName      string `toml:"storage_account_name"`
		AccountKey string `toml:"storage_account_access_key"`
		Container string `toml:"container"`
	}

	var config map[string]envInfo
	if _, err := toml.DecodeFile(configFile, &config); err != nil {
		return nil, err
	}

	env, ok := config[environment]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	cfg := &Config{
		AccountName: env.AccountName, 
		AccountKey: env.AccountKey, 
		Container: env.Container, 
		Verbose: verbose, 
		HomeDir: usr.HomeDir,
		Docker: getDockerConfig(usr.HomeDir),
	}

	return cfg, nil
}


func ensureConfigFileExists(homedir string, verbose bool) (string, error) {
	path := filepath.Join(homedir, ".azdockertool.toml")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if verbose {
			fmt.Printf("writing new configuration to '%s'\n", path)
		}

		f, err := os.Create(path)
		if err != nil {
			return "", err
		}

		defer f.Close()

		_, err = f.WriteString(prefab)
		if err != nil {
			return "", err
		}
	}

	return path, nil
}

func getDockerConfig(homedir string) *DockerConfig {
	host := os.Getenv("DOCKER_HOST")
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}

    certdir := os.Getenv("DOCKER_CERT_PATH")
	if certdir == "" {
		certdir = filepath.Join(homedir, ".docker")
	}

	ca := filepath.Join(certdir, "ca.pem")
	cert := filepath.Join(certdir, "cert.pem")
	priv := filepath.Join(certdir, "key.pem")

	if allPathsExist([]string{ca, cert, priv}) {
		return &DockerConfig{
			Host: host,
			UseTLS: true, 
			CaCertPath: ca,
			CertPath: cert,
			PrivateKeyPath: priv,
		}
	}

	return &DockerConfig{Host: host}
}

func allPathsExist(coll []string) bool {
	for _, path := range coll {
		_, err := os.Stat(path)
		if err != nil {
			return false
		}
	} 

	return true
}