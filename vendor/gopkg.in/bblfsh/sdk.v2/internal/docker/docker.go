package docker

import (
	"io"
	"os"

	"github.com/ory/dockertest/docker"
)

const (
	FileName  = "Dockerfile"
	Socket    = "/var/run/docker.sock"
	SocketURL = "unix://" + Socket
)

func Installed() bool {
	_, err := os.Stat(Socket)
	return err == nil
}

func Dial() (*Client, error) {
	return docker.NewClient(SocketURL)
}

type Client = docker.Client

type Config = docker.Config

type HostConfig = docker.HostConfig
type Container = docker.Container

type BuildImageOptions = docker.BuildImageOptions

type CreateContainerOptions = docker.CreateContainerOptions
type RemoveContainerOptions = docker.RemoveContainerOptions
type AttachToContainerOptions = docker.AttachToContainerOptions

type CreateExecOptions = docker.CreateExecOptions
type StartExecOptions = docker.StartExecOptions

type CloseWaiter = docker.CloseWaiter

func Run(cli *Client, opt CreateContainerOptions) (*Container, error) {
	c, err := cli.CreateContainer(opt)
	if err != nil {
		return nil, err
	}
	err = cli.StartContainer(c.ID, nil)
	if err != nil {
		cli.RemoveContainer(docker.RemoveContainerOptions{
			ID: c.ID, Force: true,
		})
		return nil, err
	}
	return cli.InspectContainer(c.ID)
}

func RunAndWait(cli *Client, stdout, stderr io.Writer, opt CreateContainerOptions) error {
	c, err := cli.CreateContainer(opt)
	if err != nil {
		return err
	}
	defer cli.RemoveContainer(docker.RemoveContainerOptions{
		ID: c.ID, Force: true,
	})

	cw, err := cli.AttachToContainerNonBlocking(docker.AttachToContainerOptions{
		Container:    c.ID,
		OutputStream: stdout, ErrorStream: stderr,
		Stdout: stdout != nil, Stderr: stderr != nil,
		Logs: true, Stream: true,
	})
	if err != nil {
		return err
	}
	defer cw.Close()

	err = cli.StartContainer(c.ID, nil)
	if err != nil {
		return err
	}
	return cw.Wait()
}
