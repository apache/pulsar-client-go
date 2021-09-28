// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

// Packages is admin interface for functions management
type Packages interface {
	// Download Function/Connector Package
	// @param destinationFile
	//        file where data should be downloaded to
	// @param packageURL
	//        the package URL
	Download(packageURL, destinationFile string) error

	// Upload Function/Connector Package
	// @param filePath
	//        file where data should be uploaded to
	// @param packageURL
	//        type://tenant/namespace/packageName@version
	// @param description
	//        descriptions of a package
	// @param contact
	//        contact information of a package
	// @param properties
	// 		  external infromations of a package
	Upload(packageURL, filePath, description, contact string, properties map[string]string) error

	// List all the packages with the given type in a namespace
	List(typeName, namespace string) ([]string, error)

	// ListVersions list all the versions of a package
	ListVersions(packageURL string) ([]string, error)

	// Delete the specified package
	Delete(packageURL string) error

	// GetMetadata get a package metadata information
	GetMetadata(packageURL string) (utils.PackageMetadata, error)

	// UpdateMetadata update a package metadata information
	UpdateMetadata(packageURL, description, contact string, properties map[string]string) error
}

type packages struct {
	pulsar   *pulsarClient
	basePath string
}

func (p *packages) createStringFromField(w *multipart.Writer, value string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s" `, value))
	h.Set("Content-Type", "application/json")
	return w.CreatePart(h)
}

// Packages is used to access the functions endpoints
func (c *pulsarClient) Packages() Packages {
	return &packages{
		pulsar:   c,
		basePath: "/packages",
	}
}

func (p packages) Download(packageURL, destinationFile string) error {
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion())
	_, err = os.Open(destinationFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("file %s already exists, please delete "+
				"the file first or change the file name", destinationFile)
		}
	}
	file, err := os.Create(destinationFile)
	if err != nil {
		return err
	}

	_, err = p.pulsar.Client.GetWithOptions(endpoint, nil, nil, false, file)
	if err != nil {
		return err
	}
	return nil
}

func (p packages) Upload(packageURL, filePath, description, contact string, properties map[string]string) error {
	if strings.TrimSpace(filePath) == "" {
		return errors.New("file path is empty")
	}
	if strings.TrimSpace(packageURL) == "" {
		return errors.New("package URL is empty")
	}
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion())
	metadata := utils.PackageMetadata{
		Description: description,
		Contact:     contact,
		Properties:  properties,
	}
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	stringWriter, err := p.createStringFromField(multiPartWriter, "metadata")
	if err != nil {
		return err
	}

	_, err = stringWriter.Write(metadataJSON)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	part, err := multiPartWriter.CreateFormFile("file", filepath.Base(file.Name()))

	if err != nil {
		return err
	}

	// copy the actual file content to the filed's writer
	_, err = io.Copy(part, file)
	if err != nil {
		return err
	}

	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()
	err = p.pulsar.Client.PostWithMultiPart(endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (p packages) List(typeName, namespace string) ([]string, error) {
	var packageList []string
	endpoint := p.pulsar.endpoint(p.basePath, typeName, namespace)
	err := p.pulsar.Client.Get(endpoint, &packageList)
	return packageList, err
}

func (p packages) ListVersions(packageURL string) ([]string, error) {
	var versionList []string
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return versionList, err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName())
	err = p.pulsar.Client.Get(endpoint, &versionList)
	return versionList, err
}

func (p packages) Delete(packageURL string) error {
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion())

	return p.pulsar.Client.Delete(endpoint)
}

func (p packages) GetMetadata(packageURL string) (utils.PackageMetadata, error) {
	var metadata utils.PackageMetadata
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return metadata, err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion(), "metadata")
	err = p.pulsar.Client.Get(endpoint, &metadata)
	return metadata, err
}

func (p packages) UpdateMetadata(packageURL, description, contact string, properties map[string]string) error {
	metadata := utils.PackageMetadata{
		Description: description,
		Contact:     contact,
		Properties:  properties,
	}
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion(), "metadata")

	return p.pulsar.Client.Put(endpoint, &metadata)
}
