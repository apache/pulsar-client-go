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

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Packages is admin interface for functions management
type Packages interface {
	// Download downloads Function/Connector Package
	// @param destinationFile
	//        file where data should be downloaded to
	// @param packageURL
	//        the package URL
	Download(packageURL, destinationFile string) error

	// DownloadWithContext downloads Function/Connector Package
	// @param ctx
	//        context used for the request
	// @param destinationFile
	//        file where data should be downloaded to
	// @param packageURL
	//        the package URL
	DownloadWithContext(ctx context.Context, packageURL, destinationFile string) error

	// Upload uploads Function/Connector Package
	// @param filePath
	//        file where data should be uploaded to
	// @param packageURL
	//        type://tenant/namespace/packageName@version
	// @param description
	//        descriptions of a package
	// @param contact
	//        contact information of a package
	// @param properties
	// 		  external informations of a package
	Upload(packageURL, filePath, description, contact string, properties map[string]string) error

	// UploadWithContext uploads Function/Connector Package
	// @param ctx
	//        context used for the request
	// @param filePath
	//        file where data should be uploaded to
	// @param packageURL
	//        type://tenant/namespace/packageName@version
	// @param description
	//        descriptions of a package
	// @param contact
	//        contact information of a package
	// @param properties
	// 		  external informations of a package
	UploadWithContext(ctx context.Context, packageURL, filePath, description, contact string, properties map[string]string) error

	// List lists all the packages with the given type in a namespace
	List(typeName, namespace string) ([]string, error)

	// ListWithContext lists all the packages with the given type in a namespace
	ListWithContext(ctx context.Context, typeName, namespace string) ([]string, error)

	// ListVersions lists all the versions of a package
	ListVersions(packageURL string) ([]string, error)

	// ListVersionsWithContext lists all the versions of a package
	ListVersionsWithContext(ctx context.Context, packageURL string) ([]string, error)

	// Delete deletes the specified package
	Delete(packageURL string) error

	// DeleteWithContext deletes the specified package
	DeleteWithContext(ctx context.Context, packageURL string) error

	// GetMetadata returns a package metadata information
	GetMetadata(packageURL string) (utils.PackageMetadata, error)

	// GetMetadataWithContext returns a package metadata information
	GetMetadataWithContext(ctx context.Context, packageURL string) (utils.PackageMetadata, error)

	// UpdateMetadata updates a package metadata information
	UpdateMetadata(packageURL, description, contact string, properties map[string]string) error

	// UpdateMetadataWithContext updates a package metadata information
	UpdateMetadataWithContext(ctx context.Context, packageURL, description, contact string, properties map[string]string) error
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
	return p.DownloadWithContext(context.Background(), packageURL, destinationFile)
}

func (p packages) DownloadWithContext(ctx context.Context, packageURL, destinationFile string) error {
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion())

	parent := path.Dir(destinationFile)
	if parent != "." {
		err = os.MkdirAll(parent, 0755)
		if err != nil {
			return fmt.Errorf("failed to create parent directory %s: %w", parent, err)
		}
	}

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

	_, err = p.pulsar.Client.GetWithOptions(ctx, endpoint, nil, nil, false, file)
	if err != nil {
		return err
	}
	return nil
}

func (p packages) Upload(packageURL, filePath, description, contact string, properties map[string]string) error {
	return p.UploadWithContext(context.Background(), packageURL, filePath, description, contact, properties)
}

func (p packages) UploadWithContext(ctx context.Context, packageURL, filePath, description, contact string, properties map[string]string) error {
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
	err = p.pulsar.Client.PostWithMultiPart(ctx, endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (p packages) List(typeName, namespace string) ([]string, error) {
	return p.ListWithContext(context.Background(), typeName, namespace)
}

func (p packages) ListWithContext(ctx context.Context, typeName, namespace string) ([]string, error) {
	var packageList []string
	endpoint := p.pulsar.endpoint(p.basePath, typeName, namespace)
	err := p.pulsar.Client.Get(ctx, endpoint, &packageList)
	return packageList, err
}

func (p packages) ListVersions(packageURL string) ([]string, error) {
	return p.ListVersionsWithContext(context.Background(), packageURL)
}

func (p packages) ListVersionsWithContext(ctx context.Context, packageURL string) ([]string, error) {
	var versionList []string
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return versionList, err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName())
	err = p.pulsar.Client.Get(ctx, endpoint, &versionList)
	return versionList, err
}

func (p packages) Delete(packageURL string) error {
	return p.DeleteWithContext(context.Background(), packageURL)
}

func (p packages) DeleteWithContext(ctx context.Context, packageURL string) error {
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion())

	return p.pulsar.Client.Delete(ctx, endpoint)
}

func (p packages) GetMetadata(packageURL string) (utils.PackageMetadata, error) {
	return p.GetMetadataWithContext(context.Background(), packageURL)
}

func (p packages) GetMetadataWithContext(ctx context.Context, packageURL string) (utils.PackageMetadata, error) {
	var metadata utils.PackageMetadata
	packageName, err := utils.GetPackageName(packageURL)
	if err != nil {
		return metadata, err
	}
	endpoint := p.pulsar.endpoint(p.basePath, string(packageName.GetType()), packageName.GetTenant(),
		packageName.GetNamespace(), packageName.GetName(), packageName.GetVersion(), "metadata")
	err = p.pulsar.Client.Get(ctx, endpoint, &metadata)
	return metadata, err
}

func (p packages) UpdateMetadata(packageURL, description, contact string, properties map[string]string) error {
	return p.UpdateMetadataWithContext(context.Background(), packageURL, description, contact, properties)
}

func (p packages) UpdateMetadataWithContext(ctx context.Context, packageURL, description, contact string, properties map[string]string) error {
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

	return p.pulsar.Client.Put(ctx, endpoint, &metadata)
}
