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
	"path/filepath"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Functions is admin interface for functions management
type Functions interface {
	// CreateFunc create a new function.
	CreateFunc(data *utils.FunctionConfig, fileName string) error

	// CreateFuncWithContext create a new function.
	CreateFuncWithContext(ctx context.Context, data *utils.FunctionConfig, fileName string) error

	// CreateFuncWithURL creates a new function by providing url from which fun-pkg can be downloaded.
	// supported url: http/file
	// eg:
	//  File: file:/dir/fileName.jar
	//  Http: http://www.repo.com/fileName.jar
	//
	// @param functionConfig
	//      the function configuration object
	// @param pkgURL
	//      url from which pkg can be downloaded
	CreateFuncWithURL(data *utils.FunctionConfig, pkgURL string) error

	// CreateFuncWithURLWithContext creates a new function by providing url from which fun-pkg can be downloaded.
	// supported url: http/file
	// eg:
	//  File: file:/dir/fileName.jar
	//  Http: http://www.repo.com/fileName.jar
	//
	// @param ctx
	//        context used for the request
	// @param functionConfig
	//      the function configuration object
	// @param pkgURL
	//      url from which pkg can be downloaded
	CreateFuncWithURLWithContext(ctx context.Context, data *utils.FunctionConfig, pkgURL string) error

	// StopFunction stops all function instances
	StopFunction(tenant, namespace, name string) error

	// StopFunctionWithContext stops all function instances
	StopFunctionWithContext(ctx context.Context, tenant, namespace, name string) error

	// StopFunctionWithID stops function instance
	StopFunctionWithID(tenant, namespace, name string, instanceID int) error

	// StopFunctionWithIDWithContext stops function instance
	StopFunctionWithIDWithContext(ctx context.Context, tenant, namespace, name string, instanceID int) error

	// DeleteFunction deletes an existing function
	DeleteFunction(tenant, namespace, name string) error

	// DeleteFunctionWithContext deletes an existing function
	DeleteFunctionWithContext(ctx context.Context, tenant, namespace, name string) error

	// Download Function Code
	// @param destinationFile
	//        file where data should be downloaded to
	// @param path
	//        Path where data is located
	DownloadFunction(path, destinationFile string) error

	// Download Function Code
	// @param ctx
	//        context used for the request
	// @param destinationFile
	//        file where data should be downloaded to
	// @param path
	//        Path where data is located
	DownloadFunctionWithContext(ctx context.Context, path, destinationFile string) error

	// Download Function Code
	// @param destinationFile
	//        file where data should be downloaded to
	// @param tenant
	//        Tenant name
	// @param namespace
	//        Namespace name
	// @param function
	//        Function name
	DownloadFunctionByNs(destinationFile, tenant, namespace, function string) error

	// Download Function Code
	// @param ctx
	//        context used for the request
	// @param destinationFile
	//        file where data should be downloaded to
	// @param tenant
	//        Tenant name
	// @param namespace
	//        Namespace name
	// @param function
	//        Function name
	DownloadFunctionByNsWithContext(ctx context.Context, destinationFile, tenant, namespace, function string) error

	// StartFunction starts all function instances
	StartFunction(tenant, namespace, name string) error

	// StartFunctionWithContext starts all function instances
	StartFunctionWithContext(ctx context.Context, tenant, namespace, name string) error

	// StartFunctionWithID starts function instance
	StartFunctionWithID(tenant, namespace, name string, instanceID int) error

	// StartFunctionWithIDWithContext starts function instance
	StartFunctionWithIDWithContext(ctx context.Context, tenant, namespace, name string, instanceID int) error

	// RestartFunction restart all function instances
	RestartFunction(tenant, namespace, name string) error

	// RestartFunctionWithContext restart all function instances
	RestartFunctionWithContext(ctx context.Context, tenant, namespace, name string) error

	// RestartFunctionWithID restart function instance
	RestartFunctionWithID(tenant, namespace, name string, instanceID int) error

	// RestartFunctionWithIDWithContext restart function instance
	RestartFunctionWithIDWithContext(ctx context.Context, tenant, namespace, name string, instanceID int) error

	// GetFunctions returns the list of functions
	GetFunctions(tenant, namespace string) ([]string, error)

	// GetFunctionsWithContext returns the list of functions
	GetFunctionsWithContext(ctx context.Context, tenant, namespace string) ([]string, error)

	// GetFunction returns the configuration for the specified function
	GetFunction(tenant, namespace, name string) (utils.FunctionConfig, error)

	// GetFunctionWithContext returns the configuration for the specified function
	GetFunctionWithContext(ctx context.Context, tenant, namespace, name string) (utils.FunctionConfig, error)

	// GetFunctionStatus returns the current status of a function
	GetFunctionStatus(tenant, namespace, name string) (utils.FunctionStatus, error)

	// GetFunctionStatusWithContext returns the current status of a function
	GetFunctionStatusWithContext(ctx context.Context, tenant, namespace, name string) (utils.FunctionStatus, error)

	// GetFunctionStatusWithInstanceID returns the current status of a function instance
	GetFunctionStatusWithInstanceID(tenant, namespace, name string, instanceID int) (
		utils.FunctionInstanceStatusData, error)

	// GetFunctionStatusWithInstanceIDWithContext returns the current status of a function instance
	GetFunctionStatusWithInstanceIDWithContext(ctx context.Context, tenant, namespace, name string, instanceID int) (
		utils.FunctionInstanceStatusData, error)

	// GetFunctionStats returns the current stats of a function
	GetFunctionStats(tenant, namespace, name string) (utils.FunctionStats, error)

	// GetFunctionStatsWithContext returns the current stats of a function
	GetFunctionStatsWithContext(ctx context.Context, tenant, namespace, name string) (utils.FunctionStats, error)

	// GetFunctionStatsWithInstanceID gets the current stats of a function instance
	GetFunctionStatsWithInstanceID(tenant, namespace, name string, instanceID int) (utils.FunctionInstanceStatsData, error)

	// GetFunctionStatsWithInstanceIDWithContext gets the current stats of a function instance
	GetFunctionStatsWithInstanceIDWithContext(
		ctx context.Context,
		tenant,
		namespace,
		name string,
		instanceID int,
	) (utils.FunctionInstanceStatsData, error)

	// GetFunctionState fetches the current state associated with a Pulsar Function
	//
	// Response Example:
	// 		{ "value : 12, version : 2"}
	GetFunctionState(tenant, namespace, name, key string) (utils.FunctionState, error)

	// GetFunctionStateWithContext fetches the current state associated with a Pulsar Function
	//
	// Response Example:
	// 		{ "value : 12, version : 2"}
	GetFunctionStateWithContext(ctx context.Context, tenant, namespace, name, key string) (utils.FunctionState, error)

	// PutFunctionState puts the given state associated with a Pulsar Function
	PutFunctionState(tenant, namespace, name string, state utils.FunctionState) error

	// PutFunctionStateWithContext puts the given state associated with a Pulsar Function
	PutFunctionStateWithContext(ctx context.Context, tenant, namespace, name string, state utils.FunctionState) error

	// TriggerFunction triggers the function by writing to the input topic
	TriggerFunction(tenant, namespace, name, topic, triggerValue, triggerFile string) (string, error)

	// TriggerFunctionWithContext triggers the function by writing to the input topic
	TriggerFunctionWithContext(
		ctx context.Context,
		tenant,
		namespace,
		name,
		topic,
		triggerValue,
		triggerFile string,
	) (string, error)

	// UpdateFunction updates the configuration for a function.
	UpdateFunction(functionConfig *utils.FunctionConfig, fileName string, updateOptions *utils.UpdateOptions) error

	// UpdateFunctionWithContext updates the configuration for a function.
	UpdateFunctionWithContext(
		ctx context.Context,
		functionConfig *utils.FunctionConfig,
		fileName string,
		updateOptions *utils.UpdateOptions,
	) error

	// UpdateFunctionWithURL updates the configuration for a function.
	//
	// Update a function by providing url from which fun-pkg can be downloaded. supported url: http/file
	// eg:
	// File: file:/dir/fileName.jar
	// Http: http://www.repo.com/fileName.jar
	UpdateFunctionWithURL(functionConfig *utils.FunctionConfig, pkgURL string, updateOptions *utils.UpdateOptions) error

	// UpdateFunctionWithURLWithContext updates the configuration for a function.
	//
	// Update a function by providing url from which fun-pkg can be downloaded. supported url: http/file
	// eg:
	// File: file:/dir/fileName.jar
	// Http: http://www.repo.com/fileName.jar
	UpdateFunctionWithURLWithContext(
		ctx context.Context,
		functionConfig *utils.FunctionConfig,
		pkgURL string,
		updateOptions *utils.UpdateOptions,
	) error

	// Upload function to Pulsar
	Upload(sourceFile, path string) error

	// UploadWithContext function to Pulsar
	UploadWithContext(ctx context.Context, sourceFile, path string) error
}

type functions struct {
	pulsar   *pulsarClient
	basePath string
}

// Functions is used to access the functions endpoints
func (c *pulsarClient) Functions() Functions {
	return &functions{
		pulsar:   c,
		basePath: "/functions",
	}
}

func (f *functions) createStringFromField(w *multipart.Writer, value string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s" `, value))
	h.Set("Content-Type", "application/json")
	return w.CreatePart(h)
}

func (f *functions) createTextFromFiled(w *multipart.Writer, value string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s" `, value))
	h.Set("Content-Type", "text/plain")
	return w.CreatePart(h)
}

func (f *functions) CreateFunc(funcConf *utils.FunctionConfig, fileName string) error {
	return f.CreateFuncWithContext(context.Background(), funcConf, fileName)
}

func (f *functions) CreateFuncWithContext(ctx context.Context, funcConf *utils.FunctionConfig, fileName string) error {
	endpoint := f.pulsar.endpoint(f.basePath, funcConf.Tenant, funcConf.Namespace, funcConf.Name)

	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	jsonData, err := json.Marshal(funcConf)
	if err != nil {
		return err
	}

	stringWriter, err := f.createStringFromField(multiPartWriter, "functionConfig")
	if err != nil {
		return err
	}

	_, err = stringWriter.Write(jsonData)
	if err != nil {
		return err
	}

	if fileName != "" && !strings.HasPrefix(fileName, "builtin://") {
		// If the function code is built in, we don't need to submit here
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer file.Close()

		part, err := multiPartWriter.CreateFormFile("data", filepath.Base(file.Name()))

		if err != nil {
			return err
		}

		// copy the actual file content to the filed's writer
		_, err = io.Copy(part, file)
		if err != nil {
			return err
		}
	}

	// In here, we completed adding the file and the fields, let's close the multipart writer
	// So it writes the ending boundary
	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()
	err = f.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (f *functions) CreateFuncWithURL(funcConf *utils.FunctionConfig, pkgURL string) error {
	return f.CreateFuncWithURLWithContext(context.Background(), funcConf, pkgURL)
}

func (f *functions) CreateFuncWithURLWithContext(
	ctx context.Context,
	funcConf *utils.FunctionConfig,
	pkgURL string,
) error {
	endpoint := f.pulsar.endpoint(f.basePath, funcConf.Tenant, funcConf.Namespace, funcConf.Name)
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	textWriter, err := f.createTextFromFiled(multiPartWriter, "url")
	if err != nil {
		return err
	}

	_, err = textWriter.Write([]byte(pkgURL))
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(funcConf)
	if err != nil {
		return err
	}

	stringWriter, err := f.createStringFromField(multiPartWriter, "functionConfig")
	if err != nil {
		return err
	}

	_, err = stringWriter.Write(jsonData)
	if err != nil {
		return err
	}

	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()
	err = f.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (f *functions) StopFunction(tenant, namespace, name string) error {
	return f.StopFunctionWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) StopFunctionWithContext(ctx context.Context, tenant, namespace, name string) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/stop", nil)
}

func (f *functions) StopFunctionWithID(tenant, namespace, name string, instanceID int) error {
	return f.StopFunctionWithIDWithContext(context.Background(), tenant, namespace, name, instanceID)
}

func (f *functions) StopFunctionWithIDWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
	instanceID int,
) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, id)

	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/stop", nil)
}

func (f *functions) DeleteFunction(tenant, namespace, name string) error {
	return f.DeleteFunctionWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) DeleteFunctionWithContext(ctx context.Context, tenant, namespace, name string) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	return f.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (f *functions) DownloadFunction(path, destinationFile string) error {
	return f.DownloadFunctionWithContext(context.Background(), path, destinationFile)
}

func (f *functions) DownloadFunctionWithContext(ctx context.Context, path, destinationFile string) error {
	endpoint := f.pulsar.endpoint(f.basePath, "download")
	_, err := os.Open(destinationFile)
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

	tmpMap := make(map[string]string)
	tmpMap["path"] = path

	_, err = f.pulsar.Client.GetWithOptionsWithContext(ctx, endpoint, nil, tmpMap, false, file)
	if err != nil {
		return err
	}
	return nil
}

func (f *functions) DownloadFunctionByNs(destinationFile, tenant, namespace, function string) error {
	return f.DownloadFunctionByNsWithContext(context.Background(), destinationFile, tenant, namespace, function)
}

func (f *functions) DownloadFunctionByNsWithContext(
	ctx context.Context,
	destinationFile,
	tenant,
	namespace,
	function string,
) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, function, "download")
	_, err := os.Open(destinationFile)
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

	_, err = f.pulsar.Client.GetWithOptionsWithContext(ctx, endpoint, nil, nil, false, file)
	if err != nil {
		return err
	}

	return nil
}

func (f *functions) StartFunction(tenant, namespace, name string) error {
	return f.StartFunctionWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) StartFunctionWithContext(ctx context.Context, tenant, namespace, name string) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/start", nil)
}

func (f *functions) StartFunctionWithID(tenant, namespace, name string, instanceID int) error {
	return f.StartFunctionWithIDWithContext(context.Background(), tenant, namespace, name, instanceID)
}

func (f *functions) StartFunctionWithIDWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
	instanceID int,
) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, id)

	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/start", nil)
}

func (f *functions) RestartFunction(tenant, namespace, name string) error {
	return f.RestartFunctionWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) RestartFunctionWithContext(ctx context.Context, tenant, namespace, name string) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/restart", nil)
}

func (f *functions) RestartFunctionWithID(tenant, namespace, name string, instanceID int) error {
	return f.RestartFunctionWithIDWithContext(context.Background(), tenant, namespace, name, instanceID)
}

func (f *functions) RestartFunctionWithIDWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
	instanceID int,
) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, id)

	return f.pulsar.Client.PostWithContext(ctx, endpoint+"/restart", nil)
}

func (f *functions) GetFunctions(tenant, namespace string) ([]string, error) {
	return f.GetFunctionsWithContext(context.Background(), tenant, namespace)
}

func (f *functions) GetFunctionsWithContext(ctx context.Context, tenant, namespace string) ([]string, error) {
	var functions []string
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint, &functions)
	return functions, err
}

func (f *functions) GetFunction(tenant, namespace, name string) (utils.FunctionConfig, error) {
	return f.GetFunctionWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) GetFunctionWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
) (utils.FunctionConfig, error) {
	var functionConfig utils.FunctionConfig
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint, &functionConfig)
	return functionConfig, err
}

func (f *functions) UpdateFunction(functionConfig *utils.FunctionConfig, fileName string,
	updateOptions *utils.UpdateOptions) error {
	return f.UpdateFunctionWithContext(context.Background(), functionConfig, fileName, updateOptions)
}

func (f *functions) UpdateFunctionWithContext(
	ctx context.Context,
	functionConfig *utils.FunctionConfig,
	fileName string,
	updateOptions *utils.UpdateOptions,
) error {
	endpoint := f.pulsar.endpoint(f.basePath, functionConfig.Tenant, functionConfig.Namespace, functionConfig.Name)
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	jsonData, err := json.Marshal(functionConfig)
	if err != nil {
		return err
	}

	stringWriter, err := f.createStringFromField(multiPartWriter, "functionConfig")
	if err != nil {
		return err
	}

	_, err = stringWriter.Write(jsonData)
	if err != nil {
		return err
	}

	if updateOptions != nil {
		updateData, err := json.Marshal(updateOptions)
		if err != nil {
			return err
		}

		updateStrWriter, err := f.createStringFromField(multiPartWriter, "updateOptions")
		if err != nil {
			return err
		}

		_, err = updateStrWriter.Write(updateData)
		if err != nil {
			return err
		}
	}

	if fileName != "" && !strings.HasPrefix(fileName, "builtin://") {
		// If the function code is built in, we don't need to submit here
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer file.Close()

		part, err := multiPartWriter.CreateFormFile("data", filepath.Base(file.Name()))

		if err != nil {
			return err
		}

		// copy the actual file content to the filed's writer
		_, err = io.Copy(part, file)
		if err != nil {
			return err
		}
	}

	// In here, we completed adding the file and the fields, let's close the multipart writer
	// So it writes the ending boundary
	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()
	err = f.pulsar.Client.PutWithMultiPart(ctx, endpoint, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (f *functions) UpdateFunctionWithURL(functionConfig *utils.FunctionConfig, pkgURL string,
	updateOptions *utils.UpdateOptions) error {
	return f.UpdateFunctionWithURLWithContext(context.Background(), functionConfig, pkgURL, updateOptions)
}

func (f *functions) UpdateFunctionWithURLWithContext(
	ctx context.Context,
	functionConfig *utils.FunctionConfig,
	pkgURL string,
	updateOptions *utils.UpdateOptions,
) error {
	endpoint := f.pulsar.endpoint(f.basePath, functionConfig.Tenant, functionConfig.Namespace, functionConfig.Name)
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	textWriter, err := f.createTextFromFiled(multiPartWriter, "url")
	if err != nil {
		return err
	}

	_, err = textWriter.Write([]byte(pkgURL))
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(functionConfig)
	if err != nil {
		return err
	}

	stringWriter, err := f.createStringFromField(multiPartWriter, "functionConfig")
	if err != nil {
		return err
	}

	_, err = stringWriter.Write(jsonData)
	if err != nil {
		return err
	}

	if updateOptions != nil {
		updateData, err := json.Marshal(updateOptions)
		if err != nil {
			return err
		}

		updateStrWriter, err := f.createStringFromField(multiPartWriter, "updateOptions")
		if err != nil {
			return err
		}

		_, err = updateStrWriter.Write(updateData)
		if err != nil {
			return err
		}
	}

	// In here, we completed adding the file and the fields, let's close the multipart writer
	// So it writes the ending boundary
	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()
	err = f.pulsar.Client.PutWithMultiPart(ctx, endpoint, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (f *functions) GetFunctionStatus(tenant, namespace, name string) (utils.FunctionStatus, error) {
	return f.GetFunctionStatusWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) GetFunctionStatusWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
) (utils.FunctionStatus, error) {
	var functionStatus utils.FunctionStatus
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint+"/status", &functionStatus)
	return functionStatus, err
}

func (f *functions) GetFunctionStatusWithInstanceID(tenant, namespace, name string,
	instanceID int) (utils.FunctionInstanceStatusData, error) {
	return f.GetFunctionStatusWithInstanceIDWithContext(context.Background(), tenant, namespace, name, instanceID)
}

func (f *functions) GetFunctionStatusWithInstanceIDWithContext(ctx context.Context, tenant, namespace, name string,
	instanceID int) (utils.FunctionInstanceStatusData, error) {
	var functionInstanceStatusData utils.FunctionInstanceStatusData
	id := fmt.Sprintf("%d", instanceID)
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, id)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint+"/status", &functionInstanceStatusData)
	return functionInstanceStatusData, err
}

func (f *functions) GetFunctionStats(tenant, namespace, name string) (utils.FunctionStats, error) {
	return f.GetFunctionStatsWithContext(context.Background(), tenant, namespace, name)
}

func (f *functions) GetFunctionStatsWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
) (utils.FunctionStats, error) {
	var functionStats utils.FunctionStats
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint+"/stats", &functionStats)
	return functionStats, err
}

func (f *functions) GetFunctionStatsWithInstanceID(tenant, namespace, name string,
	instanceID int) (utils.FunctionInstanceStatsData, error) {
	return f.GetFunctionStatsWithInstanceIDWithContext(context.Background(), tenant, namespace, name, instanceID)
}

func (f *functions) GetFunctionStatsWithInstanceIDWithContext(ctx context.Context, tenant, namespace, name string,
	instanceID int) (utils.FunctionInstanceStatsData, error) {
	var functionInstanceStatsData utils.FunctionInstanceStatsData
	id := fmt.Sprintf("%d", instanceID)
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, id)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint+"/stats", &functionInstanceStatsData)
	return functionInstanceStatsData, err
}

func (f *functions) GetFunctionState(tenant, namespace, name, key string) (utils.FunctionState, error) {
	return f.GetFunctionStateWithContext(context.Background(), tenant, namespace, name, key)
}

func (f *functions) GetFunctionStateWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name,
	key string,
) (utils.FunctionState, error) {
	var functionState utils.FunctionState
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, "state", key)
	err := f.pulsar.Client.GetWithContext(ctx, endpoint, &functionState)
	return functionState, err
}

func (f *functions) PutFunctionState(tenant, namespace, name string, state utils.FunctionState) error {
	return f.PutFunctionStateWithContext(context.Background(), tenant, namespace, name, state)
}

func (f *functions) PutFunctionStateWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name string,
	state utils.FunctionState,
) error {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, "state", state.Key)

	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	stateData, err := json.Marshal(state)

	if err != nil {
		return err
	}

	stateWriter, err := f.createStringFromField(multiPartWriter, "state")
	if err != nil {
		return err
	}

	_, err = stateWriter.Write(stateData)

	if err != nil {
		return err
	}

	// In here, we completed adding the file and the fields, let's close the multipart writer
	// So it writes the ending boundary
	if err = multiPartWriter.Close(); err != nil {
		return err
	}

	contentType := multiPartWriter.FormDataContentType()

	err = f.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, bodyBuf, contentType)

	if err != nil {
		return err
	}

	return nil
}

func (f *functions) TriggerFunction(tenant, namespace, name, topic, triggerValue, triggerFile string) (string, error) {
	return f.TriggerFunctionWithContext(context.Background(), tenant, namespace, name, topic, triggerValue, triggerFile)
}

func (f *functions) TriggerFunctionWithContext(
	ctx context.Context,
	tenant,
	namespace,
	name,
	topic,
	triggerValue,
	triggerFile string,
) (string, error) {
	endpoint := f.pulsar.endpoint(f.basePath, tenant, namespace, name, "trigger")

	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	if triggerFile != "" {
		file, err := os.Open(triggerFile)
		if err != nil {
			return "", err
		}
		defer file.Close()

		part, err := multiPartWriter.CreateFormFile("dataStream", filepath.Base(file.Name()))

		if err != nil {
			return "", err
		}

		// copy the actual file content to the filed's writer
		_, err = io.Copy(part, file)
		if err != nil {
			return "", err
		}
	}

	if triggerValue != "" {
		valueWriter, err := f.createTextFromFiled(multiPartWriter, "data")
		if err != nil {
			return "", err
		}

		_, err = valueWriter.Write([]byte(triggerValue))
		if err != nil {
			return "", err
		}
	}

	if topic != "" {
		topicWriter, err := f.createTextFromFiled(multiPartWriter, "topic")
		if err != nil {
			return "", err
		}

		_, err = topicWriter.Write([]byte(topic))
		if err != nil {
			return "", err
		}
	}

	// In here, we completed adding the file and the fields, let's close the multipart writer
	// So it writes the ending boundary
	if err := multiPartWriter.Close(); err != nil {
		return "", err
	}

	contentType := multiPartWriter.FormDataContentType()
	var str string
	err := f.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, &str, bodyBuf, contentType)
	if err != nil {
		return "", err
	}

	return str, nil
}

func (f *functions) Upload(sourceFile, path string) error {
	return f.UploadWithContext(context.Background(), sourceFile, path)
}

func (f *functions) UploadWithContext(ctx context.Context, sourceFile, path string) error {
	if strings.TrimSpace(sourceFile) == "" && strings.TrimSpace(path) == "" {
		return fmt.Errorf("source file or path is empty")
	}
	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()
	endpoint := f.pulsar.endpoint(f.basePath, "upload")
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	writer, err := w.CreateFormFile("data", file.Name())
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, file)
	if err != nil {
		return err
	}
	if err := w.WriteField("path", path); err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return f.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, &b, w.FormDataContentType())
}
