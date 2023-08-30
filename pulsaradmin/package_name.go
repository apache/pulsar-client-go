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

package pulsaradmin

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type PackageName struct {
	packageType         PackageType
	namespace           string
	tenant              string
	name                string
	version             string
	completePackageName string
	completeName        string
}

func invalidPackageNameError(completeName string) error {
	return errors.Errorf("Invalid package name '%s', it should be "+
		"in the format of type://tenant/namespace/name@version", completeName)
}

func GetPackageNameWithComponents(packageType PackageType,
	tenant, namespace, name, version string) (*PackageName, error) {
	return GetPackageName(fmt.Sprintf("%s://%s/%s/%s@%s", packageType, tenant, namespace, name, version))
}

func GetPackageName(completeName string) (*PackageName, error) {
	var packageName PackageName
	var err error
	if !strings.Contains(completeName, "://") {
		return nil, invalidPackageNameError(completeName)
	}
	parts := strings.Split(completeName, "://")
	if len(parts) != 2 {
		return nil, invalidPackageNameError(completeName)
	}
	packageName.packageType, err = parsePackageType(parts[0])
	if err != nil {
		return nil, err
	}
	rest := parts[1]
	if !strings.Contains(rest, "@") {
		// if the package name does not contains '@', that means user does not set the version of package.
		// We will set the version to latest.
		rest += "@"
	}
	parts = strings.Split(rest, "@")
	if len(parts) != 2 {
		return nil, invalidPackageNameError(completeName)
	}
	partsWithoutVersion := strings.Split(parts[0], "/")
	if len(partsWithoutVersion) != 3 {
		return nil, invalidPackageNameError(completeName)
	}
	packageName.tenant = partsWithoutVersion[0]
	packageName.namespace = partsWithoutVersion[1]
	packageName.name = partsWithoutVersion[2]
	packageName.version = "latest"
	if parts[1] != "" {
		packageName.version = parts[1]
	}
	packageName.completeName = fmt.Sprintf("%s/%s/%s",
		packageName.tenant, packageName.namespace, packageName.name)
	packageName.completePackageName = fmt.Sprintf("%s://%s/%s/%s@%s",
		packageName.packageType, packageName.tenant, packageName.namespace, packageName.name, packageName.version)

	return &packageName, nil
}

func (p *PackageName) String() string {
	return p.completePackageName
}

func (p *PackageName) GetType() PackageType {
	return p.packageType
}

func (p *PackageName) GetTenant() string {
	return p.tenant
}

func (p *PackageName) GetNamespace() string {
	return p.namespace
}

func (p *PackageName) GetName() string {
	return p.name
}

func (p *PackageName) GetVersion() string {
	return p.version
}

func (p *PackageName) GetCompleteName() string {
	return p.completeName
}
