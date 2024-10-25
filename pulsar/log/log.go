// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional in_ion
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

package log

// DefaultNopLogger returns a nop logger.
func DefaultNopLogger() Logger {
	return nopLogger{}
}

type nopLogger struct{}

func (l nopLogger) SubLogger(_ Fields) Logger               { return l }
func (l nopLogger) WithFields(_ Fields) Entry               { return nopEntry{} }
func (l nopLogger) WithField(_ string, _ interface{}) Entry { return nopEntry{} }
func (l nopLogger) WithError(_ error) Entry                 { return nopEntry{} }
func (l nopLogger) Debug(_ ...interface{})                  {}
func (l nopLogger) Info(_ ...interface{})                   {}
func (l nopLogger) Warn(_ ...interface{})                   {}
func (l nopLogger) Error(_ ...interface{})                  {}
func (l nopLogger) Debugf(_ string, _ ...interface{})       {}
func (l nopLogger) Infof(_ string, _ ...interface{})        {}
func (l nopLogger) Warnf(_ string, _ ...interface{})        {}
func (l nopLogger) Errorf(_ string, _ ...interface{})       {}

type nopEntry struct{}

func (e nopEntry) WithFields(_ Fields) Entry               { return nopEntry{} }
func (e nopEntry) WithField(_ string, _ interface{}) Entry { return nopEntry{} }

func (e nopEntry) Debug(_ ...interface{})            {}
func (e nopEntry) Info(_ ...interface{})             {}
func (e nopEntry) Warn(_ ...interface{})             {}
func (e nopEntry) Error(_ ...interface{})            {}
func (e nopEntry) Debugf(_ string, _ ...interface{}) {}
func (e nopEntry) Infof(_ string, _ ...interface{})  {}
func (e nopEntry) Warnf(_ string, _ ...interface{})  {}
func (e nopEntry) Errorf(_ string, _ ...interface{}) {}
