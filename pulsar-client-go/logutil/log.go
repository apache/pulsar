//
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
//

package logutil

import (
	"bytes"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LoggerLevel int

const (
	DEBUG LoggerLevel = iota
	INFO
	WARN
	ERROR
)

func (l LoggerLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"

	default:
		return fmt.Sprintf("UNKNOWN: %d", l)
	}
}

const (
	defaultLogLevel      = log.InfoLevel
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
)

func Info(v ...interface{}) {
	log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

func Debug(v ...interface{}) {
	log.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func Warn(v ...interface{}) {
	log.Warn(v...)
}

func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

func Error(v ...interface{}) {
	log.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

func Fatal(v ...interface{}) {
	log.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// hook injects file name and line pos into log entry.
type hook struct{}

// Fire implements logrus.Hook interface
func (h *hook) Fire(entry *log.Entry) error {
	// these two num are set by manually testing
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(10, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (h *hook) Levels() []log.Level {
	return log.AllLevels
}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/apache/pulsar/pulsar-client-go/logutil")
}

// formatter is for compatibility with ngaut/log
type formatter struct {
	DisableTimestamp, EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *formatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			if k != "file" && k != "line" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		for k, v := range entry.Data {
			if k != "file" && k != "line" {
				fmt.Fprintf(b, " %v=%v", k, v)
			}
		}
	}

	b.WriteByte('\n')

	return b.Bytes(), nil
}

// SetLevel sets log's level by a level string.
func SetLevel(level string) {
	log.SetLevel(stringToLogLevel(level))
}

// GetLevel gets current log's level as a level string.
func GetLevel() string {
	return log.GetLevel().String()
}

// SetOutput sets the filename for the log.
func SetOutput(filename string) {
	output := &lumberjack.Logger{
		Filename:  filename,
		LocalTime: true,
	}
	log.SetOutput(output)
}

func init() {
	log.SetLevel(defaultLogLevel)
	log.SetFormatter(&formatter{})
	log.AddHook(&hook{})
}
