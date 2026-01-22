// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fsartifact provides a file system implementation of [artifact.Service].
//
// This package allows storing and retrieving artifacts in the local file system.
// Artifacts are organized by application name, user ID, session ID, and filename,
// with support for versioning.
package fsartifact

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/adk/artifact"
	"google.golang.org/genai"
)

// fsService is a file system implementation of the Service.
type fsService struct {
	rootDir string
}

// NewService creates a FS service for the specified root directory.
func NewService(rootDir string) (artifact.Service, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root dir: %w", err)
	}
	return &fsService{
		rootDir: rootDir,
	}, nil
}

// fileHasUserNamespace checks if a filename indicates a user-namespaced blob.
func fileHasUserNamespace(filename string) bool {
	return strings.HasPrefix(filename, "user:")
}

// buildPath constructs the file path in the file system.
func (s *fsService) buildPath(appName, userID, sessionID, fileName string, version int64) string {
	if fileHasUserNamespace(fileName) {
		return filepath.Join(s.rootDir, appName, userID, "user", fileName, fmt.Sprintf("%d", version))
	}
	return filepath.Join(s.rootDir, appName, userID, sessionID, fileName, fmt.Sprintf("%d", version))
}

// buildDir constructs the directory path for a specific artifact (containing versions).
func (s *fsService) buildDir(appName, userID, sessionID, fileName string) string {
	if fileHasUserNamespace(fileName) {
		return filepath.Join(s.rootDir, appName, userID, "user", fileName)
	}
	return filepath.Join(s.rootDir, appName, userID, sessionID, fileName)
}

func (s *fsService) buildSessionDir(appName, userID, sessionID string) string {
	return filepath.Join(s.rootDir, appName, userID, sessionID)
}

func (s *fsService) buildUserDir(appName, userID string) string {
	return filepath.Join(s.rootDir, appName, userID, "user")
}

// Save implements [artifact.Service]
func (s *fsService) Save(ctx context.Context, req *artifact.SaveRequest) (*artifact.SaveResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	newArtifact := req.Part

	nextVersion := int64(1)
	if req.Version > 0 {
		nextVersion = req.Version
	} else {
		// Find next version
		response, err := s.Versions(ctx, &artifact.VersionsRequest{
			AppName: req.AppName, UserID: req.UserID, SessionID: req.SessionID, FileName: req.FileName,
		})
		if err == nil && len(response.Versions) > 0 {
			nextVersion = slices.Max(response.Versions) + 1
		}
	}

	path := s.buildPath(appName, userID, sessionID, fileName, nextVersion)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	var data []byte
	var contentType string

	if newArtifact.InlineData != nil {
		data = newArtifact.InlineData.Data
		contentType = newArtifact.InlineData.MIMEType
	} else {
		data = []byte(newArtifact.Text)
		contentType = "text/plain"
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	// Write metadata file for ContentType
	metaPath := path + ".meta"
	if err := os.WriteFile(metaPath, []byte(contentType), 0644); err != nil {
		// Best effort cleanup
		os.Remove(path)
		return nil, fmt.Errorf("failed to write metadata file: %w", err)
	}

	return &artifact.SaveResponse{Version: nextVersion}, nil
}

// Load implements [artifact.Service]
func (s *fsService) Load(ctx context.Context, req *artifact.LoadRequest) (*artifact.LoadResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	version := req.Version

	if version == 0 {
		response, err := s.Versions(ctx, &artifact.VersionsRequest{
			AppName: req.AppName, UserID: req.UserID, SessionID: req.SessionID, FileName: req.FileName,
		})
		if err != nil {
			return nil, err // artifact not found error comes from Versions
		}
		version = slices.Max(response.Versions)
	}

	path := s.buildPath(appName, userID, sessionID, fileName, version)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact '%s' version %d not found: %w", fileName, version, fs.ErrNotExist)
		}
		return nil, fmt.Errorf("could not read file '%s': %w", path, err)
	}

	var contentType string
	metaPath := path + ".meta"
	metaData, err := os.ReadFile(metaPath)
	if err == nil {
		contentType = string(metaData)
	} else {
		contentType = "text/plain"
	}

	part := genai.NewPartFromBytes(data, contentType)
	return &artifact.LoadResponse{Part: part}, nil
}

// Delete implements [artifact.Service]
func (s *fsService) Delete(ctx context.Context, req *artifact.DeleteRequest) error {
	if err := req.Validate(); err != nil {
		return fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	version := req.Version

	if version != 0 {
		path := s.buildPath(appName, userID, sessionID, fileName, version)
		err := os.Remove(path)
		// Clean up meta file as well
		os.Remove(path + ".meta")
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete artifact file: %w", err)
		}
		return nil
	}

	// Delete all versions (remove the whole directory for the artifact)
	dir := s.buildDir(appName, userID, sessionID, fileName)
	err := os.RemoveAll(dir)
	if err != nil {
		return fmt.Errorf("failed to delete artifact directory: %w", err)
	}
	return nil
}

// List implements [artifact.Service]
func (s *fsService) List(ctx context.Context, req *artifact.ListRequest) (*artifact.ListResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID := req.AppName, req.UserID, req.SessionID
	filenamesSet := map[string]bool{}

	// Helper to read dir
	readDir := func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return // Ignore missing dirs
		}
		for _, entry := range entries {
			if entry.IsDir() {
				filenamesSet[entry.Name()] = true
			}
		}
	}

	// List session artifacts
	readDir(s.buildSessionDir(appName, userID, sessionID))

	// List user artifacts
	readDir(s.buildUserDir(appName, userID))

	filenames := slices.Collect(maps.Keys(filenamesSet))
	sort.Strings(filenames)
	return &artifact.ListResponse{FileNames: filenames}, nil
}

// Versions implements [artifact.Service]
func (s *fsService) Versions(ctx context.Context, req *artifact.VersionsRequest) (*artifact.VersionsResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName

	dir := s.buildDir(appName, userID, sessionID, fileName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact not found: %w", fs.ErrNotExist)
		}
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	var versions []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".meta") {
			continue
		}

		v, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		versions = append(versions, v)
	}

	if len(versions) == 0 {
		return nil, fmt.Errorf("artifact not found: %w", fs.ErrNotExist)
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return &artifact.VersionsResponse{Versions: versions}, nil
}
