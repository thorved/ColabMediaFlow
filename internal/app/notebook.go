package app

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

const (
	notebookName = "ColabMediaFlow.ipynb"
	notebookMIME = "application/vnd.google.colaboratory"

	managedProperty = "colabMediaFlowManaged"
)

//go:embed notebook/worker.py
var workerPython string

const notebookEntry = "if 'MEDIA_FLOW_RUN_PATH' not in globals():\n    raise RuntimeError('Start the managed worker through the ColabMediaFlow API.')\nrun_worker(Path(MEDIA_FLOW_RUN_PATH))\n"

func notebookCode() []string {
	return []string{workerPython, notebookEntry}
}

func notebookContents() []byte {
	cells := []map[string]any{
		{
			"cell_type": "markdown", "id": "media-flow-intro", "metadata": map[string]any{},
			"source": "# ColabMediaFlow\nManaged Drive archive worker. Use the ColabMediaFlow API to run this notebook manually or on a schedule.",
		},
	}
	for index, source := range notebookCode() {
		cells = append(cells, map[string]any{
			"cell_type": "code", "id": fmt.Sprintf("media-flow-%d", index), "metadata": map[string]any{},
			"source": source, "execution_count": nil, "outputs": []any{},
		})
	}
	return encodeJSON(map[string]any{
		"nbformat": 4, "nbformat_minor": 5, "cells": cells,
		"metadata": map[string]any{
			"kernelspec":    map[string]string{"name": "python3", "display_name": "Python 3", "language": "python"},
			"language_info": map[string]string{"name": "python"},
		},
	})
}

func verifiedCode(data []byte) ([]string, error) {
	mismatch := errors.New("managed notebook code differs from the bundled worker; restore it with PUT /v1/notebook")
	var notebook struct {
		Cells []struct {
			Type   string          `json:"cell_type"`
			Source json.RawMessage `json:"source"`
		}
	}
	if json.Unmarshal(data, &notebook) != nil {
		return nil, mismatch
	}
	var code []string
	for _, cell := range notebook.Cells {
		if cell.Type != "code" {
			continue
		}
		var source string
		if err := json.Unmarshal(cell.Source, &source); err != nil {
			var lines []string
			if json.Unmarshal(cell.Source, &lines) != nil {
				return nil, mismatch
			}
			source = strings.Join(lines, "")
		}
		code = append(code, strings.ReplaceAll(source, "\r\n", "\n"))
	}
	expected := notebookCode()
	for index := range expected {
		expected[index] = strings.ReplaceAll(expected[index], "\r\n", "\n")
	}
	if !slices.Equal(code, expected) {
		return nil, mismatch
	}
	return code, nil
}

func ensureNotebook(ctx context.Context, g *googleapi.Client, state *store, parent string) (string, bool, error) {
	folder, err := g.ReadDriveFile(ctx, parent)
	if err != nil {
		return "", false, err
	}
	if folder.MimeType != googleapi.FolderMIME || folder.Trashed {
		return "", false, errors.New("configured notebook parent is not an active Drive folder")
	}
	readyID, err := state.metadata(ctx, "notebook_id")
	if err != nil {
		return "", false, err
	}
	candidate, err := state.metadata(ctx, "notebook_candidate_id")
	if err != nil {
		return "", false, err
	}
	var file googleapi.DriveFile
	if candidate != "" {
		file, err = g.ReadDriveFile(ctx, candidate)
		if err != nil && !googleapi.IsNotFound(err) {
			return "", false, err
		}
		if file.Trashed || (googleapi.IsNotFound(err) && candidate == readyID) {
			candidate, file = "", googleapi.DriveFile{}
		}
	}
	if candidate == "" {
		candidate, err = g.ReserveDriveID(ctx)
		if err != nil {
			return "", false, err
		}
		if err := state.setMetadata(ctx, "notebook_candidate_id", candidate); err != nil {
			return "", false, err
		}
	}
	created := file.ID == ""
	if created {
		file, err = g.UploadDriveFile(ctx, googleapi.DriveFile{
			ID: candidate, Name: notebookName, MimeType: notebookMIME, Parents: []string{parent},
			AppProperties: map[string]string{managedProperty: "true"},
		}, notebookMIME, notebookContents(), "")
		if googleapi.IsConflict(err) {
			file, err = g.ReadDriveFile(ctx, candidate)
		}
		if err != nil {
			return "", false, err
		}
	}
	if file.ID != candidate || file.Trashed || file.MimeType != notebookMIME ||
		file.AppProperties[managedProperty] != "true" || !slices.Contains(file.Parents, parent) {
		return "", false, errors.New("the managed notebook does not match the configured Drive folder and file type")
	}
	data, err := g.DownloadDriveFile(ctx, candidate)
	if err != nil {
		return "", false, err
	}
	if _, codeErr := verifiedCode(data); codeErr != nil || file.Name != notebookName {
		if _, err := g.UploadDriveFile(ctx, googleapi.DriveFile{Name: notebookName}, notebookMIME, notebookContents(), candidate); err != nil {
			return "", false, err
		}
		data, err = g.DownloadDriveFile(ctx, candidate)
		if err != nil {
			return "", false, err
		}
		if _, err := verifiedCode(data); err != nil {
			return "", false, err
		}
	}
	return candidate, created, state.setMetadata(ctx, "notebook_id", candidate)
}
