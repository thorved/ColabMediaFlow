package googleapi

import (
	"bytes"
	"context"
	"errors"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
)

const FolderMIME = "application/vnd.google-apps.folder"

var driveID = regexp.MustCompile("^[a-zA-Z0-9_-]+$")

func ValidDriveID(id string) bool {
	return driveID.MatchString(id)
}

type DriveFile struct {
	ID            string            `json:"id,omitempty"`
	Name          string            `json:"name,omitempty"`
	MimeType      string            `json:"mimeType,omitempty"`
	Parents       []string          `json:"parents,omitempty"`
	AppProperties map[string]string `json:"appProperties,omitempty"`
	Trashed       bool              `json:"trashed,omitempty"`
}

const driveFields = "id,name,mimeType,parents,appProperties,trashed"

func (g *Client) ReadDriveFile(ctx context.Context, fileID string) (DriveFile, error) {
	var file DriveFile
	err := g.rest(ctx, http.MethodGet, queryURL(g.driveURL+"/files/"+url.PathEscape(fileID), url.Values{"fields": {driveFields}}),
		"read Drive file metadata", "", nil, &file)
	return file, err
}

func (g *Client) ReserveDriveID(ctx context.Context) (string, error) {
	var ids struct{ IDs []string }
	err := g.rest(ctx, http.MethodGet, queryURL(g.driveURL+"/files/generateIds", url.Values{"space": {"drive"}, "count": {"1"}}),
		"reserve Drive file ID", "", nil, &ids)
	if err != nil {
		return "", err
	}
	if len(ids.IDs) != 1 || !ValidDriveID(ids.IDs[0]) {
		return "", errors.New("Drive did not return a valid file ID")
	}
	return ids.IDs[0], nil
}

func (g *Client) DownloadDriveFile(ctx context.Context, fileID string) ([]byte, error) {
	var data []byte
	address := queryURL(g.driveURL+"/files/"+url.PathEscape(fileID), url.Values{"alt": {"media"}})
	err := g.rest(ctx, http.MethodGet, address, "read managed notebook from Drive", "", nil, &data)
	return data, err
}

func multipartDrive(metadata DriveFile, mimeType string, data []byte) (string, []byte, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	parts := []struct {
		contentType string
		data        []byte
	}{
		{"application/json; charset=UTF-8", encodeJSON(metadata)},
		{mimeType, data},
	}
	for _, part := range parts {
		partWriter, err := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {part.contentType}})
		if err != nil {
			return "", nil, err
		}
		if _, err := partWriter.Write(part.data); err != nil {
			return "", nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return "", nil, err
	}
	return "multipart/related; boundary=" + writer.Boundary(), body.Bytes(), nil
}

func (g *Client) UploadDriveFile(ctx context.Context, metadata DriveFile, mimeType string, data []byte, updateID string) (DriveFile, error) {
	contentType, body, err := multipartDrive(metadata, mimeType, data)
	if err != nil {
		return DriveFile{}, err
	}
	method, address := http.MethodPost, g.uploadURL+"/files"
	if updateID != "" {
		method, address = http.MethodPatch, address+"/"+url.PathEscape(updateID)
	}
	var file DriveFile
	err = g.rest(ctx, method, queryURL(address, url.Values{"uploadType": {"multipart"}, "fields": {driveFields}}),
		"save file to Drive", contentType, body, &file)
	return file, err
}
