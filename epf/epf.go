// Package epf provides a client for USPS Electronic Product Fulfillment.
package epf

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"cloud.google.com/go/civil"
)

const baseURL = "https://epfws.usps.gov/ws/resources/"

type Session struct {
	logonkey string
	tokenkey string
}

type File struct {
	ID              string
	Filename        string
	Path            string
	Size            uint64
	FulfillmentDate civil.Date
	ProductCode     string
	ProductID       string
	Status          FileStatus
}

type FileStatus string

const (
	FileStatusNew               FileStatus = "N"
	FileStatusDownloadStarted   FileStatus = "S"
	FileStatusDownloadCancelled FileStatus = "X"
	FileStatusDownloadComplete  FileStatus = "C"
)

func Version() (string, string, error) {
	resp, err := http.DefaultClient.Get(baseURL + "epf/version")
	if err != nil {
		return "", "", err
	}

	v := versionResponse{}
	err = parseResult(resp.Body, &v)
	if err != nil {
		return "", "", err
	}

	return v.Version, v.Build, nil
}

func Login(email string, password string) (*Session, error) {
	args := map[string]string{
		"login": email,
		"pword": password,
	}

	sess := Session{}

	err := sess.doParse("epf/login", args, &sessionResponse{})
	if err != nil {
		return nil, err
	}

	return &sess, nil
}

func (s *Session) Logout() error {
	err := s.doParse("epf/logout", nil, &sessionResponse{})
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) Files() ([]File, error) {
	resp := dnldlistResponse{}
	err := s.doParse("download/dnldlist", nil, &resp)
	if err != nil {
		return nil, err
	}

	files := []File{}
	for _, e := range resp.DnldfileList {
		f, err := fileFromDnldlistEntry(e)
		if err != nil {
			return nil, err
		}
		files = append(files, *f)
	}

	return files, nil
}

func (s *Session) FilesByProduct(productCode string, productID string) ([]File, error) {
	return s.FilesByProductFiltered(productCode, productID, nil)
}

func (s *Session) FilesByProductFiltered(productCode string, productID string, statuses []FileStatus) ([]File, error) {
	args := map[string]string{
		"productcode": productCode,
		"productid":   productID,
	}

	if len(statuses) > 0 {
		joined := ""
		for _, s := range statuses {
			joined += string(s)
		}
		args["status"] = joined
	}

	resp := listplusResponse{}
	err := s.doParse("download/listplus", args, &resp)
	if err != nil {
		return nil, err
	}

	files := []File{}
	for _, e := range resp.FileList {
		f, err := fileFromListplusEntry(e)
		if err != nil {
			return nil, err
		}
		f.ProductCode = productCode
		f.ProductID = productID
		files = append(files, *f)
	}

	return files, nil
}

func (s *Session) Download(fileID string) (io.ReadCloser, error) {
	args := map[string]string{"fileid": fileID}

	resp, err := s.do("download/epf", args)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *Session) SetStatus(fileID string, status FileStatus) error {
	args := map[string]string{
		"fileid":    fileID,
		"newstatus": string(status),
	}

	return s.doParse("download/status", args, &sessionResponse{})
}

func (s *Session) doParse(path string, args map[string]string, result result) error {
	resp, err := s.do(path, args)
	if err != nil {
		return err
	}

	return parseResult(resp.Body, result)
}

func (s *Session) do(path string, args map[string]string) (*http.Response, error) {
	obj := make(map[string]string)
	for k := range args {
		obj[k] = args[k]
	}
	obj["logonkey"] = s.logonkey
	obj["tokenkey"] = s.tokenkey

	objJSON, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	v := url.Values{}
	v.Add("obj", string(objJSON))

	resp, err := http.DefaultClient.PostForm(baseURL+path, v)
	if err != nil {
		return nil, err
	}

	s.logonkey = resp.Header.Get("User-Logonkey")
	s.tokenkey = resp.Header.Get("User-Tokenkey")

	return resp, nil
}

func parseResult(r io.ReadCloser, result result) error {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}

	if result.Status() != "success" {
		return fmt.Errorf("%v: %v", result.Status(), result.Message())
	}

	return nil
}

type result interface {
	Status() string
	Message() string
}

type response struct {
	Response string
	Messages string
}

func (r response) Status() string {
	return r.Response
}

func (r response) Message() string {
	return r.Messages
}

type sessionResponse struct {
	response
	Logonkey string
	Tokenkey string
}

// epf/version
type versionResponse struct {
	response
	Version string
	Build   string
}

// download/dnldlist
type dnldlistResponse struct {
	sessionResponse
	Reccount     string
	DnldfileList []dndlistEntry
}

type dndlistEntry struct {
	Productcode string
	Productid   string
	Fulfilled   string
	Status      string
	Fileid      string
	Filepath    string
	Filename    string
	Filesize    string
}

// download/listplus
type listplusResponse struct {
	sessionResponse
	Reccount string
	FileList []listplusEntry
}

type listplusEntry struct {
	Fileid    string
	Status    string
	Fulfilled string
	Filepath  string
	Filename  string
	Filesize  string
}

func fileFromDnldlistEntry(e dndlistEntry) (*File, error) {
	size, err := strconv.ParseUint(e.Filesize, 10, 64)
	if err != nil {
		return nil, err
	}

	fulfillmentDate, err := civil.ParseDate(e.Fulfilled)
	if err != nil {
		return nil, err
	}

	return &File{
		ID:              e.Fileid,
		Filename:        e.Filename,
		Path:            e.Filepath,
		Size:            size,
		FulfillmentDate: fulfillmentDate,
		ProductCode:     e.Productcode,
		ProductID:       e.Productid,
		Status:          FileStatus(e.Status),
	}, nil
}

func fileFromListplusEntry(e listplusEntry) (*File, error) {
	size, err := strconv.ParseUint(e.Filesize, 10, 64)
	if err != nil {
		return nil, err
	}

	fulfillmentDate, err := civil.ParseDate(e.Fulfilled)
	if err != nil {
		return nil, err
	}

	return &File{
		ID:              e.Fileid,
		Filename:        e.Filename,
		Path:            e.Filepath,
		Size:            size,
		FulfillmentDate: fulfillmentDate,
		Status:          FileStatus(e.Status),
	}, nil
}
