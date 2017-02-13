package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

func Usage() {
	fmt.Fprintf(os.Stderr, `Usage: %s [OPTIONS] [KEY=VALUE]...

Downloads selected OONI results.
KEY and VALUE are query string parameters as described at
https://measurements.ooni.torproject.org/api/. For example:
%s test_name=tcp_connect probe_cc=US

`, os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

// https://measurements.ooni.torproject.org/api/
const ooniAPIURL = "https://measurements.ooni.torproject.org/api/v1/files"
const ooniAPILimit = 1000
const numDownloadThreads = 5

var outputDirectory = "."

// Represents the state of a download attempt. processIndex writes these into a
// channel and the main goroutine collects them for status updates and cleanup.
type result struct {
	URL           string
	LocalFilename string
	TmpFilename   string
	Exists        bool
	Err           error
}

type progressCounter struct {
	n, total uint
	mutex    sync.Mutex
}

func (progress *progressCounter) format() string {
	totalString := fmt.Sprintf("%d", progress.total)
	return fmt.Sprintf("%*d/%s", len(totalString), progress.n, totalString)
}

var progress progressCounter

type ooniMetadata struct {
	Count  uint `json:"count"`
	Offset uint `json:"offset"`
	Limit  uint `json:"limit"`
	// `json:"current_page"`
	// `json:"next_url"`
	// `json:"pages"`
}

type ooniResult struct {
	DownloadURL string `json:"download_url"`
	Index       uint   `json:"index"`
	// `json:"probe_asn"`
	// `json:"probe_cc"`
	// `json:"test_start_time"`
}

type ooniIndexPage struct {
	Metadata ooniMetadata `json:"metadata"`
	Results  []ooniResult `json:"results"`
}

// Download the contents of a URL and copy them into w.
func downloadToWriter(urlString string, w io.Writer) (err error) {
	var resp *http.Response
	resp, err = http.Get(urlString)
	if err != nil {
		return err
	}
	defer func() {
		err2 := resp.Body.Close()
		if err == nil {
			err = err2
		}
	}()

	_, err = io.Copy(w, resp.Body)
	return err
}

func downloadToTmpFile(urlString string, tmpFilenameChan chan<- string) (string, error) {
	tmpfile, err := ioutil.TempFile(outputDirectory, "ooni-sync.tmp.")
	if err != nil {
		return "", err
	}
	// Tell the main goroutine thread to clean up this temporary file.
	tmpFilenameChan <- tmpfile.Name()

	err = downloadToWriter(urlString, tmpfile)
	err2 := tmpfile.Close()
	if err == nil {
		err = err2
	}

	return tmpfile.Name(), err
}

func maybeDownload(urlString string, tmpFilenameChan chan<- string) *result {
	r := &result{}
	r.URL = urlString

	u, err := url.Parse(r.URL)
	if err != nil {
		r.Err = err
		return r
	}
	r.LocalFilename = filepath.Join(outputDirectory, path.Base(u.Path))

	_, err = os.Stat(r.LocalFilename)
	if err == nil {
		r.Exists = true
		return r
	} else if !os.IsNotExist(err) {
		r.Err = err
		return r
	}

	r.TmpFilename, r.Err = downloadToTmpFile(r.URL, tmpFilenameChan)
	return r
}

func logOK(downloadURL string) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s ok: %s\n", progress.format(), downloadURL)
	progress.mutex.Unlock()
}

func logExists(downloadURL string) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s exists: %s\n", progress.format(), downloadURL)
	progress.mutex.Unlock()
}

func logError(downloadURL string, err error) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s error: %s: %s\n", progress.format(), downloadURL, err)
	progress.mutex.Unlock()
}

func downloadFromChan(downloadURLChan <-chan string, resultChan chan<- *result, tmpFilenameChan chan<- string) {
	for downloadURL := range downloadURLChan {
		resultChan <- maybeDownload(downloadURL, tmpFilenameChan)
	}
}

func fetchIndexPage(baseQuery url.Values, limit, offset uint) (*ooniIndexPage, error) {
	u, err := url.Parse(ooniAPIURL)
	if err != nil {
		return nil, err
	}

	// Copy the requested query values (e.g. "test_name").
	query := u.Query()
	for k, v := range baseQuery {
		query[k] = v
	}
	// Set query values "order", "limit", and "offset".
	query.Set("order", "asc")
	query.Set("limit", fmt.Sprintf("%d", limit))
	query.Set("offset", fmt.Sprintf("%d", offset))
	u.RawQuery = query.Encode()

	fmt.Printf("Index: %s\n", u.String())
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var indexPage ooniIndexPage
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&indexPage)
	if err != nil {
		return nil, err
	}
	if decoder.More() {
		return &indexPage, fmt.Errorf("expected only one JSON value")
	}

	return &indexPage, nil
}

func processIndex(query url.Values, downloadURLChan chan<- string) error {
	var offset uint = 0
	for {
		indexPage, err := fetchIndexPage(query, ooniAPILimit, offset)
		if err != nil {
			return err
		}

		// Sanity checks.
		if indexPage.Metadata.Limit != ooniAPILimit {
			return fmt.Errorf("expected limit=%d, got limit=%d", ooniAPILimit, indexPage.Metadata.Limit)
		}
		if offset != indexPage.Metadata.Offset {
			return fmt.Errorf("expected offset=%d, got offset=%d", offset, indexPage.Metadata.Offset)
		}

		numResults := uint(len(indexPage.Results))

		// Require at least one result so we're guaranteed to make
		// progress on each iteration. The only time zero results are
		// allowed is when indexPage.Metadata.Count == 0.
		if indexPage.Metadata.Count > 0 && numResults == 0 {
			return fmt.Errorf("zero results")
		}

		progress.mutex.Lock()
		progress.total = indexPage.Metadata.Count
		progress.mutex.Unlock()

		offset += uint(len(indexPage.Results))

		if offset > indexPage.Metadata.Count {
			return fmt.Errorf("offset exceeds count: %d > %d", offset, indexPage.Metadata.Count)
		}

		for _, result := range indexPage.Results {
			downloadURLChan <- result.DownloadURL
		}

		if offset == indexPage.Metadata.Count {
			// All done.
			break
		}
		// Otherwise continue looping.
	}

	return nil
}

// Parse a sequence of "key=value" strings into a url.Values.
func parseArgsToQuery(args []string) (url.Values, error) {
	query := url.Values{}
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return query, fmt.Errorf("malformed query parameter: %q", arg)
		}
		query.Add(parts[0], parts[1])
	}
	return query, nil
}

func main() {
	flag.Usage = Usage
	flag.StringVar(&outputDirectory, "directory", outputDirectory, "directory in which to save results")
	flag.Parse()

	err := os.MkdirAll(outputDirectory, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	query, err := parseArgsToQuery(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	tmpFilenameChan := make(chan string)
	resultChan := make(chan *result)

	downloadURLChan := make(chan string, ooniAPILimit)
	go func() {
		err = processIndex(query, downloadURLChan)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(1)
		}
		close(downloadURLChan)
	}()
	var wg sync.WaitGroup
	wg.Add(numDownloadThreads)
	for i := 0; i < numDownloadThreads; i++ {
		go func() {
			downloadFromChan(downloadURLChan, resultChan, tmpFilenameChan)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Keep track of temporary files we need to delete at the end.
	tmpFilenames := make(map[string]struct{})

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

loop:
	for {
		select {
		case tmpFilename := <-tmpFilenameChan:
			tmpFilenames[tmpFilename] = struct{}{}
		case r, ok := <-resultChan:
			if !ok {
				break loop
			}
			if r.Err != nil {
				logError(r.URL, r.Err)
			} else if r.Exists {
				logExists(r.URL)
			} else {
				err := os.Rename(r.TmpFilename, r.LocalFilename)
				if err != nil {
					logError(r.URL, err)
				} else {
					delete(tmpFilenames, r.TmpFilename)
					logOK(r.URL)
				}
			}
		case <-sigChan:
			break loop
		}
	}

	for tmpFilename := range tmpFilenames {
		err := os.Remove(tmpFilename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot delete temporary: %s", err)
		}
	}
}
