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
	"path"
	"path/filepath"
	"strings"
	"sync"
)

func Usage() {
	fmt.Fprintf(os.Stderr,
`Usage: %s [OPTIONS] [KEY=VALUE]...

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

var downloadURLChan chan string

type progressCounter struct {
	n, total uint
	mutex sync.RWMutex
}

func (progress *progressCounter) SetTotal(total uint) {
	progress.mutex.Lock()
	defer progress.mutex.Unlock()
	progress.total = total
}

func (progress *progressCounter) Increment() (n, total uint) {
	progress.mutex.Lock()
	defer progress.mutex.Unlock()
	progress.n += 1
	return progress.n, progress.total
}

func formatProgress(n, total uint) string {
	totalString := fmt.Sprintf("%d", total)
	return fmt.Sprintf("%*d/%s", len(totalString), n, totalString)
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

func downloadToFile(urlString, filename string) error {
	tmpfile, err := ioutil.TempFile(outputDirectory, "ooni-sync.tmp.")
	if err != nil {
		return err
	}

	err = downloadToWriter(urlString, tmpfile)
	err2 := tmpfile.Close()
	if err == nil {
		err = err2
	}
	if err != nil {
		os.Remove(tmpfile.Name())
		return err
	}

	return os.Rename(tmpfile.Name(), filename)
}

func maybeDownloadToFile(urlString, filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err == nil {
		return true, err
	} else if !os.IsNotExist(err) {
		return false, err
	}
	return false, downloadToFile(urlString, filename)
}

func maybeDownload(urlString string) (bool, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return false, err
	}
	filename := path.Base(u.Path)
	filename = filepath.Join(outputDirectory, filename)
	return maybeDownloadToFile(urlString, filename)
}

func downloadFromChan(downloadURLChan <-chan string) {
	for downloadURL := range downloadURLChan {
		exists, err := maybeDownload(downloadURL)
		n, total := progress.Increment()
		if err != nil {
			fmt.Printf("%s error: %s: %s\n", formatProgress(n, total), downloadURL, err)
		} else if exists {
			fmt.Printf("%s exists: %s\n", formatProgress(n, total), downloadURL)
		} else {
			fmt.Printf("%s ok: %s\n", formatProgress(n, total), downloadURL)
		}
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

func processIndex(query url.Values) error {
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

		progress.SetTotal(indexPage.Metadata.Count)

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

	var wg sync.WaitGroup
	downloadURLChan = make(chan string, ooniAPILimit)
	for i := 0; i < numDownloadThreads; i++ {
		wg.Add(1)
		go func() {
			downloadFromChan(downloadURLChan)
			wg.Done()
		}()
	}

	query, err := parseArgsToQuery(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
	err = processIndex(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	close(downloadURLChan)
	wg.Wait()
}
