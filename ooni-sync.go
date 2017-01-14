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
	"strconv"
	"sync"
)

// https://measurements.ooni.torproject.org/api/
const ooniAPIURL = "https://measurements.ooni.torproject.org/api/v1/files"
const ooniAPILimit = 1000
const outputDirectory = "results"
const numDownloadThreads = 5

var downloadURLChan chan string

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

	fmt.Printf("Downloading to %s: %s\n", tmpfile.Name(), urlString)
	err = downloadToWriter(urlString, tmpfile)
	err2 := tmpfile.Close()
	if err == nil {
		err = err2
	}
	if err != nil {
		fmt.Printf("Deleting %s\n", tmpfile.Name())
		os.Remove(tmpfile.Name())
		return err
	}

	fmt.Printf("Renaming %s to %s\n", tmpfile.Name(), filename)
	return os.Rename(tmpfile.Name(), filename)
}

func maybeDownloadToFile(urlString, filename string) error {
	_, err := os.Stat(filename)
	if !os.IsNotExist(err) {
		fmt.Printf("Already exists: %s\n", filename)
		return err
	}
	return downloadToFile(urlString, filename)
}

// Downloads the given URL if the corresponding local file does not already
// exist.
func maybeDownload(urlString string) error {
	u, err := url.Parse(urlString)
	if err != nil {
		return err
	}
	filename := path.Base(u.Path)
	filename = filepath.Join(outputDirectory, filename)
	return maybeDownloadToFile(urlString, filename)
}

func downloadFromChan(downloadURLChan <-chan string) {
	for downloadURL := range downloadURLChan {
		err := maybeDownload(downloadURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %s\n", downloadURL, err)
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
	query.Set("limit", strconv.FormatUint(uint64(limit), 10))
	query.Set("offset", strconv.FormatUint(uint64(offset), 10))
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

		offset += uint(len(indexPage.Results))
		fmt.Printf("Processed %d/%d\n", offset, indexPage.Metadata.Count)

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

func main() {
	flag.Parse()
	if flag.NArg() > 0 {
		// No arguments allowed.
		flag.Usage()
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

	query := url.Values{}
	query.Set("test_name", "tcp_connect")
	err := processIndex(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	close(downloadURLChan)
	wg.Wait()
}
