// Fast downloader of OONI reports using the OONI API. Syncs a local directory
// with all reports satisfying a given API query. Only downloads reports that
// are not already present locally.
//
// Example usage:
// 	ooni-sync -xz -directory reports.tcp_connect.201701 test_name=tcp_connect since=2017-01-01 until=2017-02-02
// This command will create the directory reports.tcp_connect.201701 if it
// doesn't exist, download all reports satisfying the given query that are not
// already present in the directory, and compress the downloaded reports with
// xz.
//
// Possible API query parameters:
// 	test_name=[name] # e.g. web_connectivity, http_host, tcp_connect
// 	probe_cc=[cc]
// 	probe_asn=AS[num]
// 	since=[yyyy-mm-dd]
// 	until=[yyyy-mm-dd]
//
// By default, downloaded reports will be saved into the current directory. Use
// the -directory option to control the output directory. Use the -xz option to
// compress the downloaded reports (the .xz extension will be taken into account
// during later syncs, to avoid downloading the same report again).
//
// The program doesn't use checksums or timestamps for to compare local and
// remote content, only filenames. It assumes that if there is a local file with
// the same name as a remote file (perhaps adding a .xz extension), that the
// contents are identical. For that reason, the program tries hard not allow a
// local file to exist with the same name as a remote file unless it has the
// same contents. For example, an interrupted download will be discarded rather
// than left partially downloaded under its final filename.
//
// For documentation on the OONI API, see
// https://measurements.ooni.torproject.org/api/.
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
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: %s [OPTIONS] [KEY=VALUE]...

Downloads selected OONI results.
KEY and VALUE are query string parameters as described at
https://measurements.ooni.torproject.org/api/. For example:
%s -xz test_name=tcp_connect probe_cc=US

`, os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

// https://measurements.ooni.torproject.org/api/
const ooniAPIURL = "https://measurements.ooni.torproject.org/api/v1/files"
const ooniAPILimit = 1000
const numDownloadThreads = 5

// Controlled by the -directory option.
var outputDirectory = "."

// The -xz option changes these.
var outputExtension = ""
var downloadFilter = identityFilter

// Output filter to use when -xz is not in effect (save reports verbatim).
func identityFilter(w io.WriteCloser) (io.WriteCloser, error) {
	return w, nil
}

type xzFilter struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

// Output filter to use when -xz is in effect.
func newXZFilter(w io.WriteCloser) (io.WriteCloser, error) {
	var err error
	xz := &xzFilter{}

	xz.cmd = exec.Command("xz", "-c")
	xz.cmd.Stdout = w
	xz.stdin, err = xz.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	err = xz.cmd.Start()
	if err != nil {
		return nil, err
	}
	return xz, nil
}

func (xz *xzFilter) Write(p []byte) (int, error) {
	return xz.stdin.Write(p)
}

func (xz *xzFilter) Close() error {
	err := xz.stdin.Close()
	if err != nil {
		return err
	}
	return xz.cmd.Wait()
}

// Represents the state of a download attempt. processIndex writes these into a
// channel and the main goroutine collects them for status updates and cleanup.
type result struct {
	URL           string
	LocalFilename string
	TmpFilename   string
	Exists        bool
	Err           error
}

// This struct helps serialize the "X/Y" output messages.
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
func downloadToWriteCloser(urlString string, w io.WriteCloser) (err error) {
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
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got %q", resp.Status)
	}

	_, err = io.Copy(w, resp.Body)
	return err
}

// Download a URL to a temporary file. Writes the name of the temporary file to
// tmpFilenameChan before doing anything with it. Runs the downloaded contents
// through the an io.WriteCloser produced by calling downloadFilter on the
// temporary file.
func downloadToTmpFile(urlString string, tmpFilenameChan chan<- string) (string, error) {
	tmpfile, err := ioutil.TempFile(outputDirectory, "ooni-sync.tmp.")
	if err != nil {
		return "", err
	}
	// Tell the main goroutine thread to clean up this temporary file.
	tmpFilenameChan <- tmpfile.Name()

	// Optionally compress.
	w, err := downloadFilter(tmpfile)
	if err != nil {
		return "", err
	}

	err = downloadToWriteCloser(urlString, w)
	err2 := w.Close()
	if err == nil {
		err = err2
	}

	return tmpfile.Name(), err
}

// Check if a URL needs to be downloaded by checking for a matching local file,
// and download it if so.
func maybeDownload(urlString string, tmpFilenameChan chan<- string) *result {
	r := &result{}
	r.URL = urlString

	u, err := url.Parse(r.URL)
	if err != nil {
		r.Err = err
		return r
	}
	r.LocalFilename = filepath.Join(outputDirectory, path.Base(u.Path)) + outputExtension

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
	// We order by test_start_time and start with the oldest reports, so
	// that any reports that are published while the program is running will
	// be more likely to be appended to the final index page, and not throw
	// off the offsets for index pages already downloaded.
	// It would be better to order by index rather than test_start_time,
	// because index is increasing over time while newly published reports
	// may have a test_start_time in the past.
	query.Set("order", "asc")
	query.Set("limit", fmt.Sprintf("%d", limit))
	query.Set("offset", fmt.Sprintf("%d", offset))
	u.RawQuery = query.Encode()

	fmt.Printf("Index: %s\n", u.String())
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := resp.Body.Close()
		if err == nil {
			err = err2
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got %q", resp.Status)
	}

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

// Fix up the input query string to match the formats the server expects.
// Uppercases the values of probe_cc and adds a missing "AS" to the values of
// probe_asn.
func canonicalizeQuery(query url.Values) url.Values {
	canon := url.Values{}
	for key, values := range query {
		if key == "probe_cc" {
			// Country codes have to be uppercase.
			for _, v := range values {
				canon.Add(key, strings.ToUpper(v))
			}
		} else if key == "probe_asn" {
			for _, v := range values {
				// If it's just a number, add an "AS" prefix.
				if _, err := strconv.ParseUint(v, 10, 32); err == nil {
					v = "AS" + v
				}
				v = strings.ToUpper(v)
				canon.Add(key, v)
			}
		} else {
			canon[key] = values
		}
	}
	return canon
}

func logOK(localFilename string) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s ok: %s\n", progress.format(), localFilename)
	progress.mutex.Unlock()
}

func logExists(localFilename string) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s exists: %s\n", progress.format(), localFilename)
	progress.mutex.Unlock()
}

func logError(downloadURL string, err error) {
	progress.mutex.Lock()
	progress.n += 1
	fmt.Printf("%s error: %s: %s\n", progress.format(), downloadURL, err)
	progress.mutex.Unlock()
}

func main() {
	var xz bool

	flag.Usage = usage
	flag.StringVar(&outputDirectory, "directory", outputDirectory, "directory in which to save results")
	flag.BoolVar(&xz, "xz", xz, "compress downloads with xz")
	flag.Parse()

	err := os.MkdirAll(outputDirectory, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	if xz {
		outputExtension = ".xz"
		downloadFilter = newXZFilter
	}

	query, err := parseArgsToQuery(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
	query = canonicalizeQuery(query)

	// The overall structure: processIndex downloads index pages for the
	// given query and feeds the resulting report URLs into downloadURLChan.
	// downloadFromChan reads from downloadURLChan, checks for each URL
	// whether a copy already exists locally, and downloads it if not,
	// writing the result of the download attempt to resultChan and logging
	// any temporary files it creates to tmpFilenameChan. The loop in main
	// reads from resultChan and tmpFilenameChan, renaming temporary files
	// to their final filenames as necessary and keeping track of temporary
	// files that have not been renamed (so they can be deleted in case the
	// program is interrupted).

	downloadURLChan := make(chan string, ooniAPILimit)
	tmpFilenameChan := make(chan string)
	resultChan := make(chan *result)
	go func() {
		// Download indexes and write the URLs they contain to
		// downloadURLChan.
		err = processIndex(query, downloadURLChan)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(1)
		}
		close(downloadURLChan)
	}()
	// Start concurrent downloader threads.
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
	// Things we need to know for the final exit code.
	var numErrors uint
	var signalFlag bool

	// Handle SIGINT for cleanup purposes (deleting temporary files).
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
				numErrors += 1
			} else if r.Exists {
				logExists(r.LocalFilename)
			} else {
				err := os.Rename(r.TmpFilename, r.LocalFilename)
				if err != nil {
					logError(r.URL, err)
					numErrors += 1
				} else {
					// This temporary file has been handled;
					// stop tracking it.
					delete(tmpFilenames, r.TmpFilename)
					logOK(r.LocalFilename)
				}
			}
		case <-sigChan:
			signalFlag = true
			break loop
		}
	}

	// Either resultChan was closed or we received a signal. Clean up.
	for tmpFilename := range tmpFilenames {
		err := os.Remove(tmpFilename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot delete temporary: %s", err)
			numErrors += 1
		}
	}

	if numErrors > 0 {
		fmt.Fprintf(os.Stderr, "%d errors occurred\n", numErrors)
	}
	if numErrors > 0 || signalFlag {
		os.Exit(1)
	}
}
