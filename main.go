package main

import (
	"crypto/md5"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"gopkg.in/yaml.v2"
)

const (
	StatusPending   = "pnd"
	StatusCompleted = "done"
	StatusConflict  = "cnf"
	StatusDuplicate = "dup"
)

/*
ruclone --state ./state.yml --from /d/ --from /q/ --to /z/ --dry-run

state.yml
```yml
files:
	/d/apple.png:
		md5: 12345
		format: png
		sizeBytes: 123456
		status: pending
		destination: /z/apple.png
	/q/apple.png:
		md5: 12345
		format: png
		sizeBytes: 123456
		status: pending
		destination: /z/apple.png
	/d/meta
		md5: 1233333
		format: json
		sizeBytes: 12
		status: pending
		destination: /z/meta
```

above command clones the data from /d/* and /q/* to the directory /z/ and stores its progress in ./state.yml.


The system works in the following way:
1. We parse the arguments
2. If there is a state file, we load it, otherwise we will create a new one in memory.
State Index Update:
3. We iterate over the files in the from directories, if they dont exist in the state, we add them to the state. Otherwise we dont need to add to index.
State Meta Update:
4. We iterate over the files in the state, if the md5 is not present, we calculate it and simultainously sniff the file format from the bytes and filepath, additionally we calculate the destination path.
Conflict Resolution:
5. If the state has 2 files with the same destnation, and the md5 is different, we mark the status as conflict.
6. If the state has 2 files with the same destination, and the md5 is same, we mark one as pending and the other as duplicated.
Execution:
7. If the status is conflict, duplicate, or completed and skip the file.
8. if a file is present at the destnation, we compare the md5, if they are same, we mark the status as completed, otherwise we mark the status as conflict.
9. We iterate over the files in the state, if the status is pending, we copy the file from the source to the destination and mark the status as completed.

We write the state to the file system every 100 files, every 5 minutes, and on exit.

*/

type (
	FileState struct {
		MD5            string `yaml:"ha"`
		Format         string `yaml:"fo"`
		SizeBytes      int64  `yaml:"by"`
		Status         string `yaml:"st"`
		Origin         string
		Destination    string `yaml:"de"`
		LastModifiedAt time.Time
	}

	ProcessState struct {
		Self  string                `yaml:"self"`
		Files map[string]*FileState `yaml:"files"`

		debounceLastWriteAt time.Time
		debounceMutex       sync.RWMutex

		debounceLastReport      time.Time
		debounceLastReportMutex sync.RWMutex

		opBytesExpected int64
		opEntryCount    int
	}
)

func main() {
	if err := cmd(); err != nil {
		log.Fatal(err)
	}
}

func cmd() error {
	// Parse the arguments
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "   %s [flags] <fromDirs>\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Example:\n   $ %s --state ./state.yml --dry-run --to /z/ /d/ /q/", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "\nFlags:\n")
		flag.PrintDefaults()
	}
	statePath := flag.String("state", "state.yml", "The path to the state file")
	toDir := flag.String("to", "", "The destination directory")
	maxParallel := flag.Uint("max_parallel", 10, "The maximum number of goroutines running in parallel")

	do_copy := flag.Bool("do_copy", false, "execute the copy")
	do_index := flag.Bool("do_index", false, "execute the index generation")

	flag.Parse()
	fromDirs := flag.Args()

	if len(fromDirs) == 0 {
		return fmt.Errorf("at least one from directory is required")
	}
	if len(fromDirs) == 0 {
		return fmt.Errorf("at least one from directory is required")
	}

	if *statePath == "" {
		return fmt.Errorf("--state path is required to be not empty")
	}

	if *toDir == "" {
		return fmt.Errorf("--to directory is required to be not empty")
	}

	// Load the state
	log.Printf("Loading state from '%s'...\n", *statePath)
	state, err := loadState(*statePath)
	if err != nil {
		return err
	}

	if *do_index {
		// Update the state index
		oldEntriesCount := len(state.Files)
		log.Printf("Updating state index from %v with existing entries count %d...\n", fromDirs, oldEntriesCount)
		if err := state.updateStateIndex(fromDirs, *toDir); err != nil {
			return err
		}

		// Calculate the new entry count and size of the diff
		newEntriesCount := len(state.Files) - oldEntriesCount
		diffSize := float64(newEntriesCount) / float64(oldEntriesCount) * 100
		if diffSize >= 100 {
			diffSize = 100
		}

		// Log the new entry count and size of the diff
		log.Printf("New entries count: %d, Diff size: %0.00f%%\n", newEntriesCount, diffSize)

		// Update the state meta
		log.Println("Updating state meta...")
		if err := state.updateStateMeta(uint32(*maxParallel)); err != nil {
			return err
		}

		// Resolve the conflicts
		log.Println("Resolving conflicts...")
		if err := state.resolveMetaConflicts(); err != nil {
			return err
		}
	}

	if *do_copy {
		// Execute the copy
		log.Println("Executing copy...")
		if err := state.executeCopy(uint32(*maxParallel)); err != nil {
			return err
		}
		_ = toDir
	}

	log.Println("Completed requested operations...")
	return nil
}

func loadState(path string) (*ProcessState, error) {
	// Load the state from the file system using the yaml parser
	absP, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	var state ProcessState
	data, err := os.ReadFile(absP)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		state = ProcessState{Files: map[string]*FileState{}, Self: absP}
	} else {
		err = yaml.Unmarshal(data, &state)
		if err != nil {
			return nil, err
		}
	}

	(&state).forceSetAllReportTimes()

	return &state, nil
}

func (p *ProcessState) forceWriteState() error {
	p.debounceMutex.Lock()
	defer p.debounceMutex.Unlock()

	// Write the state to the file system using the yaml parser
	data, err := yaml.Marshal(p)
	if err != nil {
		return err
	}

	if err := os.WriteFile(p.Self, data, 0644); err != nil {
		return err
	}

	// Update the last write time
	p.debounceLastWriteAt = time.Now()
	return nil
}

func (p *ProcessState) debouncedWriteState() {
	// Debounce Every 500ms
	p.debounceMutex.RLock()
	if time.Since(p.debounceLastWriteAt) < 500*time.Millisecond {
		p.debounceMutex.RUnlock()
		return
	}
	p.debounceMutex.RUnlock()
	// Write the state to the file system using the yaml parser
	if err := p.forceWriteState(); err != nil {
		log.Printf("Error debounce writing state: %v\n", err)
		return
	}
}

func (p *ProcessState) forceSetAllReportTimes() {
	// Set the last report time for all files
	p.debounceLastReport = time.Now()

	for path, file := range p.Files {
		file.Origin = path
	}
}

// Force Report the progress to the terminal
func (p *ProcessState) forceReport(opType string) {
	p.debounceLastReportMutex.Lock()
	defer p.debounceLastReportMutex.Unlock()

	// Report count of files processed, report the total file bytes processed in human readable format.
	cnt := 0
	var totalBytes int64
	for _, file := range p.Files {
		if file.LastModifiedAt.After(p.debounceLastReport) {
			cnt++
			totalBytes += file.SizeBytes
		}
	}

	// Update remaining
	p.opBytesExpected -= totalBytes
	p.opEntryCount -= cnt
	if p.opBytesExpected < 0 {
		p.opBytesExpected = 0
	}

	elapsed := time.Since(p.debounceLastReport)
	bytesRate := float64(totalBytes) / elapsed.Seconds()
	filesRate := float64(cnt) / elapsed.Seconds()
	remainingBytes := p.opBytesExpected
	remainingFiles := p.opEntryCount

	var eta time.Duration
	// Use the slower rate as the limiting factor
	etaBytes := time.Duration(float64(remainingBytes) / bytesRate * float64(time.Second))
	etaFiles := time.Duration(float64(remainingFiles) / filesRate * float64(time.Second))

	// More than 20MB/s, use bytes rate as we are likely copying large files
	if bytesRate > 20*1024*1024 {
		eta = etaBytes
	} else {
		eta = etaFiles
	}

	// Print the total progress, and rate of each value.
	log.Printf("Operation: %s, Affected Files: %d, Total Bytes: %s, Rate: %ss, Remaining Bytes: %s, Remaining Op Count: %d, eta: %v\n", opType, cnt, humanizeBytes(totalBytes), humanizeBytes(int64(float64(totalBytes)/time.Since(p.debounceLastReport).Seconds())), humanizeBytes(p.opBytesExpected), p.opEntryCount, eta)

	p.debounceLastReport = time.Now()
}

func humanizeBytes(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
		pb = 1024 * tb
	)

	switch {
	case bytes >= pb:
		return fmt.Sprintf("%.2fPB", float64(bytes)/float64(pb))
	case bytes >= tb:
		return fmt.Sprintf("%.2fTB", float64(bytes)/float64(tb))
	case bytes >= gb:
		return fmt.Sprintf("%.2fGB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.2fMB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.2fKB", float64(bytes)/float64(kb))
	}
	return fmt.Sprintf("%dB", bytes)

}

// Debounced Report the progress to the terminal
func (p *ProcessState) debouncedReport(opType string) {
	p.debounceLastReportMutex.RLock()
	if time.Since(p.debounceLastReport) < 500*time.Millisecond {
		p.debounceLastReportMutex.RUnlock()
		return
	}
	p.debounceLastReportMutex.RUnlock()

	// Report the progress to the terminal
	p.forceReport(opType)
}

// recacluate state index
func (p *ProcessState) updateStateIndex(fromDirs []string, destDir string) error {
	// iterate over the fromDirs

	p.opBytesExpected = 0
	p.opEntryCount = 0

	for _, dir := range fromDirs {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			relPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			if _, ok := p.Files[relPath]; !ok {
				localRelPath, _ := filepath.Rel(dir, path)
				destAbs, _ := filepath.Abs(filepath.Join(destDir, localRelPath))
				p.Files[relPath] = &FileState{
					Status:         StatusPending,
					Destination:    destAbs,
					LastModifiedAt: time.Now(),
				}
			}

			p.debouncedWriteState()
			p.debouncedReport("index")
			return nil
		})
		if err != nil {
			log.Printf("Error walking directory %s: %v\n", dir, err)
		}
	}

	p.forceWriteState()
	p.forceReport("index")
	return nil
}

func (p *ProcessState) updateStateMeta(maxParallel uint32) error {
	// Create a channel to limit the number of goroutines running in parallel
	semaphore := make(chan struct{}, maxParallel)

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	p.opBytesExpected = 0
	p.opEntryCount = len(p.Files)

	// Iterate over the files in the state
	for path, file := range p.Files {
		// Acquire a semaphore to limit the number of goroutines running in parallel
		semaphore <- struct{}{}

		// Increment the wait group counter
		wg.Add(1)

		// Launch a goroutine to update the state meta for each file
		go func(path string, file *FileState) {
			defer func() {
				// Release the semaphore when the goroutine is finished
				<-semaphore

				// Decrement the wait group counter
				wg.Done()
			}()

			// If the md5 is not present, calculate it
			if file.MD5 == "" {
				md5, err := calculateMD5(path)
				if err != nil {
					log.Printf("Error calculating MD5 for file %s: %v\n", path, err)
					return
				}
				file.MD5 = md5
				file.LastModifiedAt = time.Now()
			}

			// If the format is not present, sniff it
			if file.Format == "" {
				format, err := sniffFormat(path)
				if err != nil {
					log.Printf("Error sniffing format for file %s: %v\n", path, err)
					return
				}
				file.Format = format
				file.LastModifiedAt = time.Now()
			}

			// if the bytes is not present, calculate it
			if file.SizeBytes == 0 {
				info, err := os.Stat(path)
				if err != nil {
					log.Printf("Error getting file info for file %s: %v\n", path, err)
					return
				}
				file.SizeBytes = info.Size()
				file.LastModifiedAt = time.Now()
			}

			p.debouncedWriteState()
			p.debouncedReport("indexMeta")
		}(path, file)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	p.forceWriteState()
	p.forceReport("indexMeta")
	return nil
}

func calculateMD5(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

func sniffFormat(path string) (string, error) {
	// Detect the file type using the mimetype library
	mime, err := mimetype.DetectFile(path)
	if err != nil {
		return "", err
	}
	return mime.String(), nil
}

func (p *ProcessState) resolveMetaConflicts() error {
	byDest := make(map[string][]*FileState)
	for _, file := range p.Files {
		byDest[file.Destination] = append(byDest[file.Destination], file)
	}

	p.opBytesExpected = 0
	p.opEntryCount = len(p.Files)

	// Iterate over the files in the state
	for _, file := range p.Files {
		// If the status is conflict, duplicate, or completed, skip the file
		if file.Status == StatusConflict || file.Status == StatusDuplicate || file.Status == StatusCompleted {
			continue
		}
		// Check if the file is present at the destination
		_, err := os.Stat(file.Destination)
		if err == nil {
			// File exists, compare the md5
			md5, err := calculateMD5(file.Destination)
			if err != nil {
				log.Printf("Error calculating MD5 for file %s: %v\n", file.Destination, err)
				continue
			}
			if md5 == file.MD5 {
				// MD5 is same, mark the status as completed
				file.Status = StatusCompleted
				file.LastModifiedAt = time.Now()
			} else {
				// MD5 is different, mark the status as conflict
				file.Status = StatusConflict
				file.LastModifiedAt = time.Now()
			}
		} else if os.IsNotExist(err) {
			// File does not exist, mark the status as pending
			file.Status = StatusPending
		} else {
			// Error occurred while checking file existence
			log.Printf("Error checking file existence for file %s: %v\n", file.Destination, err)
		}

		if file.Status == StatusPending {
			// Check if a peer file exists with the same destination
			peers := byDest[file.Destination]
			if len(peers) > 1 {
				// Iterate over the peer files
				for _, peer := range peers {
					// If the md5 is same, mark the status as duplicated
					if peer.MD5 == file.MD5 {
						peer.Status = StatusDuplicate
						file.Status = StatusPending
						file.LastModifiedAt = time.Now()
						peer.LastModifiedAt = time.Now()
					} else {
						// If the md5 is different, mark the status as conflict
						peer.Status = StatusConflict
						file.Status = StatusConflict
						file.LastModifiedAt = time.Now()
						peer.LastModifiedAt = time.Now()
					}
				}
			}
		}

		p.debouncedWriteState()
		p.debouncedReport("conflictResolution")
	}
	p.forceWriteState()
	p.forceReport("conflictResolution")
	return nil
}

func (p *ProcessState) executeCopy(maxParallel uint32) error {
	// Create a channel to limit the number of goroutines running in parallel
	semaphore := make(chan struct{}, maxParallel)

	intermediateDirMutex := sync.RWMutex{}

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Sort by largest files to smallest files
	p.opBytesExpected = 0
	p.opEntryCount = 0
	rankedFiles := make([]*FileState, 0, len(p.Files))
	for _, file := range p.Files {
		if file.Status != StatusPending {
			continue
		}
		rankedFiles = append(rankedFiles, file)
		p.opBytesExpected += file.SizeBytes
		p.opEntryCount += 1
	}
	// Sort the files by size
	sort.Slice(rankedFiles, func(i, j int) bool {
		return rankedFiles[i].SizeBytes > rankedFiles[j].SizeBytes
	})

	// Iterate over the files in the state
	for path, file := range p.Files {
		// If the status is conflict, duplicate, or completed, skip the file
		if file.Status != StatusPending {
			continue
		}

		// Acquire a semaphore to limit the number of goroutines running in parallel
		semaphore <- struct{}{}

		// Increment the wait group counter
		wg.Add(1)

		// Launch a goroutine to copy the file
		go func(path string, file *FileState) {
			defer func() {
				// Release the semaphore when the goroutine is finished
				<-semaphore

				// Decrement the wait group counter
				wg.Done()
			}()

			// Create the intermediate directories
			intermediateDirMutex.Lock()
			if err := os.MkdirAll(filepath.Dir(file.Destination), 0755); err != nil {
				log.Printf("Error creating intermediate directories for file %s: %v\n", file.Destination, err)
				intermediateDirMutex.Unlock()
				return
			}
			intermediateDirMutex.Unlock()

			// Copy the file from the source to the destination
			srcFile, err := os.Open(path)
			if err != nil {
				log.Printf("Error opening file %s: %v\n", path, err)
				return
			}
			defer srcFile.Close()

			destFile, err := os.Create(file.Destination)
			if err != nil {
				log.Printf("Error creating file %s: %v\n", file.Destination, err)
				return
			}
			defer destFile.Close()

			_, err = io.Copy(destFile, srcFile)
			if err != nil {
				log.Printf("Error copying file %s to %s: %v\n", path, file.Destination, err)
				return
			}

			// Mark the status as completed
			file.Status = StatusCompleted
			file.LastModifiedAt = time.Now()

			p.debouncedWriteState()
			p.debouncedReport("copy")
		}(path, file)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	p.forceWriteState()
	p.forceReport("copy")

	return nil
}
