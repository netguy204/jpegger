// Examines the EXIF data in a file to determine where it should be linked
// into an output directory structure. If EXIF is not available then the
// files creation time instead.
package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"github.com/coreos/bbolt"
	//"github.com/djherbis/times"
	"github.com/xiam/exif"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

var (
	Database   = flag.String("database", "state.db", "path to persisted state")
	Log        = flag.String("log", "actions.log", "path to result log")
	Extensions = []string{".mov", ".jpg", ".jpeg", ".avi", ".mp4"}

	PreconditionFailed = fmt.Errorf("precondition not met")

	NoFile         []byte = nil
	DiscoveredFile        = []byte{1}
	CopiedFile            = []byte{2}
)

const (
	DateKey     = "Date and Time"
	DateFormat  = "2006:01:02 15:04:05"
	ContentHash = "ContentHash"
	HashWorkers = 3
)

// Where the file date came from.
type DateSource int

const (
	DateSourceExif = DateSource(iota)
	DateSourceFilesystem
)

// Is the path an example of the extensions that we care about?
func ValidName(path string) bool {
	path = strings.ToLower(path)
	for _, ext := range Extensions {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

// Call a function with FileInfo for every file recursively under a
// starting point
func WithFiles(path string, callback func(os.FileInfo, string) error) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		newPath := fmt.Sprintf("%s/%s", path, file.Name())
		if file.IsDir() {
			WithFiles(newPath, callback)
		} else {
			err = callback(file, newPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// A file to link to a new location
type FileStamp struct {
	Path   string
	Time   time.Time
	Source DateSource
	Key    []byte
}

// Compute a unique key based on the contents of the file
func FileKey(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// Transition the state machine for this file from one state to the next.
// Error if the file was not in the anticipated state.
func CommitState(db *bolt.DB, key []byte, reqPrevState, reqNextState []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ContentHash))

		prevState := b.Get(key)
		if bytes.Compare(prevState, reqPrevState) != 0 {
			return PreconditionFailed
		}
		err := b.Put(key, reqNextState)
		if err != nil {
			return err
		}
		return nil
	})
}

// Recursively create a directory if it doesn't exist
func EnsureDir(path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		if os.IsExist(err) {
			return nil
		} else {
			return err
		}
	}
	return nil
}

// Create a path fragment based on a time
func TimePath(time time.Time) string {
	return fmt.Sprintf("%d/%02d", time.Year(), time.Month())
}

func main() {
	flag.Parse()

	// after parsing we should have 2 arguments left (input and output)
	if flag.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "usage: [input directory] [output directory]\n")
		flag.PrintDefaults()
		return
	}

	// attach logger to file
	f, err := os.OpenFile(*Log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)

	input := flag.Arg(0)
	output := flag.Arg(1)

	db, err := bolt.Open(*Database, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create our buckets
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(ContentHash))
		if err != nil {
			return fmt.Errorf("while creating bucket %s: %v", ContentHash, err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	stamps := make(chan FileStamp)

	printExif := func(file os.FileInfo, name string) error {
		if !ValidName(name) {
			return nil
		}

		date := file.ModTime()
		/* doesn't produce expected results
		stat, err := times.Stat(name)
		if err == nil {
			if stat.HasBirthTime() {
				date = stat.BirthTime()
			} else if stat.HasChangeTime() {
				date = stat.ChangeTime()
			}
		}
		*/
		source := DateSourceFilesystem

		data, err := exif.Read(name)
		if err != nil {
			if err != exif.ErrNoExifData {
				return err
			}
		} else {
			dateStr, ok := data.Tags[DateKey]
			if ok {
				maybeDate, err := time.Parse(DateFormat, dateStr)
				if err != nil {
					return err
				}
				date = maybeDate
				source = DateSourceExif
			}
		}

		stamps <- FileStamp{name, date, source, nil}

		return nil
	}

	// start traversing
	go func() {
		err := WithFiles(input, printExif)
		if err != nil {
			log.Fatalf("while traversing files: %v", err)
		}
		close(stamps)
	}()

	hashedStamps := make(chan FileStamp)

	// hash workers
	var wg sync.WaitGroup
	for w := 0; w < HashWorkers; w += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for stamp := range stamps {
				stamp.Key, err = FileKey(stamp.Path)
				if err != nil {
					log.Fatalf("while hashing files: %v", err)
				}
				hashedStamps <- stamp
			}
		}()
	}

	go func() {
		wg.Wait()
		close(hashedStamps)
	}()

	// actually copy the file
	for result := range hashedStamps {
		err = CommitState(db, result.Key, NoFile, DiscoveredFile)
		if err != nil {
			if err == PreconditionFailed {
				log.Printf("skipping handled file: %s", result.Path)
			} else {
				log.Fatalf("while recording file %s: %v", result.Path, err)
			}
		} else {
			// form the path
			directory := fmt.Sprintf("%s/%s", output, TimePath(result.Time))
			path := fmt.Sprintf("%s/%s", directory, path.Base(result.Path))

			err = EnsureDir(directory)
			if err != nil {
				log.Fatalf("while creating directory %s: %v", directory, err)
			}

			err = os.Link(result.Path, path)
			if err != nil {
				log.Fatalf("while linking %s to %s: %v", result.Path, path, err)
			}

			err = CommitState(db, result.Key, DiscoveredFile, CopiedFile)
			if err != nil {
				log.Fatalf("while commiting file %s: %v", result.Path, err)
			}

			log.Printf("finished: %s\n", result.Path)
		}

	}
}