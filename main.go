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
	Database        = flag.String("database", "state.db", "path to persisted state")
	Log             = flag.String("log", "actions.log", "path to result log")
	DeleteCopyState = flag.Bool("delete-copy-state", false, "delete the memory of what we've copied. does not forget hashes")

	Extensions   = []string{".mov", ".jpg", ".jpeg", ".avi", ".mp4"}
	SkipPatterns = []string{".AppleDouble"}
	ExifKeys     = []string{
		"Date and Time (Original)",
		"Date and Time (Digitized)",
		"Create Date",
	}

	PreconditionFailed = fmt.Errorf("precondition not met")

	NoFile         []byte = nil
	DiscoveredFile        = []byte{1}
	CopiedFile            = []byte{2}
)

const (
	DateKey     = "Date and Time"
	DateFormat  = "2006:01:02 15:04:05"
	ContentHash = "ContentHash"
	SourcePath  = "SourcePath"
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
	for _, pat := range SkipPatterns {
		if strings.Contains(path, pat) {
			return false
		}
	}

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
func FileKey(db *bolt.DB, path string) ([]byte, error) {
	var cachedKey []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(SourcePath))
		cachedKey = b.Get([]byte(path))
		return nil
	})
	if err != nil {
		return nil, err
	}

	if cachedKey != nil {
		return cachedKey, nil
	}

	// otherwise, compute the hash
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
func CommitState(db *bolt.DB, path string, key, reqPrevState, reqNextState []byte) (bool, error) {
	transitioned := false

	rErr := db.Update(func(tx *bolt.Tx) error {
		// associate the key with the path
		b2 := tx.Bucket([]byte(SourcePath))
		err := b2.Put([]byte(path), key)
		if err != nil {
			return err
		}

		// record the state transition
		b := tx.Bucket([]byte(ContentHash))
		prevState := b.Get(key)
		if bytes.Compare(prevState, reqPrevState) != 0 {
			return nil
		}
		err = b.Put(key, reqNextState)
		if err != nil {
			return err
		}
		transitioned = true

		return nil
	})

	return transitioned, rErr
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
		if *DeleteCopyState {
			err := tx.DeleteBucket([]byte(ContentHash))
			if err != nil {
				panic(err)
			}
		}

		_, err := tx.CreateBucketIfNotExists([]byte(ContentHash))
		if err != nil {
			return fmt.Errorf("while creating bucket %s: %v", ContentHash, err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(SourcePath))
		if err != nil {
			return fmt.Errorf("while creating bucket %s: %v", SourcePath, err)
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
			for _, key := range ExifKeys {
				dateStr, ok := data.Tags[key]
				if ok {
					maybeDate, err := time.Parse(DateFormat, dateStr)
					if err != nil {
						return err
					}
					date = maybeDate
					source = DateSourceExif
					break
				}
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
				stamp.Key, err = FileKey(db, stamp.Path)
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
		transitioned, err := CommitState(db, result.Path, result.Key, NoFile, DiscoveredFile)
		if err != nil {
			log.Fatalf("while recording file %s: %v", result.Path, err)
		}

		if !transitioned {
			log.Printf("skipping handled file %s", result.Path)
			continue // file wasn't in the expected state
		}

		// form the path
		baseName := path.Base(result.Path)
		directory := fmt.Sprintf("%s/%s", output, TimePath(result.Time))
		destPath := fmt.Sprintf("%s/%s", directory, baseName)

		err = EnsureDir(directory)
		if err != nil {
			log.Fatalf("while creating directory %s: %v", directory, err)
		}

		err = os.Link(result.Path, destPath)
		if err != nil {
			if os.IsExist(err) {
				// try an alternative path
				keyFragment := fmt.Sprintf("%x", result.Key)[:8]
				destPath = fmt.Sprintf("%s/%s_%s", directory, keyFragment, baseName)
				err = os.Link(result.Path, destPath)
			}

			// check again because it may have changed as a result of IsExist
			if err != nil {
				log.Fatalf("while linking: %v", err)
			}
		}

		_, err = CommitState(db, result.Path, result.Key, DiscoveredFile, CopiedFile)
		if err != nil {
			log.Fatalf("while commiting file %s: %v", result.Path, err)
		}

		log.Printf("finished: %s\n", result.Path)
	}
}
