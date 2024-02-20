package main

import (
	"bufio"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Database represents the structure of the database
type Database struct {
	filename string
	data     map[string][]int
	mutex    sync.Mutex
}

// NewDatabase initializes a new database
func NewDatabase(filename string) *Database {
	return &Database{
		filename: filename,
		data:     make(map[string][]int),
	}
}

// Initialize loads an existing database from a file
func (db *Database) Initialize() error {
	file, err := os.Open(db.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&db.data); err != nil {
		return err
	}

	return nil
}

// Save writes the database to a file
func (db *Database) Save() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	file, err := os.Create(db.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(db.data); err != nil {
		return err
	}

	return nil
}

// Set inserts or updates a key-value pair in the database
func (db *Database) Set(key string, value []int) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.data[key] = value
}

// Get retrieves the value associated with a key from the database
func (db *Database) Get(key string) ([]int, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	value, ok := db.data[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	return value, nil
}

// Delete removes a key-value pair from the database
func (db *Database) Delete(key string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	_, ok := db.data[key]
	if !ok {
		return errors.New("key not found")
	}

	delete(db.data, key)
	return nil
}

// Merge merges the content of two arrays
func (db *Database) Merge(destKey, srcKey string) error {
	dest, ok := db.data[destKey]
	if !ok {
		return errors.New("destination array does not exist")
	}
	src, ok := db.data[srcKey]
	if !ok {
		return errors.New("source array does not exist")
	}

	db.data[destKey] = append(dest, src...)
	return nil
}

// Show prints the content of an array
func (db *Database) Show(key string) error {
	value, ok := db.data[key]
	if !ok {
		return errors.New("array does not exist")
	}

	fmt.Println(value)
	return nil
}

// Sort sorts the content of an array
func (db *Database) Sort(key string) error {
	value, ok := db.data[key]
	if !ok {
		return errors.New("array does not exist")
	}

	sort.Ints(value)
	db.data[key] = value
	return nil
}

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "db-path", ".wkn", "Path to the database file")
	flag.Parse()

	// Ensure the database file path is relative to the current directory
	dbPath = filepath.Join(".", dbPath)

	db := NewDatabase(dbPath)

	// Check if the database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// Initialize a new database
		err := db.Save()
		if err != nil {
			fmt.Println("Error creating database file:", err)
			return
		}
	} else {
		// Load existing database
		err := db.Initialize()
		if err != nil {
			fmt.Println("Error loading database:", err)
			return
		}
	}

	// Start the REPL
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("wkn> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "new":
			if len(parts) < 2 {
				fmt.Println("Usage: new <array_name> [<comma-separated-values>]")
				continue
			}
			key := parts[1]
			var values []int
			if len(parts) > 2 {
				values = parseIntArray(parts[2])
			}
			db.Set(key, values)
			fmt.Println("CREATED")
		case "show":
			if len(parts) != 2 {
				fmt.Println("Usage: show <array_name>")
				continue
			}
			key := parts[1]
			err := db.Show(key)
			if err != nil {
				fmt.Println("Error:", err)
			}
		case "del":
			if len(parts) != 2 {
				fmt.Println("Usage: del <array_name>")
				continue
			}
			key := parts[1]
			err := db.Delete(key)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("DELETED")
			}
		case "merge":
			if len(parts) != 3 {
				fmt.Println("Usage: merge <dest_array_name> <src_array_name>")
				continue
			}
			destKey := parts[1]
			srcKey := parts[2]
			err := db.Merge(destKey, srcKey)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("MERGED")
			}
		case "exit":
			err := db.Save()
			if err != nil {
				fmt.Println("Error saving database:", err)
			}
			fmt.Println("Bye!")
			return
		case "help":
			fmt.Println("Commands:")
			fmt.Println("  new <array_name> [<comma-separated-values>]: Create a new array")
			fmt.Println("  show <array_name>: Print the content of an array")
			fmt.Println("  del <array_name>: Delete an array")
			fmt.Println("  merge <dest_array_name> <src_array_name>: Merge two arrays")
			fmt.Println("  exit: Exit the REPL")
			fmt.Println("  help: Show this help message")
		default:
			fmt.Println("Unknown command:", parts[0])
		}
	}
}

func parseIntArray(s string) []int {
	parts := strings.Split(s, ",")
	var result []int
	for _, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			fmt.Println("Error parsing value:", err)
			return nil
		}
		result = append(result, n)
	}
	return result
}
