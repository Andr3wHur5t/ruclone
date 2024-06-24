package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	// Define CLI flags
	n := flag.Int("n", 10, "number of files")
	targetDir := flag.String("targetDir", "", "target directory")
	flag.Parse()

	if *targetDir == "" {
		fmt.Println("Target directory is required")
		return
	}
	// Create the target directory if it doesn't exist
	err := os.MkdirAll(*targetDir, os.ModePerm)
	if err != nil {
		fmt.Println("Failed to create target directory:", err)
		return
	}

	// Generate N number of files inside the target directory
	for i := 1; i <= *n; i++ {
		fileName := fmt.Sprintf("file%d.txt", i)
		filePath := filepath.Join(*targetDir, fileName)

		// Create the file
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Println("Failed to create file:", err)
			return
		}
		defer file.Close()

		// Write some content to the file
		_, err = file.WriteString(fmt.Sprintf("This is file %d", i))
		if err != nil {
			fmt.Println("Failed to write to file:", err)
			return
		}

		fmt.Println("Created file:", filePath)
	}
}
