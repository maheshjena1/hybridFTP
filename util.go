package main

import (
	"crypto/md5"
	"crypto/sha512"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

const chunkSize = 2 * 1024 * 1024 // 2 MB
const mtuSize = 64 * 1024

var mtx sync.Mutex

type fileMetaData struct {
	Name       string
	Length     int64
	Checksum   []byte
	NoOfChunks int
	ChunkSize  int
	Author     string
	Version    string
	Date       string
	Sources    []string
}

type fileChunkMetaData struct {
	FileName  string
	ChunkSeq  int64
	Checksum  []byte
	ChunkSize int64
}

type Status int

const (
	Initialized Status = 1 + iota
	FileNotPresent
	DownloadInProgress
	DownloadFailed
	DownloadSucceed
)

func createFileMeta(fileName string) string {
	var meta fileMetaData
	meta.Name = fileName
	meta.Checksum = getFileCheckSum(fileName)
	meta.Length = getFileLength(fileName)
	meta.ChunkSize = getChunkSize()
	meta.NoOfChunks = int((meta.Length / int64(meta.ChunkSize)) + 1)
	meta.Author = "thaliava"
	meta.Version = getFileVersion(fileName)
	meta.Date = getFileDate(fileName)
	metaInfo, err := json.Marshal(meta)
	if err != nil {
		log.Println("Failed to marshal the file meta info:", err)
		return ""
	}
	return string(metaInfo)
}

func getFileCheckSum(fileName string) []byte {
	// mtx.Lock()
	// defer mtx.Unlock()
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return h.Sum(nil)
}

func getFileLength(fName string) int64 {
	// mtx.Lock()
	// defer mtx.Unlock()
	stat, err := os.Stat(fName)
	if err != nil {
		return -1
	}
	return stat.Size()
}
func getChunkSize() int {
	// TODO: Chunk size needs to be configurable
	return chunkSize
}

func getFileVersion(fName string) string {
	// TODO: get version of the file
	return "V1.0"
}

func getFileDate(fName string) string {
	// mtx.Lock()
	// defer mtx.Unlock()
	stat, err := os.Stat(fName)
	if err != nil {
		return ""
	}
	return stat.ModTime().String()
}

func getBufCheckSum(buf []byte) []byte {
	// mtx.Lock()
	// defer mtx.Unlock()
	h := sha512.New()
	io.WriteString(h, string(buf))
	// log.Println(n, err)
	return h.Sum(nil)
}

func createChunkHeader(seq int64, buf []byte, bufLen int64, fName string) ([]byte, error) {
	var chunkMeta fileChunkMetaData
	chunkMeta.ChunkSeq = seq
	chunkMeta.ChunkSize = bufLen
	chunkMeta.FileName = fName
	chunkMeta.Checksum = getBufCheckSum(buf)
	meta, err := json.Marshal(chunkMeta)
	if err != nil {
		log.Println("Failed to marshal the chunk metadata:", err)
		return meta, err
	}
	return meta, err
}

func fileWrite(fileName string, buffer []byte, length, filePosition int64) error {
	mtx.Lock()
	defer mtx.Unlock()
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	// _, err = f.Seek(filePosition, 0)
	// if err != nil {
	// 	log.Println("Failed to seek to the position:", err)
	// 	return err
	// }
	log.Printf("File writing at position:%d with %d KB chunk", filePosition, len(buffer)/1024)
	_, err = f.WriteAt(buffer[:length], filePosition)
	return err
}

func getFileChunkSize(fileMeta fileMetaData, chunkSeq int) int64 {
	chunkTotal := fileMeta.NoOfChunks
	if chunkSeq != chunkTotal {
		return chunkSize
	}

	file, err := os.Open(fileMeta.Name)
	if err != nil {
		log.Println("Failed to open the file:", err)
		return -1
	}
	defer file.Close()
	fileinfo, err := file.Stat()
	if err != nil {
		log.Println("Failed to get properties of the file:", err)
		return -1
	}
	filesize := fileinfo.Size()
	return filesize - int64(((chunkSeq - 1) * chunkSize))
}

func checkFileExist(fName string) bool {
	if _, err := os.Stat(fName); err == nil {
		return true
	}
	return false
}
