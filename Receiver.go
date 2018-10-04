package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const fileDownloadPort = ":8080"

type receiver struct {
	FileMeta        fileMetaData
	FileStatus      Status
	FileDwnldChan   chan int64
	FileName        string
	FileSource      string
	FileDestination string
	StartTime       time.Time
	EndTime         time.Time
	Mtx             sync.Mutex
	RcvPortList     string
}

func (rcv *receiver) StartFileStreaming() {
	//TODO: take file name and source from the command line or config file
	fileName := "test3.txt"
	src := "http://localhost:1333/"
	dest := "test23.txt"
	portList := ":1334,:1335,:1336,:1337,:1338,:1339"

	rcv.FileName = fileName
	rcv.FileSource = src
	rcv.FileDestination = dest
	rcv.FileStatus = Initialized
	rcv.RcvPortList = portList

	log.Println("Start Time: ", rcv.StartTime)
	if meta, bPresent := rcv.checkFileExistInSrc(fileName); bPresent {
		log.Println("File Exist. Metadata:", string(meta))
		rcv.FileStatus = DownloadInProgress
		rcv.StartTime = time.Now()
		log.Println("Download starts at:", rcv.StartTime)
		rcv.checkDestFileExist()
		go rcv.startDownload(meta)
	} else {
		rcv.FileStatus = FileNotPresent
		log.Println("File Does not exist")
	}
}

func (rcv *receiver) checkDestFileExist() {
	if checkFileExist(rcv.FileDestination) {
		log.Println("Removing the old file.")
		var err = os.Remove(rcv.FileDestination)
		if err != nil {
			log.Println("Faild to delete the file:", err)
		}
	}
}

func (rcv *receiver) checkFileExistInSrc(fn string) ([]byte, bool) {
	var msg []byte
	client := &http.Client{}
	source := rcv.FileSource + "isFilePresent"
	req, err := http.NewRequest("GET", source, nil)
	if err != nil {
		log.Println("Failed to create http request for new FW check.")
		return msg, false
	}
	// pass the current
	req.Header.Set("FileName", fn)
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Failed to call the HTTP Get request:", err)
		return msg, false
	}
	log.Println("Requested file:", fn)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		msg, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println("Failed to read the response body:", err)
			return msg, false
		}
		if len(msg) > 0 {
			return msg, true
		}
		return msg, false
	}
	return msg, false
}

func (rcv *receiver) startDownload(meta []byte) {
	var metaInfo fileMetaData
	err := json.Unmarshal(meta, &metaInfo)
	if err != nil {
		log.Println("Failed to unmarshal the file meta data:", err)
		return
	}
	rcv.FileMeta = metaInfo
	rcv.FileDwnldChan = make(chan int64, metaInfo.NoOfChunks)

	go rcv.downloadInChunks()

	var count int
	for {
		if count >= rcv.FileMeta.NoOfChunks {
			close(rcv.FileDwnldChan)
			break
		}
		select {
		case chunkSeq, ok := <-rcv.FileDwnldChan:
			if ok {
				log.Printf("File Chunk: %d is downloaded successfully.", chunkSeq)
				count++
			}
		}
	}
	log.Println("File Downloaded Completely")
	if rcv.checkFileIntegrity() {
		rcv.FileStatus = DownloadSucceed
		rcv.EndTime = time.Now()
		log.Println("Download starts at:", rcv.StartTime)
		log.Println("Download ends at:", time.Now())
		log.Println("Total duration(in Second): ", time.Since(rcv.StartTime).Seconds())
	} else {
		rcv.FileStatus = DownloadFailed
	}
}

func (rcv *receiver) downloadInChunks() {
	portPool := strings.Split(rcv.RcvPortList, ",")
	temp := portPool
	for chunkSeq := 1; chunkSeq <= rcv.FileMeta.NoOfChunks; {
		if rcv.FileStatus != DownloadSucceed {
			portPool = temp
			for _, port := range portPool {
				portPool = portPool[1:]
				log.Println("Chunk request:", chunkSeq, port, portPool)
				go rcv.startDownloadInChunks(chunkSeq, port, portPool)
				chunkSeq++
				if chunkSeq > rcv.FileMeta.NoOfChunks {
					break
				}
			}
		}
	}

}

func (rcv *receiver) checkFileIntegrity() bool {
	checkSum := getFileCheckSum(rcv.FileDestination)
	// log.Println(string(checkSum))
	// log.Println(string(rcv.FileMeta.Checksum))
	if bytes.Equal(checkSum, rcv.FileMeta.Checksum) {
		log.Println("Checksum matches.")
		return true
	}
	log.Println("Checksum is different.")
	return false
}

func (rcv *receiver) startDownloadInChunks(chunkSeq int, port string, portPool []string) {
	go rcv.listen(port, portPool)
	client := &http.Client{}
	source := rcv.FileSource + "download"
	req, err := http.NewRequest("POST", source, nil)
	if err != nil {
		log.Println("Failed to create http request for new FW check.")
		rcv.FileStatus = DownloadFailed
		return
	}
	metaData, err := json.Marshal(rcv.FileMeta)
	if err != nil {
		log.Println("Failed to marshal the File meta data.")
		rcv.FileStatus = DownloadFailed
		return
	}
	// pass the current
	req.Header.Set("FileMeta", string(metaData))
	req.Header.Set("FileChunk", strconv.Itoa(chunkSeq))
	req.Header.Set("Port", port)
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Failed to call the HTTP Get request:", err)
		rcv.FileStatus = DownloadFailed
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Println("Failed to send download request to the server")
	}
}
func (rcv *receiver) listen(port string, portPool []string) {
	// log.Println("Listening at port: ", port)
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("Failed to listen to the port: %s, %s ", port, err.Error())
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Failed to accept connection. ", err)
			return
		}
		rcv.handleConnection(conn)
		portPool = append(portPool, port)
	}
}

func (rcv *receiver) handleConnection(conn net.Conn) {
	defer conn.Close()
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1500)
	// Read the incoming connection into the buffer.
	log.Println("Waiting for data:")
	len, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}
	err = rcv.processBuffer(buf, len, conn)
	if err != nil {
		log.Println("Failed to process file chunk", err)
		conn.Write([]byte("Failed."))

	} else {
		// Send a response back to person contacting us.
		conn.Write([]byte("Success\n"))
	}
}

func (rcv *receiver) processBuffer(buf1 []byte, length int, conn net.Conn) error {
	headerLen := int(buf1[0])
	header := buf1[1 : headerLen+1]

	log.Printf("Receive side:: header length: %d, header : %s, recvd len: %d", headerLen, string(header), length)
	var meta fileChunkMetaData
	err := json.Unmarshal(header, &meta)
	if err != nil {
		log.Println("failed to Unmarshal the chunk header:", err)
		return err
	}
	log.Printf("Send chunk: %d transfer request.", meta.ChunkSeq)
	conn.Write([]byte("Start Transfer\n"))
	var buffer []byte
	var length1 int64
	for length1 = 0; length1 < meta.ChunkSize; {
		buf := make([]byte, mtuSize)
		len1, err := conn.Read(buf)
		if err != nil {
			log.Println("Rececive failed:", err)
			return err
		}

		length1 += int64(len1)
		buffer = append(buffer, buf[:len1]...)
	}

	chunkCheckSum := getBufCheckSum(buffer)

	if !bytes.Equal(chunkCheckSum, meta.Checksum) {
		log.Println("Checksum is different for chunk:", meta.ChunkSeq)
		return errors.New("Invalid Checksum")
	}
	log.Printf("Chunk: %d checksum matches", meta.ChunkSeq)

	filePosition := (meta.ChunkSeq - 1) * chunkSize
	err = fileWrite(rcv.FileDestination, buffer, meta.ChunkSize, filePosition)
	if err != nil {
		log.Println("Failed to write data on the file:", err)
		return err
	}
	rcv.FileDwnldChan <- meta.ChunkSeq
	return nil
}

func sizeOfFileMeta(meta fileMetaData) int {
	size := int(unsafe.Sizeof(meta))
	size += len(meta.Author)
	size += len(meta.Version)
	size += len(meta.Date)
	size += len(meta.Name)
	size += len(meta.Checksum)
	log.Println("Metadata size: ", size)
	return size
}
