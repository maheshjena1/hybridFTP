package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

type Sender struct {
}

func startListen() {
	var sender Sender
	// Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.POST("/download", (&sender).HandleDownloadReq)
	e.GET("/isFilePresent", (&sender).HandleIsFilePresent)

	// Start Management channel
	e.Logger.Fatal(e.Start(":1333"))
}

//Handle file chunk download request
func (sender *Sender) HandleDownloadReq(c echo.Context) error {
	fileMeta := c.Request().Header.Get("FileMeta")
	rcvPort := c.Request().Header.Get("Port")
	chunkSeq, _ := strconv.Atoi(c.Request().Header.Get("FileChunk"))
	log.Printf("File Download request for %s, on port: %s", string(fileMeta), rcvPort)
	var meta fileMetaData
	err := json.Unmarshal([]byte(fileMeta), &meta)
	if err != nil {
		log.Println("Failed to unmarshal the file meta data:", err)
		return c.String(http.StatusBadRequest, err.Error())
	}
	// return c.File(meta.Name)
	sender.startFileSend(meta, rcvPort, chunkSeq)
	return c.String(http.StatusOK, "ok")
}

func (sender *Sender) startFileSend(meta fileMetaData, port string, chunkSeq int) {
	chunkLen := getFileChunkSize(meta, chunkSeq)
	if chunkLen <= 0 {
		log.Println("Invalid chunk sequence:", chunkSeq)
		return
	}
	filePosition := int64((chunkSeq - 1) * chunkSize)

	file, err := os.Open(meta.Name)
	if err != nil {
		log.Println("Failed to open the file:", err)
		return
	}
	defer file.Close()

	buffer := make([]byte, chunkLen)
	bytesread, err := file.ReadAt(buffer, filePosition)
	if err != nil {
		log.Println("File Read Ends:", err)
		return
	}
	log.Printf("Chunkseq: %d, buffer len: %d, bytesRead: %d, port: %s, file name: %s", chunkSeq, len(buffer), bytesread, port, meta.Name)
	sender.sendFileBuffer(int64(chunkSeq), buffer, int64(bytesread), port, meta.Name)
}

func (sender *Sender) sendFileBuffer(chunkSeq int64, buf []byte, bufLen int64, port, fName string) {
	log.Println("sendFileBuffer: begin")
	chunkHeader, err := createChunkHeader(chunkSeq, buf, bufLen, fName)
	if err != nil {
		log.Println("Failed to create header for the chunk:", err)
		return
	}
	log.Println("chunkHeader:", string(chunkHeader))
	headerLen := len(chunkHeader)
	lenbyte := byte(headerLen)
	var buffer []byte
	buffer = append(buffer, lenbyte)
	buffer = append(buffer, chunkHeader...)
	log.Printf("Sender side:: Chunk: %d Header len: %d, Header: %s, buffer len: %d.", chunkSeq, headerLen, string(chunkHeader), bufLen)
	count := 0
	for {
		if count > 5 {
			log.Println("Maximum retries exceeded, Failed to send chunk", chunkSeq)
			break
		}
		count++
		conn, err := net.Dial("tcp", port)
		if err != nil {
			log.Println("Failed to connect to the receiver: ", err)
		} else {
			defer conn.Close()
			_, err = conn.Write(buffer)
			if err != nil {
				log.Println("Failed to send the chunk to the receiver:", err)
			} else {
				msg, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					log.Println("Failed to receive the receiver response.", err)
				} else if strings.Compare(msg, "Start Transfer\n") == 0 {
					log.Printf("Sending chunk to receiver")
					var len1 int64
					innerCount := 1
					for len1 = 0; len1 < bufLen; {
						if innerCount > 5 {
							break
						}
						var maxLen int64
						if (bufLen - len1) > mtuSize {
							maxLen = mtuSize
						} else {
							maxLen = bufLen - len1
						}
						buffer := buf[len1 : len1+maxLen]
						_, err = conn.Write(buffer)
						if err != nil {
							log.Println("Failed to send the chunk to the receiver:", err)
							innerCount++
						} else {
							len1 += maxLen
						}
					}
					log.Printf("Chunk : %d delivered", chunkSeq)
				}
				msg, err = bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					log.Println("Failed to receive the receiver response.", err)
				} else {
					if strings.Compare(msg, "Success\n") == 0 {
						log.Printf("Receiver received the chunk: %d successfully.", chunkSeq)
						break
					} else {
						log.Println("Receiver failed to process chunk: ", msg)
					}
				}
			}
		}
		// wait for 5 milisecond before next try
		time.Sleep(time.Millisecond * 5)
	}

}

// HandleIsFilePresent : check if the requested file exist, if exist, send File Metadata
func (sender *Sender) HandleIsFilePresent(c echo.Context) error {
	fileName := c.Request().Header.Get("FileName")

	// TODO: add the file path along with the file name
	var fileMetaData string
	if checkFileExist(fileName) {
		fileMetaData = createFileMeta(fileName)
	} else {
		log.Printf("The File: %s is not present.", fileName)
	}

	return c.String(http.StatusOK, fileMetaData)
}
