package main

import (
	"context"
	"log"
	"math"
	"net"
	"time"
)

func makeRateLimitChannel(ctx context.Context, numSlots int, chunkSize int, sleepIntervalMs int64, traceId *int) <-chan int {
	rateLimitChan := make(chan int, numSlots)

	if traceId != nil {
		log.Printf("Pre-loading rate limiter #%d.\n", *traceId)
	}
	for i := 0; i < numSlots; i++ {
		rateLimitChan <- chunkSize
	}
	if traceId != nil {
		log.Printf("Rate limiter #%d is fully-loaded.\n", *traceId)
	}

	go func(ctx context.Context, c chan int) {
		if traceId != nil {
			log.Printf("Rate limiter #%d is launched.\n", *traceId)
		}

		defer func() {
			if traceId != nil {
				log.Printf("Rate limiter #%d is closed.\n", *traceId)
			}
		}()

		for {
			select {
			case c <- chunkSize:
				if traceId != nil {
					log.Printf("Rate limiter #%d just loaded chunk of size %d bytes and will sleep for %d milliseconds.\n", *traceId, chunkSize, sleepIntervalMs)
				}
				time.Sleep(time.Duration(sleepIntervalMs * int64(math.Pow10(6))))
			case <-ctx.Done():
				if traceId != nil {
					log.Printf("Rate limiter #%d received exit signal and is about to return.\n", *traceId)
				}
				close(c)
				return
			}
		}

	}(ctx, rateLimitChan)

	return rateLimitChan
}

type ConnStats struct {
	track map[string]bool
}

/** A simple circular buffer struct */
type CircularBuffer struct {
	/** Buffer that actually holding the data */
	buf []byte

	/** Where should be read from, and (offset + size) % len(buf) is where should be write from. */
	offset int

	/** Number of bytes that are currently holding in this circular buffer */
	size int

	/** Emits when one can immediately get some data from here without blocking.  */
	onData chan bool

	/** Emits when one can immediately write some data to here without blocking. */
	onFreeCapacity chan bool
}

func makeCircularBuffer(cap int) *CircularBuffer {
	circBuf := new(CircularBuffer)
	circBuf.buf = make([]byte, cap)
	circBuf.offset = 0
	circBuf.size = 0
	circBuf.onData = make(chan bool)
	circBuf.onFreeCapacity = make(chan bool)
	return circBuf
}

func (circBuf *CircularBuffer) Capacity() int {
	return len(circBuf.buf)
}

func (circBuf *CircularBuffer) Size() int {
	return circBuf.size
}

func (circBuf *CircularBuffer) FreeCapacity() int {
	return len(circBuf.buf) - circBuf.size
}

func smaller(a, b int) int {
	if b < a {
		a = b
	}

	return a
}

func (circBuf *CircularBuffer) WriteUnsafe(data []byte, offset, limit int) int {
	dstBegin := (circBuf.offset + circBuf.size) % len(circBuf.buf)

	bytesToCopy := smaller(smaller(limit, circBuf.FreeCapacity()), len(circBuf.buf)-dstBegin)

	dstEnd := smaller(dstBegin+bytesToCopy, len(circBuf.buf))

	srcBegin := offset
	srcEnd := srcBegin + bytesToCopy

	n := copy(circBuf.buf[dstBegin:dstEnd], data[srcBegin:srcEnd])
	log.Printf("Copyied chunk of %d bytes size: from data[%d:%d] to circBuf[%d:%d]", n, srcBegin, srcEnd, dstBegin, dstEnd)

	circBuf.size += n
	return n
}

/** Send data to this circular buffer, data[offset:(offset+limit)] would be actually sent, returns how many bytes actually sent. */
func (circBuf *CircularBuffer) Send(ctx context.Context, data []byte, offset, limit int) int {
	bytesSent := 0
	for bytesSent < limit {
		select {
		case <-ctx.Done():
			return bytesSent
		default:
			cap := circBuf.FreeCapacity()
			if cap == 0 {
				<-circBuf.onFreeCapacity
				continue
			}

			bytesSent += circBuf.WriteUnsafe(data, offset+bytesSent, smaller(limit-bytesSent, cap))
		}
	}

	return bytesSent
}

/** Receive some data from this circular buffer, returns how many bytes actually got. */
func (circBuf *CircularBuffer) Receive(ctx context.Context, data []byte, offset, limit int) int {
	bytesReceived := 0
	for {
		select {
		case <-ctx.Done():
			return bytesReceived
		}
	}
}

func doRead(rateLimit <-chan int, circBuf *CircularBuffer, cli net.Conn) <-chan bool {
	finish := make(chan bool)

	go func(done chan bool) {
		sendCtx, cancelSend := context.WithCancel(context.Background())
		defer func() {
			cancelSend()
			done <- true
		}()

		tempBuf := make([]byte, circBuf.Capacity())
		for {
			chunkSize, ok := <-rateLimit
			if !ok {
				return
			}

			n, err := cli.Read(tempBuf[0:chunkSize])
			if err != nil {
				return
			}

			log.Printf("Got %d bytes from %s\n", n, cli.RemoteAddr().String())

			circBuf.Send(sendCtx, tempBuf, 0, n)
		}

	}(finish)

	return finish
}

func doWrite(rateLimit <-chan int, circBuf *CircularBuffer, cli net.Conn) <-chan bool {
	finish := make(chan bool)

	go func(done chan bool) {
		receiveCtx, cancelReceive := context.WithCancel(context.Background())
		defer func() {
			cancelReceive()
			done <- true
		}()

		tempBuf := make([]byte, circBuf.Capacity())
		for {

			chunkSize, ok := <-rateLimit
			if !ok {
				return
			}

			n := circBuf.Receive(receiveCtx, tempBuf, 0, chunkSize)

			n, err := cli.Write(tempBuf[0:n])
			if err != nil {
				return
			}

			log.Printf("Wrote %d bytes to %s\n", n, cli.RemoteAddr().String())
		}

	}(finish)

	return finish
}

func handleClient(cli net.Conn, stats *ConnStats) {
	if stats == nil {
		log.Fatalln("Don't call handleClient with nil ConnStats.")
	}

	const numRLSlots int = 10
	const numRLChunSize int = 1 << 2
	const numRLSleepIntervalMS int64 = 100

	remoteAddr := cli.RemoteAddr().String()
	readTraceId := len(stats.track) << 1
	writeTraceId := readTraceId + 1
	log.Println("Got new connection:", remoteAddr, "readTraceId", readTraceId, "writeTraceId", writeTraceId)
	stats.track[remoteAddr] = true

	readCtx, cancelRead := context.WithCancel(context.Background())
	readRateLimit := makeRateLimitChannel(readCtx, numRLSlots, numRLChunSize, numRLSleepIntervalMS, &readTraceId)
	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writeRateLimit := makeRateLimitChannel(writeCtx, numRLSlots, numRLChunSize, numRLSleepIntervalMS, &writeTraceId)

	defer func() {
		log.Println(remoteAddr, "closed")
		cancelRead()
		cancelWrite()
		cli.Close()
	}()

	bufSize := numRLChunSize << 2
	circBuf := makeCircularBuffer(bufSize)
	log.Println("Buffer len is", circBuf.Capacity(), "bytes")

	readFinish := doRead(readRateLimit, circBuf, cli)
	writeFinish := doWrite(writeRateLimit, circBuf, cli)

	select {
	case <-readFinish:
		return
	case <-writeFinish:
		return
	}
}

func main() {

	connStats := ConnStats{
		track: make(map[string]bool),
	}

	skt, err := net.Listen("tcp", ":9191")
	if err != nil {
		log.Fatalln("Can't bind to 0.0.0.0:9191")
	}

	for {
		cli, err := skt.Accept()
		if err != nil {
			log.Fatalln("Failed to accept new connection.")
		}

		go handleClient(cli, &connStats)
	}
}
