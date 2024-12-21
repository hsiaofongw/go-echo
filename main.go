package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

type RateLimitConfig struct {
	NumSlots      int
	ChunkSize     int
	SleepInterval time.Duration
}

const numRLSlots int = 10
const numRLChunSize int = 1 << 3
const numRLSleepIntervalMS int64 = 400
const defaultBufSize int = 1 << 10

func RunRateLimitChannel(ctx context.Context, config *RateLimitConfig, usage chan int) {
	if config == nil {
		config = new(RateLimitConfig)
		config.NumSlots = numRLChunSize
		config.ChunkSize = numRLChunSize
		config.SleepInterval = time.Duration(numRLSleepIntervalMS * 1000000)
	}

	rl := make(chan int, config.NumSlots)

	for i := 0; i < cap(rl); i++ {
		rl <- config.ChunkSize
	}

	go func() {
		ticker := time.NewTicker(config.SleepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ticker.C:
				if !ok {
					return
				}

				if len(rl) < cap(rl) {
					rl <- config.ChunkSize
				}
			}
		}

	}()

	go func() {
		var quota int = 0
		for {
			chunkSize, ok := <-usage
			if !ok {
				return
			}

			for quota < chunkSize {
				quota += <-rl
			}

			quota -= chunkSize
		}
	}()
}

type ConnStats struct {
	track map[string]bool
}

func handleClient(cli net.Conn, stats *ConnStats) {
	if stats == nil {
		log.Fatalln("Don't call handleClient with nil ConnStats.")
	}

	remoteAddr := cli.RemoteAddr().String()
	traceId := len(stats.track)
	log.Println("Got new connection:", remoteAddr, "traceId", traceId)
	stats.track[remoteAddr] = true

	var rlConfig *RateLimitConfig = nil

	rechargingCtx, cancalRecharging := context.WithCancel(context.Background())
	usage := make(chan int)
	RunRateLimitChannel(rechargingCtx, rlConfig, usage)

	defer func() {
		log.Println(remoteAddr, "closed")
		cancalRecharging()
		cli.Close()
	}()

	done := make(chan bool)
	go func() {

		defer func() {
			close(usage)
			done <- true
		}()

		tempBuf := make([]byte, defaultBufSize)
		for {
			n, err := cli.Read(tempBuf)
			if err != nil {
				fmt.Printf("Peer %s read end closed.\n", remoteAddr)
				return
			}

			fmt.Printf("Got chunk of %d bytes size from peer %s\n", n, remoteAddr)
			if n > 0 {

				n, err := cli.Write(tempBuf[0:n])
				if err != nil {
					fmt.Printf("Peer %s write end closed.\n", remoteAddr)
					return
				}

				fmt.Printf("Wrote back chunk of %d bytes size to the peer %s\n", n, remoteAddr)

			}
		}
	}()

	<-done
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
