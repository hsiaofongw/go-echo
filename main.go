package main

import (
	"context"
	"log"
	"math"
	"net"
	"time"
)

type MeterRecord struct {
	/** Instant in nanoseconds since UNIX EPOCH */
	instant int64

	/** Meter */
	meter float64

	/** Duration, how long passed since t[0] */
	duration int64

	/** Average speed */
	avg float64

	/** Current speed (or instant speed, slope, etc.) */
	instantSpeed float64

	/** Maximum speed (fatest speed ever saw.) */
	maxSpeed float64
}

type SpeedMeter struct {
	/** [0] for record in t[0], and [1], [2] for records in t[n-1], t[n] respectively. */
	Records []MeterRecord

	/** Number of records */
	N int
}

func NewSpeedMeter() *SpeedMeter {
	mt := new(SpeedMeter)
	mt.Records = make([]MeterRecord, 2)
	mt.N = 0
	return mt
}

func (mt *SpeedMeter) Start() {
	record := MeterRecord{
		instant:      time.Now().UnixNano(),
		meter:        0,
		duration:     0,
		avg:          0,
		instantSpeed: 0,
		maxSpeed:     0,
	}
	mt.Records[0] = record
	mt.Records[1] = record
}

func (mt *SpeedMeter) Track(meter float64) {
	// mt.Records[0] saves the first record
	// mt.Records[1+(mt.N+1)%2] saves the latest (i.e. that are being written) record
	// mt.Records[1+mt.N%2] saves the previous record.
	// i.e. we use mt.Records[1:3] as a ring buffer.

	// Denotations:
	//
	// t[0], t[1], t[2], ... are instants in each record
	// m[0], m[1], m[2], ... are meters, i.e. how long did it complete, or how long did it went, or how many bytes did it transferred.
	// d[0], d[1], d[2], ... are duration, it's is how long it has been since when the Start() method was called.
	// a[0], a[1], a[2], ... are average speeds.
	// v[0], v[1], v[2], ... are instant speeds.
	// Y[0], Y[1], Y[2], ... are maximum speeds per each record.
	//
	// Use following equations to compute:
	//
	// As for integer n = 0:
	// t[0], m[0], d[0], a[0], v[0], and Y[0] are given values (as set in the `Start()` function)

	// As for integer n > 0:
	// t[n] is exactly NOW, how long in has been since the unix epoch, in nanoseconds, one nanosecond is 10^-9 second.
	// m[n] is given as the arugument use to call this function (`meter float64`).
	// d[n] := t[n]-t[n-1]
	// a[n] := (m[n]-m[0])/(t[n]-t[0])
	// v[n] := (m[n]-m[n-1])/(t[n]-t[n-1])
	// Y[n] := max(v[n], Y[n-1])

	mt.N++
	prevIdx := 1 + ((mt.N - 1) % 2)
	idx := 1 + (mt.N % 2)
	t0 := mt.Records[0].instant
	tn := time.Now().UnixNano()
	mt.Records[idx].instant = tn
	m0 := mt.Records[0].meter
	mt.Records[idx].meter = meter
	mt.Records[idx].duration = tn - t0
	mt.Records[idx].avg = (meter - m0) / float64(tn-t0)
	m_prev := mt.Records[prevIdx].meter
	t_prev := mt.Records[prevIdx].instant
	spd := (meter - m_prev) / float64(tn-t_prev)
	mt.Records[idx].instantSpeed = spd
	mt.Records[idx].maxSpeed = math.Max(spd, mt.Records[prevIdx].maxSpeed)
}

type RateLimitConfig struct {
	NumSlots      int
	ChunkSize     int
	SleepInterval time.Duration
}

const numRLSlots int = 10
const numRLChunSize int = 1 << 12
const numRLSleepIntervalMS int64 = 100
const defaultBufSize int = 1 << 10

func GetRateLimitConfig(config *RateLimitConfig) *RateLimitConfig {
	if config == nil {
		config = new(RateLimitConfig)
		config.NumSlots = numRLSlots
		config.ChunkSize = numRLChunSize
		config.SleepInterval = time.Duration(numRLSleepIntervalMS * 1000000)
	}
	return config
}

// returns rate limit in unit of bytes per second
func GetTheoreticalRateLimit() float64 {
	return float64(numRLChunSize) * float64(time.Second.Milliseconds()) / float64(numRLSleepIntervalMS)
}

func RunRateLimitChannel(ctx context.Context, config *RateLimitConfig, usage chan int) {

	config = GetRateLimitConfig(config)

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

func handleClient(cli net.Conn, stats *ConnStats, rlConfig *RateLimitConfig) {
	if stats == nil {
		log.Fatalln("Don't call handleClient with nil ConnStats.")
	}

	remoteAddr := cli.RemoteAddr().String()
	traceId := len(stats.track)
	log.Println("Got new connection:", remoteAddr, "traceId", traceId)
	stats.track[remoteAddr] = true

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
				log.Printf("Peer %s read end closed.\n", remoteAddr)
				return
			}

			usage <- n

			log.Printf("Got chunk of %d bytes size from peer %s\n", n, remoteAddr)
			if n > 0 {

				n, err := cli.Write(tempBuf[0:n])
				if err != nil {
					log.Printf("Peer %s write end closed.\n", remoteAddr)
					return
				}

				log.Printf("Wrote back chunk of %d bytes size to the peer %s\n", n, remoteAddr)

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

	rlConfig := GetRateLimitConfig(nil)

	for {
		cli, err := skt.Accept()
		if err != nil {
			log.Fatalln("Failed to accept new connection.")
		}

		go handleClient(cli, &connStats, rlConfig)
	}
}
