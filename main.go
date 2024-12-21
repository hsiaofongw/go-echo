package main

import (
	"log"
	"net"
)

func handleClient(cli net.Conn) {
	remoteAddr := cli.RemoteAddr().String()
	log.Println("Got new connection:", remoteAddr)

	defer func() {
		log.Println(remoteAddr, "closed")
		cli.Close()
	}()

	buf := make([]byte, 2<<10)
	log.Println("Buffer len is", len(buf), "bytes")

	for {
		n, err := cli.Read(buf)
		if err != nil {
			break
		}

		log.Println("Got", n, "bytes from", remoteAddr)

		n, err = cli.Write(buf[0:n])
		if err != nil {
			break
		}

		log.Println("Wrote", n, "bytes to", remoteAddr)
	}
}

func main() {

	skt, err := net.Listen("tcp", ":9191")
	if err != nil {
		log.Fatalln("Can't bind to 0.0.0.0:9191")
	}

	defer func() {
		log.Printf("Exiting... ")
		skt.Close()
		log.Println("Bye!")
	}()

	for {
		cli, err := skt.Accept()
		if err != nil {
			log.Fatalln("Failed to accept new connection.")
		}

		go handleClient(cli)
	}
}
