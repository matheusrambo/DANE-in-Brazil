package main
import "fmt"

import (
	"os"
	"crypto/tls"
	"net/smtp"
	"encoding/pem"
	"bytes"
	"bufio"
	"strconv"
	"time"
	"sync"
	"net"
	"strings"
	"encoding/base64"

)


func getCertificates465(addr string, f *os.File, port string, city string) (bool, string){

    var buf bytes.Buffer
    buf.WriteString(addr)
    buf.WriteString(":")
    buf.WriteString(port)
    fullAddr := buf.String()

    conf := &tls.Config {
        InsecureSkipVerify: true,
        ServerName: addr,
    }

    timeout := 15*time.Second
	dialer := net.Dialer{Timeout: timeout}

	c, err := tls.DialWithDialer(&dialer, "tcp", fullAddr, conf)
    if err != nil {
        return false, err.Error()
    }

    conn, err := smtp.NewClient(c, addr)
    if err != nil {
        return false, err.Error()
    }
    defer conn.Close()

	domainName := "localhost" // domain name of a scanning server, ex. mail.proxy-research.com
    err = conn.Hello(domainName)
    if err != nil {
        return false, err.Error()
    }


	state, ok := conn.TLSConnectionState()
    if !ok {
        return false, "TLSConnectionState Failed"
    }

    certs := state.PeerCertificates

	data := addr + ", " + port + ", " + city + ", Success, "  + strconv.Itoa(len(certs))
	for _, cert := range certs {

		var block = &pem.Block {
			Type: "CERTIFICATE",
			Bytes: cert.Raw,
		}

		aa := pem.EncodeToMemory(block)
		enc := base64.StdEncoding.EncodeToString(aa)
		data = data + ", " + enc

	}

	f.WriteString(data + "\n")

    err = conn.Close()
    if err != nil {
        return false, err.Error()
    }
    return true, ""
}

func getCertificates(addr string, f *os.File, port string, city string) (bool, string){

	var buf bytes.Buffer
	buf.WriteString(addr)
	buf.WriteString(":")
	buf.WriteString(port)
	fullAddr := buf.String()
	//fmt.Println("FULL ADDR: ", fullAddr)
	conf := &tls.Config {
		InsecureSkipVerify: true,
		ServerName: addr,
	}

	timeout := 15*time.Second
	c, err := net.DialTimeout("tcp", fullAddr, timeout)
	if err != nil {
		return false, err.Error()
	}
	
	err = c.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return false, err.Error()
	}

	conn, err := smtp.NewClient(c, addr)
	if err != nil {
		return false, err.Error()
	}

	defer conn.Close()

	domainName := "127.0.0.1" // domain name of a scanning server, ex. mail.proxy-research.com
	err = conn.Hello(domainName)
	if err != nil {
		return false, err.Error()
	}

	err = conn.StartTLS(conf)
	if err != nil {
		return false, err.Error()
	}

	state, ok := conn.TLSConnectionState()
	if !ok {
		return false, "TLSConnectionState Failed"
	}

	certs := state.PeerCertificates

	data := addr + ", " + port + ", " + city + ", Success, "  + strconv.Itoa(len(certs))
	for _, cert := range certs {

		var block = &pem.Block {
			Type: "CERTIFICATE",
			Bytes: cert.Raw,
		}

		aa := pem.EncodeToMemory(block)
		enc := base64.StdEncoding.EncodeToString(aa)
		data = data + ", " + enc

	}

	f.WriteString(data + "\n")

	err = conn.Close()
	if err != nil {
		return false, err.Error()
	}
	return true, ""
}


func run(jobs <- chan string, city string, certFile string, wg *sync.WaitGroup) {
	defer wg.Done()

	certf, err_ := os.Create(certFile)
	if err_ != nil {
		println(err_.Error())
	}

	for dn := range jobs {

		dnSplit := strings.Split(dn, ".")
		port := strings.Split(dnSplit[0], "_")[1]
		domain_mx:= ""
		tam_mx := len(dnSplit) - 3

		switch{
        case tam_mx == 2:
            domain_mx = dnSplit[2] + "." + dnSplit[3]
        case tam_mx == 3:
            domain_mx = dnSplit[2] + "." + dnSplit[3] + "." + dnSplit[4]
        case tam_mx == 4:
            domain_mx = dnSplit[2] + "." + dnSplit[3] + "." + dnSplit[4] + "." + dnSplit[5]
		case tam_mx == 5:
            domain_mx = dnSplit[2] + "." + dnSplit[3] + "." + dnSplit[4] + "." + dnSplit[5] + "." + dnSplit[6]
		case tam_mx == 6:
            domain_mx = dnSplit[2] + "." + dnSplit[3] + "." + dnSplit[4] + "." + dnSplit[5] + "." + dnSplit[6] + "." + dnSplit[7]
        }
		domain_mx := domain_mx + "."
		// fmt.Println("Porta: ", port)
		fmt.Println("MX: ", domain_mx)
		//fmt.Println("tam: ", tam_mx)
		if port == "465" {
			success, err := getCertificates465(domain_mx, certf, port, city)
			if !success {
				err = strings.Replace(err, "\n", " ", -1)
				certf.WriteString(domain_mx + ", " + port + ", " + city + ", False, " + err + "\n")
			}
		} else {
			success, err := getCertificates(domain_mx, certf, port, city)
			if !success {
				err = strings.Replace(err, "\n", " ", -1)
				certf.WriteString(domain_mx + ", " + port + ", " + city + ", False, " + err + "\n")
			}
		}
	}

	certf.Close()
}

func main() {
	args := os.Args[1:]
	
	city := args[0] // location of the scanning serever. ex) virginia
	numThreads, _ := strconv.Atoi(args[1]) // the number of threads
	inputFile := args[2] // seed file
	certPath := args[3] // path of th eoutput file

	jobs := make(chan string)
	var wg sync.WaitGroup

	for w := 0; w < numThreads; w++ {
		wg.Add(1)
		certFile := certPath + "certs_" + strconv.Itoa(w) + ".txt"
		go run(jobs, city, certFile, &wg)
	}

	inputf, err := os.Open(inputFile)
	if err != nil {
		err.Error()
	}
	scanner := bufio.NewScanner(inputf)

	for scanner.Scan() {
		jobs <- scanner.Text()
	}
	close(jobs)
	wg.Wait()

	inputf.Close()
}

