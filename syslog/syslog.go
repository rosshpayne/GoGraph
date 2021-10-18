package syslog

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	param "github.com/GoGraph/dygparam"
)

const (
	logrFlags = log.LstdFlags | log.Lshortfile
)

const (
	logDir  = "/home/rosshpayne/dev/log/GoGraph/"
	logName = "GoGraph"
	idFile  = "log.id"
	Force   = true
)

// global logger - accessible from any routine
var (
	logr    *log.Logger
	logFile string
)

func init() {
	logf := openLogFile()
	logr := log.New(logf, "DB:", logrFlags)
	SetLogger(logr)
	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	Off()
}

func GetLogfile() string {
	return logFile
}

func openLogFile() *os.File {
	//
	// open log id file (contains: a..z) used to generate log files with naming convention <logDIr><logName>.<a..z>.log
	//
	idf, err := os.OpenFile(logDir+idFile, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		log.Fatal(err)
	}
	//
	// read log id into postfix and update and save back to file
	//
	var n int
	postfix := make([]uint8, 1, 1)
	n, err = idf.Read(postfix)
	if err != nil && err != io.EOF {
		log.Fatalf("log: error in reading log.id, %s", err.Error())
	}
	if n == 0 {
		postfix[0] = 'a'
	} else {
		if postfix[0] == 'z' {
			postfix[0] = 'a'
		} else {
			postfix[0] += 1
		}
	}
	// reset file to beginning and save postfix
	idf.Seek(0, 0)
	_, err = idf.Write(postfix)
	if err != nil {
		log.Fatalf("log: error in writing to id file, %s", err.Error())
	}
	err = idf.Close()
	if err != nil {
		panic(err)
	}
	//
	var s strings.Builder
	s.WriteString(logDir)
	s.WriteString(logName)
	s.WriteByte('.')
	s.WriteByte(postfix[0])
	s.WriteString(".log")

	logFile = s.String()

	logf, err := os.OpenFile(s.String(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	return logf
}

func SetLogger(logr_ *log.Logger) { // TODO: does this need to be exposed?
	if logr == nil && logr_ != nil {
		logr = logr_
		logr.Println("===================================== SetLogger ===============================================")
	}
	// TODO: error here (logr is nil)
}

//var logit int
var loggingOn bool

func Off() {
	loggingOn = false
}
func On() {
	loggingOn = true
}

var prefixMutex sync.Mutex

func Log(prefix string, s string, panic ...bool) {

	// check if prefix is on the must log services. These will be logged even if parameter logging is false.
	var logit bool
	for _, s := range param.LogServices {
		if strings.HasPrefix(prefix, s) {
			logit = true
			break
		}
	}
	// abandon logging if any of these conditions are set
	if !logit && !loggingOn && !param.DebugOn {
		return
	}
	// log it
	prefixMutex.Lock()
	logr.SetPrefix(prefix)
	if len(panic) != 0 && panic[0] {
		logr.Panic(s)
		return
	}
	logr.Print(s)
	prefixMutex.Unlock()

}

func Logf(prefix string, format string, v ...interface{}) {

	logr.SetPrefix(prefix)
	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	fmt.Println(format)
	logr.Printf(format, v...)

}