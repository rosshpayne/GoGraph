package grmgr

import (
	"fmt"
	"testing"
	"time"
)

var c chan int

func TestAfter(t *testing.T) {
	c = make(chan int)

	snap := make(chan time.Time)

	go func() {
		for {
			select {
			case t := <-time.After(2 * time.Second):
				snap <- t
			}
		}
	}()
	//var dre = 6 * time.Second
	go func(t *testing.T) {
		t.Log("in go....")
		i := 0
		for {
			i++
			if i > 5 {
				return
			}
			time.Sleep(2 * time.Second)
			c <- 44
		}

	}(t)
	i := 0
	for {
		i++
		select {
		case m := <-c:
			handle(m)
		case <-snap:
			t.Log("snapped...")
		}
		if i == 10 {
			break
		}
		fmt.Println("\nfor loop.......")
	}
}

func handle(m int) {
	fmt.Printf("In handle: received %d\n", m)
}
