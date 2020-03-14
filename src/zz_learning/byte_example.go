package main

import (
	"bytes"
	"fmt"
)

func main() {
	const HeadSize = 20
	headBuf := bytes.NewBuffer(make([]byte, 0, HeadSize))
	headData := make([]byte, 0, 20)

	start := 0
	batch := 4
	for {
		headBuf.Write(headData[start : start+batch])
		fmt.Println(headBuf.Len())
		if headBuf.Len() == HeadSize {
			fmt.Println("Done")
			break
		} else {
			start += batch
		}
	}
}
