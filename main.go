package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"log"
)

var GlobalStore = make(map[string]string)

type Transaction struct{
	localStore map[string]string
	next *Transaction
}

type TransactionStack struct{
	top *Transaction
	size int
}

func (ts* TransactionStack) Push(){
	temp := Transaction{localStore: make(map[string]string)}
	temp.next = ts.top
	ts.top = &temp
	ts.size++
}

func (ts* TransactionStack) Pop(){
	if(ts.top == nil){
		log.Fatal("ERROR: No Active Transaction")
	}else{
		node := &Transaction{}
		ts.top = ts.top.next 
		node.next = nil
		ts.size--
	}
}

func (ts* TransactionStack) Peek() *Transaction{
	return ts.top
}

func (ts* TransactionStack) Commit(){
	activeTransaction := ts.Peek()
	if activeTransaction!=nil{
		for key,value := range activeTransaction.localStore{
			GlobalStore[key] = value
			if activeTransaction.next != nil{
				activeTransaction.next.localStore[key] = value
			}
		}
	}else{
		fmt.Print("Nothing to commit")
	}
}

func (ts *TransactionStack) RollBack() {
	if ts.top == nil {
		fmt.Printf("ERROR: No Active Transaction\n")
	} else {
		for key := range ts.top.localStore {
			delete(ts.top.localStore, key)
		}
	}
}

func Get(key string, ts* TransactionStack){
	activeTransaction := ts.Peek()

	if activeTransaction == nil{
		if val,ok := GlobalStore[key]; ok{
			fmt.Printf("%s\n",val)
		}else{
			fmt.Printf("%s not set in store",val)
		}
	}else{
		if val,ok := activeTransaction.localStore[key]; ok{
			fmt.Printf("%s\n",val)
		}else{
			fmt.Printf("%s not set in store",val)
		}
	}
}

func Set(key,value string, ts* TransactionStack){
	activeTransaction := ts.Peek()
	if activeTransaction == nil {
		GlobalStore[key] = value
	} else {
		activeTransaction.localStore[key] = value
	}
}

func main(){
	reader := bufio.NewReader(os.Stdin)
	items := &TransactionStack{}
	for{
		fmt.Printf(">")

		text,_ := reader.ReadString('\n')
		operation := strings.Fields(text)

		switch operation[0] {
		case "BEGIN": items.Push()
		case "ROLLBACK": items.RollBack()
		case "COMMIT": items.Commit(); items.Pop()
		case "END": items.Pop()
		case "SET": Set(operation[1],operation[2],items)
		case "GET": Get(operation[1],items)
		case "STOP": os.Exit(1)
		default:
			fmt.Printf("ERROR: Unrecognised Operation %s\n", operation[0])
		}
	}

}