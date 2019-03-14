package main

import (
	"fmt"
	"reflect"
)

type Person struct {
	name string
}

func (p Person) BB() {
	fmt.Println(p.name)
}

type Speaker interface {
	BB()
}

func main() {
	var alex interface{} = Person{"fuck"}
	var x = reflect.ValueOf(alex).Interface().(Speaker)
	x.BB()
}