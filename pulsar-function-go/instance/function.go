package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/pulsar/pulsar-function-go/log"
)

type Function interface {
	Process(ctx context.Context, input []byte) ([]byte, error)
}

type pulsarFunction func(ctx context.Context, input []byte) (interface{}, error)

func (function pulsarFunction) Process(ctx context.Context, input []byte) ([]byte, error) {
	output, err := function(ctx, input)
	if err != nil {
		log.Errorf("process function error:%s", err.Error())
		return nil, err
	}

	outputBytes, err := json.Marshal(output)
	if err != nil {
		log.Errorf("json marshal failed:%s", err.Error())
		return nil, err
	}

	return outputBytes, nil
}

func errorHandler(e error) pulsarFunction {
	return func(ctx context.Context, input []byte) (interface{}, error) {
		return nil, e
	}
}

func validateArguments(handler reflect.Type) (bool, error) {
	handlerTakesContext := false
	if handler.NumIn() > 2 {
		return false, fmt.Errorf("handlers may not take more than two arguments, but handler takes %d", handler.NumIn())
	} else if handler.NumIn() > 0 {
		contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
		argumentType := handler.In(0)
		handlerTakesContext = argumentType.Implements(contextType)
		if handler.NumIn() > 1 && !handlerTakesContext {
			return false, fmt.Errorf("handler takes two arguments, but the first is not Context. got %s", argumentType.Kind())
		}
	}

	return handlerTakesContext, nil
}

func validateReturns(handler reflect.Type) error {
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if handler.NumOut() > 2 {
		return fmt.Errorf("handler may not return more than two values")
	} else if handler.NumOut() > 1 {
		if !handler.Out(1).Implements(errorType) {
			return fmt.Errorf("handler returns two values, but the second does not implement error")
		}
	} else if handler.NumOut() == 1 {
		if !handler.Out(0).Implements(errorType) {
			return fmt.Errorf("handler returns a single value, but it does not implement error")
		}
	}
	return nil
}

func NewFunction(inputFunc interface{}) Function {
	if inputFunc == nil {
		return errorHandler(fmt.Errorf("handler is nil"))
	}
	handler := reflect.ValueOf(inputFunc)
	handlerType := reflect.TypeOf(inputFunc)
	if handlerType.Kind() != reflect.Func {
		return errorHandler(fmt.Errorf("handler kind %s is not %s", handlerType.Kind(), reflect.Func))
	}

	takesContext, err := validateArguments(handlerType)
	if err != nil {
		return errorHandler(err)
	}

	if err := validateReturns(handlerType); err != nil {
		return errorHandler(err)
	}

	return pulsarFunction(func(ctx context.Context, input []byte) (interface{}, error) {

		// construct arguments
		var args []reflect.Value
		if takesContext {
			args = append(args, reflect.ValueOf(ctx))
		}
		if (handlerType.NumIn() == 1 && !takesContext) || handlerType.NumIn() == 2 {
			eventType := handlerType.In(handlerType.NumIn() - 1)
			event := reflect.New(eventType)

			if err := json.Unmarshal(input, event.Interface()); err != nil {
				return nil, err
			}

			args = append(args, event.Elem())
		}

		response := handler.Call(args)

		// convert return values into (interface{}, error)
		var err error
		if len(response) > 0 {
			if errVal, ok := response[len(response)-1].Interface().(error); ok {
				err = errVal
			}
		}

		var val interface{}
		if len(response) > 1 {
			val = response[0].Interface()
		}

		return val, err
	})
}

// Rules:
//
// 	* handler must be a function
// 	* handler may take between 0 and two arguments.
// 	* if there are two arguments, the first argument must satisfy the "context.Context" interface.
// 	* handler may return between 0 and two arguments.
// 	* if there are two return values, the second argument must be an error.
// 	* if there is one return value it must be an error.
//
// Valid function signatures:
//
// 	func ()
// 	func () error
// 	func (TIn) error
// 	func () (TOut, error)
// 	func (TIn) (TOut, error)
// 	func (context.Context) error
// 	func (context.Context, TIn) error
// 	func (context.Context) (TOut, error)
// 	func (context.Context, TIn) (TOut, error)
//
// Where "TIn" and "TOut" are types compatible with the "encoding/json" standard library.
// See https://golang.org/pkg/encoding/json/#Unmarshal for how deserialization behaves
func Start(funcName interface{}) {
	function := NewFunction(funcName) // funcName=main.hello
	goInstance := NewGoInstance()
	goInstance.StartFunction(function)
}
