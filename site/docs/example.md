---
title: Example page
layout: docs
lead: Just for experimentation
tags:
- Foo
- Bar
- Baz
---

{% include config.html id="log4j" %}

Here is an example page.

```java
private class Foo {
        private static final String host = "localhost";

        @Override
        public String toString() {
              // Hello world
              int myInt = (int) integer();
              long l = 5000L;
              double d = 0.755;
              byte a = 0x01;
              return String.format("This is a string: %s", int);
        }

        private <T extends A> foo() {}
}

public interface Bar<T extends Void> {}
```

```go
/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	flag "github.com/spf13/pflag"
)

var (
	godepsFile         = flag.String("godeps-file", "", "absolute path to Godeps.json")
	overrideImportPath = flag.String("override-import-path", "", "import path to be written into the Godeps.json, e.g., k8s.io/client-go")
	ignoredPrefixes    = flag.StringSlice("ignored-prefixes", []string{"k8s.io/"}, "any godep entry prefixed with the ignored-prefix will be deleted from Godeps.json")
	rewrittenPrefixes  = flag.StringSlice("rewritten-prefixes", []string{}, fmt.Sprintf("any godep entry prefixed with the rewritten-prefix will be filled will dummy rev %q; overridden by ignored-prefixes", dummyRev))
)

const dummyRev = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

type Dependency struct {
	ImportPath string
	Rev        string
}

type Godeps struct {
	ImportPath   string
	GoVersion    string
	GodepVersion string
	Packages     []string `json:",omitempty"` // Arguments to save, if any.
	Deps         []Dependency
}

// rewrites the Godeps.ImportPath, removes the Deps whose ImportPath contains "k8s.io/kubernetes" or "k8s.io/apimachinery".
// entries for k8s.io/apimahinery will be written by the publishing robot before publishing to the repository.
func main() {
	flag.Parse()
	var g Godeps
	if len(*godepsFile) == 0 {
		log.Fatalf("absolute path to Godeps.json is required")
	}
	f, err := os.OpenFile(*godepsFile, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("cannot open file %q: %v", *godepsFile, err)
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&g)
	if err != nil {
		log.Fatalf("Unable to parse %q: %v", *godepsFile, err)
	}
	if len(*overrideImportPath) != 0 {
		g.ImportPath = *overrideImportPath
	}
	// removes the Deps whose ImportPath contains "k8s.io/kubernetes"
	i := 0
	for _, dep := range g.Deps {
		ignored := false
		for _, ignoredPrefix := range *ignoredPrefixes {
			if strings.HasPrefix(dep.ImportPath, ignoredPrefix) {
				ignored = true
			}
		}
		if ignored {
			continue
		}
		rewritten := false
		for _, rewrittenPrefix := range *rewrittenPrefixes {
			if strings.HasPrefix(dep.ImportPath, rewrittenPrefix) {
				rewritten = true
			}
		}
		if rewritten {
			dep.Rev = dummyRev
		}
		g.Deps[i] = dep
		i++
	}
	g.Deps = g.Deps[:i]
	b, err := json.MarshalIndent(g, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	n, err := f.WriteAt(append(b, '\n'), 0)
	if err != nil {
		log.Fatal(err)
	}
	if err := f.Truncate(int64(n)); err != nil {
		log.Fatal(err)
	}
}
```

### Table

Foo | Bar
:---|:---
`bar` | Baz
Here is some much longer text | Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

```java
String foo = "bar";
```

```go
func main() {
        println("Hello world")
}
```

{% javadoc ClientConfiguration client com.yahoo.pulsar.client.api.ClientConfiguration %}

{% endpoint GET http://foo.com/foo/:bar/baq/:baz %}
{% endpoint POST http://foo.com/foo/:bar/baq/:baz %}
{% endpoint PUT http://foo.com/foo/:bar/baq/:baz %}
{% endpoint DELETE http://foo.com/foo/:bar/baq/:baz %}

{% popover cluster %}
{% popover subscription %}
{% popover properties %}

{% include admonition.html type="info" content="
Under the hood, both the `pulsar-admin` CLI tool and the Java client use the REST API.

Second paragraph.

Third paragraph.
"%}

{% include admonition.html type='danger' title='Here\'s a really long title just for testing purposes' content='
Something or other.

Fruits:

* Apple
* Orange
* Banana

```java
String foo = "bar";
```

Something else.
' %}

{% include admonition.html type='info' title='Here\'s a really long title just for testing purposes' content='
Something or other.

Fruits:

* Apple
* Orange
* Banana

```java
String foo = "bar";
```

Something else.
' %}

{% include admonition.html type='success' title='Here\'s a really long title just for testing purposes' content='
Something or other.

Fruits:

* Apple
* Orange
* Banana

```java
String foo = "bar";
```

Something else.
' %}

{% include admonition.html type='warning' title='Here\'s a really long title just for testing purposes' content='
Something or other.

Fruits:

* Apple
* Orange
* Banana

```java
String foo = "bar";
```

Something else.
' %}
