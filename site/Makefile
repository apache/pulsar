#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BUNDLE = bundle
JEKYLL = $(BUNDLE) exec jekyll

deep_clean:
	rm -rf \
		.bundle \
		.sass-cache \
		api \
		node_modules \
		vendor \
		_data/config \
		_data/admin-rest-api-swagger.json

doxygen_install:
	rm -rf scripts/doxygen
	scripts/doxygen-install.sh

cpp_doc_gen:
	scripts/doxygen-doc-gen.sh

clean_local:
	rm -rf generated

ruby_setup:
	gem install bundler \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true $(BUNDLE) install \
		--path vendor/bundle

python_setup:
	sudo pip install pdoc pygments

setup: ruby_setup
	scripts/htmltest-setup.sh osx

linkcheck_macos:
	scripts/htmltest-osx

travis_setup: ruby_setup python_setup

travis_build: javadoc

travis_publish:
	scripts/publish-website.sh

build: api_docs swagger_docs_build
	scripts/build-all-versions.sh

serve: clean_local
	$(JEKYLL) serve \
		--incremental \
		--livereload \
		--config _config.yml,_config.local.yml

.PHONY: javadoc
javadoc:
	rm -rf api/{admin,client,pulsar-functions}
	scripts/javadoc-gen.sh

python_doc_gen:
	scripts/python-doc-gen.sh

swagger_definition_gen:
	scripts/swagger-definition-gen.sh

swagger_definition_copy:
	(cd $(shell git rev-parse --show-toplevel) && \
		cp pulsar-broker/target/docs/swagger.json site/_data/admin-rest-api-swagger.json)

swagger_docs_build: swagger_definition_gen swagger_definition_copy

protobuf_doc_gen:
	scripts/protobuf-doc-gen.sh

protobuf_setup:
	go get -u github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc

api_docs: javadoc python_doc_gen cpp_doc_gen

publish: deep_clean setup build
