BUNDLER_VERSION = $(shell cat package.json | jq .versions.bundler | tr -d '"')
BUNDLE = bundle _${BUNDLER_VERSION}_
JEKYLL = ${BUNDLE} exec jekyll
GULP = node_modules/.bin/gulp
RAKE = ${BUNDLE} exec rake

deep_clean:
	rm -rf \
		.bundle \
		.sass-cache \
		api \
		node_modules \
		vendor \
		_data/config \
		_data/admin-rest-api-swagger.json \
		protobuf.json

doxygen_install:
	rm -rf scripts/doxygen
	scripts/doxygen-install.sh

cpp_doc_gen:
	scripts/doxygen-doc-gen.sh

clean_local:
	rm -rf generated

ruby_setup:
	gem install bundler \
		-v ${BUNDLER_VERSION} \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true ${BUNDLE} install \
		--path vendor/bundle

python_setup:
	pip install pdoc pygments

setup: clean_local ruby_setup protobuf_setup doxygen_install python_setup

build:
	scripts/build-all-versions.sh

serve: clean_local
	${JEKYLL} serve \
		--incremental \
		--config _config.yml,_config.local.yml

javadoc:
	rm -rf api
	scripts/javadoc-gen.sh

python_doc_gen:
	scripts/python-doc-gen.sh

swagger_definition_gen:
	scripts/swagger-definition-gen.sh

protobuf_doc_gen:
	scripts/protobuf-doc-gen.sh

protobuf_setup:
	PROTOBUF_PREFIX=$(brew --prefix protobuf)
	rm -rf protoc-gen-doc scripts/protoc-gen-doc
	git clone https://github.com/pseudomuto/protoc-gen-doc
	rm -rf protoc-gen-doc/.git
	mv protoc-gen-doc scripts
	(cd scripts/protoc-gen-doc && PROTOBUF_PREFIX=$(shell brew --prefix protobuf) qmake && make)

api_docs: javadoc protobuf_doc_gen python_doc_gen cpp_doc_gen

publish: deep_clean setup protobuf_doc_gen swagger_definition_gen build
