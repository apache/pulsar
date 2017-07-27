BUNDLER_VERSION = 1.15.1
BUNDLE = bundle _${BUNDLER_VERSION}_
JEKYLL = ${BUNDLE} exec jekyll

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
		-v ${BUNDLER_VERSION} \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true ${BUNDLE} install \
		--path vendor/bundle

python_setup:
	sudo pip install pdoc pygments

setup: ruby_setup

travis_setup: ruby_setup python_setup

travis_build: javadoc
	scripts/build-all-versions.sh

travis_publish:
	scripts/publish-website.sh

build: api_docs
	scripts/build-all-versions.sh

serve: clean_local
	${JEKYLL} serve \
		--incremental \
		--config _config.yml,_config.local.yml

javadoc:
	rm -rf api/{admin,client}
	scripts/javadoc-gen.sh

python_doc_gen:
	scripts/python-doc-gen.sh

swagger_definition_copy:
	(cd $(shell git rev-parse --show-toplevel) && \
		cp pulsar-broker/target/docs/swagger.json site/_data/admin-rest-api-swagger.json)

protobuf_doc_gen:
	scripts/protobuf-doc-gen.sh

protobuf_setup:
	rm -rf protoc-gen-doc scripts/protoc-gen-doc
	git clone https://github.com/pseudomuto/protoc-gen-doc
	rm -rf protoc-gen-doc/.git
	mv protoc-gen-doc scripts
	(cd scripts/protoc-gen-doc && PROTOBUF_PREFIX=$(shell brew --prefix protobuf) qmake && make)

api_docs: javadoc python_doc_gen cpp_doc_gen

publish: deep_clean setup swagger_definition_copy build
