#CONFIG_PATH=/home/rozz/go/src/projects/log-distributed-system/test
CONFIG_PATH=${HOME}/.proglog/
.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	# Generate CA cert
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

	#Generate server cert
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server


	mv *.pem *.csr ${CONFIG_PATH}
    # END: client

.PHONY: test
test:
#: START: begin
	go test -race ./...

.PHONY: compile
compile:
	protoc log_package/api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--proto_path=.