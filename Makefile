.PHONY: build
build: 
	go build -o nexus ./cmd/main.go ./cmd/completer.go

.PHONY: clean
clean:
	rm -rf ./nexus
