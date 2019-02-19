.PHONY: all;

all: a_swaggerui-packr.go

a_swaggerui-packr.go: ui.go fetch-ui.sh
	./fetch-ui.sh
	packr -z
