# ------------------------------------------------------------------------
from dkr.isi/builders/golang:1.12.0-3 as build
run apk add --update musl-dev gcc

env p isi.nc/common/sync2kafka

add . /go/src/${p}/
run go install ${p}/cmd/...

# ------------------------------------------------------------------------
from alpine:3.8
entrypoint ["sync2kafka"]
copy --from=build /go/bin/* /bin/
