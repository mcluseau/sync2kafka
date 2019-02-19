# ------------------------------------------------------------------------
from golang:1.11.5-alpine3.8 as build
run apk add --update musl-dev gcc

env p isi.nc/common/sync2kafka

add . /go/src/${p}/
run go install ${p}/cmd/...

# ------------------------------------------------------------------------
from alpine:3.8
entrypoint ["sync2kafka"]
copy --from=build /go/bin/* /bin/
