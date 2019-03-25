# ------------------------------------------------------------------------
from dkr.isi/builders/golang:1.12.1 as build

# ------------------------------------------------------------------------
from alpine:3.9
entrypoint ["sync2kafka"]
copy --from=build /go/bin/* /bin/
