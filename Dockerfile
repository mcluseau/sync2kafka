# ------------------------------------------------------------------------
from mcluseau/golang-builder:1.13.3 as build

# ------------------------------------------------------------------------
from alpine:3.10
entrypoint ["sync2kafka"]
copy --from=build /go/bin/* /bin/
