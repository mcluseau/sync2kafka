# ------------------------------------------------------------------------
from mcluseau/golang-builder:1.12.5 as build

# ------------------------------------------------------------------------
from alpine:3.9
entrypoint ["sync2kafka"]
copy --from=build /go/bin/* /bin/
