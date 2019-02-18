#! /bin/sh

cat <<EOF
{"format":"json","doDelete":true,"token":"test-token","topic":"sync2kafka.test2"}
{"k":{"id":1000},"v":{"name":"test id 1000","date":"$(date)"}}
{"k":{"id":1001},"v":{"name":"test id 1001"}}
{"k":{"timestamp":"$(date +%s)"},"v":{"name":"test timestamp"}}
{"EOT":true}
EOF
