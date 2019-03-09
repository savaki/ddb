workflow "go test" {
  on = "push"
  resolves = [
    "coveralls",
  ]
}

action "go test 1.12" {
  uses = "docker://golang:1.12"
  runs = [
    "sh",
    "-c",
    "go test ./... -cover -race -coverprofile=/tmp/coverage.out; (cd /; go get github.com/mattn/goveralls); goveralls -coverprofile=/tmp/coverage.out -repotoken ${COVERALLS_TOKEN}"
  ]
  secrets = [
    "COVERALLS_TOKEN"
  ]
}

