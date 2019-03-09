workflow "go test" {
  on = "push"
  resolves = [
    "coveralls"
  ]
}

action "go test 1.12" {
  uses = "docker://golang:1.12"
  runs = "go test ./... -cover"
}

action "coveralls" {
  uses = "docker://golang:1.11"
  needs = [
    "go test 1.12"
  ]
  runs = [
    "go get github.com/mattn/goveralls",
    "goveralls -repotoken ${COVERALLS_TOKEN}"
  ]
}
