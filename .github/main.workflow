workflow "go test" {
  on = "push"
  resolves = [
    "go test 1.12"
  ]
}

action "go test 1.12" {
  uses = "docker://golang:1.12"
  runs = "go test ./... -cover"
}
