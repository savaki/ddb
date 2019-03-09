workflow "go test" {
  on = "push"
  resolves = [
    "go test"
  ]
}

action "1.12" {
  uses = "docker://golang:1.12"
  runs = "ls -al"
}
