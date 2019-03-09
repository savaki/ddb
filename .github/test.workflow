workflow "go test" {
  on = "push"
  resolves = [
    "1.12"
  ]
}

action "1.12" {
  uses = "docker://golang:1.12"
  runs = "ls -al"
}
