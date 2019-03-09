workflow "test" {
  on = "push"
  resolves = [
    "go test 1.12"
  ]
}

action "go test 1.12" {
  uses = "actions/docker/cli@8cdf801b322af5f369e00d85e9cf3a7122f49108"
  runs = "docker ps"
}
