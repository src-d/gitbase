sdk: '2'
go-runtime:
  version: '1.10'
native:
  image: '{{.Language}}:latest'
  build:
    deps:
      - 'echo dependencies'
    run:
      - 'echo build'
    artifacts:
      - path: '/native/native-binary'
        dest: 'native'
  test:
    run:
      - 'echo tests'