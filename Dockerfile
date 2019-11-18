FROM golang:latest

LABEL version="0.0.1"

RUN mkdir /go/src/app

RUN go get -u github.com/golang/dep/cmd/dep

COPY . /go/src/app

WORKDIR /go/src/app

RUN dep ensure
RUN go test -v -race
RUN go build