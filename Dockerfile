FROM golang:1.20

WORKDIR /app

# copy and build go binary before other files for better build caching
COPY cli.go .
COPY go.mod .
COPY go.sum .

RUN go mod download
RUN go get
RUN go build -o /app/cli.go

COPY . .

RUN ["chmod", "+x", "/app/set_up_and_run.sh"]

ENTRYPOINT /app/set_up_and_run.sh