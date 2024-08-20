FROM --platform=linux/amd64 golang:1.23

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
RUN go mod tidy

COPY . .

RUN go build -o producer .

CMD ["./producer"]
