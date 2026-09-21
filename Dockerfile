# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bin/marchimetrics ./cmd/marchimetrics

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/marchimetrics /usr/local/bin/marchimetrics
EXPOSE 8428
VOLUME ["/data"]
ENTRYPOINT ["marchimetrics"]
CMD ["-storageDataPath=/data"]
