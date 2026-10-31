FROM golang:1.27.2-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY scheduler ./scheduler
RUN CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /remex-scheduler ./scheduler/cmd/remex-scheduler
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /remex-scheduler /remex-scheduler
EXPOSE 8980 8081
ENTRYPOINT ["/remex-scheduler"]
