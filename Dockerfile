FROM pangpanglabs/golang:builder-beta AS builder

WORKDIR /go/src/clearance/clearance-adapter-for-sale-record
COPY . .
# disable cgo
ENV CGO_ENABLED=0
# build steps
RUN echo ">>> 1: go version" && go version
RUN echo ">>> 2: go get" && go get -v -d
RUN echo ">>> 3: go install" && go install

# make application docker image use alpine
FROM pangpanglabs/alpine-ssl
# using timezone
# RUN apk add -U tzdata
WORKDIR /go/bin/
# copy config files to image
COPY --from=builder /go/src/clearance/clearance-adapter-for-sale-record/*.yml ./
# COPY --from=builder /swagger-ui/ ./swagger-ui/
# copy execute file to image
COPY --from=builder /go/bin/clearance-adapter-for-sale-record ./
EXPOSE 8000
CMD ["./clearance-adapter-for-sale-record"]
