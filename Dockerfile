FROM alpine:3.5

COPY ./promec-watcher /bin/promec-watcher

ENV UID 999
ENV GID 999

USER $UID:$GID

ENTRYPOINT ["/bin/promec-watcher"]
