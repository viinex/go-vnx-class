FROM docker.io/bitnami/minideb:bookworm

COPY vnx-class.deb /tmp
RUN apt-get install -y /tmp/vnx-class.deb ; rm /tmp/vnx-class.deb
COPY web-dist /usr/share/viinex/web

EXPOSE 8080

ENV STATIC=/usr/share/viinex/web/browser/en
ENV ETCD_ENDPOINTS=etcd:2379
ENV ETCD_USERNAME="vnxclass"
ENV ETCD_PASSWORD="vnxclass"
ENV WAMP=0.0.0.0:8080
ENV PROMETHEUS_PUSH_URI=http://victoria-metrics:8428/api/v1/import/prometheus

USER 1001

CMD ["/usr/bin/vnx-class"]
