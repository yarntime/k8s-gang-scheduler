FROM alpine
MAINTAINER yarntime@163.com

ADD kube-scheduler /usr/local/bin/kube-scheduler

ENTRYPOINT ["/usr/local/bin/kube-scheduler"]
