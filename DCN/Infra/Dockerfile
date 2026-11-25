FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/thiagoralves/OpenPLC_v3.git /opt/OpenPLC_v3

WORKDIR /opt/OpenPLC_v3

RUN ./install.sh docker

EXPOSE 8080

CMD ["./start_openplc.sh"]
