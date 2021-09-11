FROM debian
WORKDIR /home/app
COPY ./target/trino-sxc-360-SNAPSHOT .
