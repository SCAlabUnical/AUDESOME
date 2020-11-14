ARG OPENJDK_TAG=8u232
FROM openjdk:${OPENJDK_TAG}
ARG SBT_VERSION=1.4.1
RUN mkdir /working/ && \
    cd /working/ && \
    curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm sbt-$SBT_VERSION.deb && \
    apt-get update && \
    apt-get install sbt && \
    cd && \
    rm -r /working/
ENV SPARK_APPLICATION_WORKDIR AUDESOME
WORKDIR /${SPARK_APPLICATION_WORKDIR}
ADD . /AUDESOME
RUN sbt compile
ARG command
CMD sbt run ${command}
