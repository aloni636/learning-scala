# GeoTrellis / Scala devcontainer
# - Spark 3.3.x + Scala 2.13 (via manual install)
# - Java 17 (latest supported for Spark 3.3.x)
# NOTE: eclipse-temurin is how you get openjdk now
FROM eclipse-temurin:17

# Spark binary version
ARG SPARK_VERSION=3.3.4
ARG HADOOP_VERSION=3
ARG SCALA_BINARY_VERSION=2.13

# coursier Scala version
ARG CS_VERSION=2.1.24
ARG SCALA_VERSION=2.13.18
ARG SBT_VERSION=1.11.7
ARG SCALA_CLI_VERSION=1.6.1
ARG AMMONITE_VERSION=3.0.4
# NOTE: 0.14.1 crashes the kernel with `method not found` exception
ARG ALMOND_VERSION=0.14.3
ARG ALMOND_SCALA_VERSION=2.13
ARG SCALAFMT_VERSION=3.10.2

# spark user configuration
# NOTE: eclipse-temurin:17 comes with non root ubuntu user, so this is technically not necessary
# NOTE: UID and GID 1000 is preferred by vscode for workspace mount permissions resolution
ARG USERNAME=ubuntu
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV SPARK_HOME=/opt/spark
# NOTE: When switching spark user to different name 
#       make sure to switch /home/spark to /home/{new username}
ENV PATH="${SPARK_HOME}/bin:/home/${USERNAME}/.local/share/coursier/bin:${PATH}"

# NOTE: set -eux = -e: exit on error, -u: fail on undefined vars, -x: print executed commands	

# Base tools
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    bash \
    less \
    vim \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Spark 3.3.x (Scala 2.13)
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_BINARY_VERSION}.tgz" \
    -o /tmp/spark.tgz \
    && mkdir -p /opt \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_BINARY_VERSION}" "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

# couriser (artifact fetcher)
RUN set -eux; \
    curl -fsSL "https://github.com/coursier/coursier/releases/download/v${CS_VERSION}/cs-x86_64-pc-linux.gz" \
    -o /tmp/cs.gz; \
    gunzip /tmp/cs.gz; \
    install -m 0755 /tmp/cs /usr/local/bin/cs; \
    rm -f /tmp/cs

# spark user & devcontainer workspace dir
# See: https://code.visualstudio.com/remote/advancedcontainers/add-nonroot-user#_creating-a-nonroot-user
# NOTE: ignores if group id already exists; attempts to create a new user if ${USERNAME} with ${USER_UID} does not exist - fail if unsuccessful
RUN set -eux; \
    getent group "${USER_GID}" || groupadd --gid "${USER_GID}" "${USERNAME}" \
    && ( id "${USERNAME}" && [ "$(id -u "${USERNAME}")" = "${USER_UID}" ] \
    || useradd --uid "${USER_UID}" --gid "${USER_GID}" -m -s /bin/bash "${USERNAME}" ) \
    && mkdir -p /workspaces \
    && ( chown -R "${USER_UID}:${USER_GID}" "/home/${USERNAME}" /workspaces || true )

# Install Scala and dev-tools
# NOTE: We `cs install` and not `cs setup` because we want to be explicit about package versions
USER ${USERNAME}
RUN mkdir -p ~/.local/share/coursier/bin \
    && cs install "scala:${SCALA_VERSION}" \
    "scalac:${SCALA_VERSION}" \
    "scala-cli:${SCALA_CLI_VERSION}" \
    "sbt:${SBT_VERSION}" \
    "sbtn:${SBT_VERSION}" \
    "ammonite:${AMMONITE_VERSION}" \
    "scalafmt:${SCALAFMT_VERSION}" \
    --install-dir ~/.local/share/coursier/bin

# Install GDAL to verify Geotrellis results
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Install almond kernel
USER ${USERNAME}
RUN cs launch --use-bootstrap almond:${ALMOND_VERSION} --scala ${ALMOND_SCALA_VERSION} -- --install

# Install AWS CLI v2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscliv2.zip

USER ${USERNAME}