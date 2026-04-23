FROM us-central1-docker.pkg.dev/bespokelabs/nebula-devops-registry/nebula-devops:1.1.0

ENV DISPLAY_NUM=1
ENV COMPUTER_HEIGHT_PX=768
ENV COMPUTER_WIDTH_PX=1024

ENV SKIP_BLEATER_BOOT=1
ENV ALLOWED_NAMESPACES="glitchtip,keycloak"

# Valkey server image for the runtime cache StatefulSet and for the
# backup job's init container that ships valkey-cli into shared scratch.
RUN mkdir -p /var/lib/rancher/k3s/agent/images && \
    apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    -o Dpkg::Options::="--force-confold" \
    skopeo && \
    skopeo copy --override-os linux --override-arch amd64 \
      docker://docker.io/valkey/valkey:7.2-alpine \
      docker-archive:/var/lib/rancher/k3s/agent/images/valkey.tar:docker.io/valkey/valkey:7.2-alpine && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
