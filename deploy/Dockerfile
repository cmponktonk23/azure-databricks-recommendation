FROM continuumio/miniconda3:4.8.3

COPY requirements.txt /

RUN pip install -r requirements.txt

RUN apt-get update --allow-releaseinfo-change \
    && apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        build-essential \
        libtool \
        python3-dev \
        jq \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "azure-storage-blob>=1.4.0,<2.0.0"

WORKDIR /
COPY . /
RUN chmod +x -R /deploy

CMD ["make", "deploy"]