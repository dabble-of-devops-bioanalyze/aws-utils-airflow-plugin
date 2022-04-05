FROM continuumio/miniconda3:latest

USER root

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

# Since we're building PDFs this requires a million packages
RUN apt-get update -y \
    && apt-get upgrade -y \
    # dependencies for building Python packages
    && apt-get install -y  \
    apt-utils build-essential \
    # psycopg2 dependencies
    ca-certificates \
    apt-transport-https \
    curl \
    libxslt-dev  \
    gnupg \
    pandoc libpangocairo-1.0-0 \
    libcairo2  lsb-release xdg-utils  \
    python3-lxml \
    gettext \
    texlive-latex-recommended \
    texlive-fonts-recommended \
    texlive-latex-extra \
    latexmk \
    texlive-xetex  \
    && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    && rm -rf /var/lib/apt/lists/*

RUN conda install -y -c conda-forge jupyterlab=2.2.9 jupyter-server-proxy pip
#RUN jupyter labextension install @jupyterlab/server-proxy

COPY ./requirements* /requirements
# All imports needed for autodoc.
RUN pip install -r /requirements/requirements_dev.txt -r /requirements/requirements.txt

ENV PYTHONPATH /app:$PYTHONPATH

WORKDIR /docs

#CMD make livehtml
