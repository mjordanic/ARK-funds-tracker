FROM alpine:latest

WORKDIR /ark_tracker

# install python dependencies
COPY requirements /ark_tracker/requirements/

ENV PYTHONUNBUFFERED=1

RUN apk update &&\
    apk upgrade &&\
    apk add git &&\
    # Install python/pip
    apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python  &&\
    python3 -m ensurepip  &&\
    pip3 install --no-cache --upgrade pip setuptools &&\
    pip install --no-cache-dir -U --prefer-binary -r requirements/requirements.txt

# RUN dnf update --refresh -y -qqq \
#     && dnf upgrade -y -qqq \
#     && dnf install python3-pip busybox -y -qqq \
#     && dnf install make automake gcc gcc-c++ python-devel freetype-devel -y -qqq \
#     && pip install --upgrade pip wheel \
#     && pip install --no-cache-dir -U --prefer-binary -r requirements/requirements.txt \
#     && dnf remove make automake gcc gcc-c++ python-devel freetype-devel -y -qqq \
#     && dnf clean all

# copy code
COPY . /ark_tracker/

# ADD docker/crontab /etc/crontab

# CMD supercronic -debug /etc/crontab