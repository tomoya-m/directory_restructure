# ベースイメージとして、python 3.11の軽量版を使用する。
FROM python:3.11-slim

# aptをアップデートする。
RUN apt-get update && apt-get upgrade -y

# gitをインストールする。
RUN apt-get install -y git

# requirements.txtでインストールする。
COPY requirements.txt /workspace/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /workspace/requirements.txt

# キャッシュを削除する。
# イメージサイズを小さくするため。
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*
