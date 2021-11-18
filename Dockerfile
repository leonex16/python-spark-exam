FROM jupyter/scipy-notebook:33add21fab64 AS jupiter

WORKDIR /home

COPY . .

RUN ["pip", "install", "openpyxl"]
# RUN ["tini", "-g", "--", "start-notebook.sh"] # Setup Jupyter


FROM bitnami/spark:3.2.0-debian-10-r21

CMD ["tail", "-F", "null"]