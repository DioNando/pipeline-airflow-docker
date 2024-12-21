FROM apache/airflow:2.10.3

# Passer à l'utilisateur root pour installer les paquets système
USER root

# Mise à jour et installation de vim
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Revenir à l'utilisateur airflow pour les installations de packages Python
USER airflow

# Installer lxml et pymongo
RUN pip install --no-cache-dir pymongo kafka-python-ng
