FROM docker.getcollate.io/openmetadata/ingestion:1.9.17

USER airflow

# Install keyring with version-constrained dependencies to match OpenMetadata requirements
RUN /home/airflow/.local/bin/python -m pip install --no-cache-dir \
    'keyring>=24.0.0,<25.0.0' \
    'jaraco.context==6.0.1' \
    'jaraco.functools>=4.0.0,<4.2.0' \
    'jaraco.classes>=3.0.0,<4.0.0' \
    'importlib-metadata>=6.0,<=8.4.0' \
    'secretstorage>=3.3.0'

# Configure keyring to use null backend
RUN mkdir -p /home/airflow/.local/share/python_keyring && \
    echo "[backend]" > /home/airflow/.local/share/python_keyring/keyringrc.cfg && \
    echo "default-keyring=keyring.backends.null.Keyring" >> /home/airflow/.local/share/python_keyring/keyringrc.cfg

ENV PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring

USER root
