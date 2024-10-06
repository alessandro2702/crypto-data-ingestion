FROM python:3.11
RUN pip install poetry
COPY . /crypto_package
WORKDIR /crypto_package/crypto_data_ingestion
RUN poetry install
CMD ["poetry", "run", "python", "test.py"]