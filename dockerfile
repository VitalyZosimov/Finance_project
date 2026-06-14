FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc && apt-get clean

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# Отключаем проверку SSL для pip
RUN pip config set global.trusted-host "pypi.org files.pythonhosted.org"
RUN pip config set global.cert /dev/null

RUN pip install --no-cache-dir --user \
    apache-airflow-providers-mongo==4.1.0 \
    yfinance==0.2.28 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    scikit-learn==1.3.0 \
    plotly==5.17.0 \
    joblib==1.3.2 \
    pymongo==4.5.0 \
    requests==2.31.0 \
    finnhub-python==2.4.18

RUN pip install --no-cache-dir --user --upgrade "typing_extensions>=4.10.0"