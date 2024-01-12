# create necessary folder for airflow
mkdir -p ./dags ./logs ./plugins ./config

# create .env file
touch -a .env
echo -e "AIRFLOW_UID=$(id -u)" > .env

# build customized image
docker compose build

# run airflow-init service
docker compose up airflow-init

# run the rest of airflow services
docker compose up --build

# delete env file
rm .env

# cleanup
# docker compose down --volumes --rmi all