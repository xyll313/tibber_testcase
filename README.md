# Test for Tibber Technical Interview
## Set up
Initiate local postgresSQL DB by running 

```
docker compose up -d
```

The local DB can be monitored by dbeaver or any other tools you prefer by credentails in `data_transformation/.env`

## To run the code
The first step would be prepare the environment and activate it
```
conda create --name your_env_name python=3.9
conda activate your_env_name
```
Then install packages required in requirements.txt
```
pip install -r requirements.txt

```
To test running the code, simply run the following and then check the desired update in the DB.

```
python data_transformation/main.py
```

`init()` function in `main.py` relies an environment variable `REINIT_DB` to decide whether to remove all related existing schemas, tables and views from DB, set as desired. It is also possible to change DB connections by editing `.env` 

It is also possible to package the code as a docker image and then run it, packging by doing: 
```
docker build -t app_name .
```
And then running it by the following providing postgresSQL DB running also on local machine
```
docker run -e DB_HOST="host.docker.internal" \
           -e DB_PORT="5432" \
           -e DB_USER="myuser" \
           -e DB_PASSWORD="mypassword" \
           -e DB_NAME="aa_41" \
           -e DB_INIT="TRUE"  \
           app-name
```


## CI/CD automated testing
A simple CICD is set up with black formatting and pytest.