### Exercise instructions:
The goal is to create a DAG to create a medallion architecture with its 4 directories:
- landing (where the original json files will be storage)
- bronze (transform from json to parquet)
- silver (exclude the prefix from the column names)
- gold (join the 3 dataframes in one with especific columns)

Step by step:
1. Install airflow (read below the instructions to install and to run airtflow)
2. Create the landing directory with the json files (customers, orders and orders_items) inside of it
3. Create 1 Python script with 3 different funcitons, one for each transformation step (as I did in 'tarefas.py')
4. Create an airflow dag to execute all the python scripts parallelizing the bronze and silver executions


### Instalando o airflow
1. Update the system
- `sudo apt update`
- `sudo apt upgrade`

2. Install Python and its dependencies
- `sudo apt install python3.10 python3.10-venv python3.10-dev`

3. Install and configure virtual environment
- `python3.10 -m venv airflow_venv`
- `source airflow_venv/bin/activate`

4. Install airflow
- `export AIRFLOW_HOME=~/Documentos/airflow` (~/Documentos/airflow is the path to **my airflow directoy**, change it to yours)
- `pip install apache-airflow==2.9.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"`

### Inicializando o airflow 
**These steps should be done every time you initialize your airflow!**

1. Open the terminal in your airflow directory (in **my** case: ~/Documentos/airflow) and activate the venv:
`source airflow_venv/bin/activate`

2. Configure the environment variable
`export AIRFLOW_HOME=~/Documentos/airflow` (change the directory path to yours)

3. Initialize airflow:
`airflow standalone`
- The login and password informations will apear like the image below (or you can find your password in 'standalone_admin_password.txt' file)
![login and password](image.png)

4. Open the url to access your airflow UI:
http://localhost:8080

5. At the end, always turn off your airflow application using the following commands on terminal:
- ctrl + C
- `deactivate`