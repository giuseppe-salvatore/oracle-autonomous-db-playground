# Oracle/SQLite3 DB python playground
## What is this repository for?

- Just for fun and keep up with DB programming with python
- The repo aims at containing some scripts to operate with minute candle for stock trading and analysis. It uses both SQLite3 and Oracle Autonomous DB
- There are a few useful scripts to migrate the data from a local instance of SQLite DB to the remote one that is the Oracle DB. You can then perform some checks and query candles in both DBs
</br>

## Configuration of Databases
### SQLite3
In this case the configuration is very simple. Set an environment variable named `SQLITE_DB_FILE` and make it point to the db file (and add it to .env.local file if you prefer)... that's it 

### Oracle Autonomous DB  
I have used this provider because Oracle gives you 2 database with up to 20GB for free... so why not?
The configuration needs to set up 3 variables
- `ORACLE_ADB_USER` and `ORACLE_ADB_PASSWORD` are pretty self explanatory. You will need to configure it in your Oracle Cloud dashboard
- `ORACLE_ADB_TLS_CONNECTION_STRING` for this one you will need to configure your db to use TLS and mTLS. You will be able to see a long string in your Oracle Cloud dashboard and you will need to set it as a value for this variable
</br>


## How do I get set up?

- Start your python virtual environment

```
source .venv/bin/activate
```

- Install the dependencies

```
python -m pip install -r requirements.txt user
```

- Import key environment variables

```
source .env.local
```

- Run any of the available python scrips

```
python <script>.py
```

Alternatively, if you don't want to source the config files just look at the scripts folder which contains bash scripts for linux. Each script does it for you.
For example:

```
bash scripts/<script>.sh
```
</br>
</br>

## Who do I talk to?

- Maintainer: Giuseppe Salvatore giuseppe.salvatore@gmail.com
