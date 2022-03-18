# ARK-funds-tracker


## Summary

This projects tracks the stocks in the ARK funds. It downloads the CSV tables with holdings of ARKK ETFs on daily basis. Then a coparison is made between the holdings on the day before. The differnces in holdings are reported. 


The project is in large part inspired by Part Time Larry's video [Tracking ARK Invest ETFs with Python and PostgreSQL](https://www.youtube.com/watch?v=5uW0TLHQg9w&t=4s)

Github: https://github.com/hackingthemarkets/ark-funds-tracker

## Usage
The data is stored in a [Timescale database](https://www.timescale.com).
[Tableplus](https://tableplus.com) relational database GUI is recommended to manage the database.

The database is first installed and run through a docker container using:
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=<password> timescale/timescaledb:latest-pg14
N!B! Replace <password> with your database password.

After that create the database ark_holdings (CREATE DATABASE ark_holdings), connect to the database and run the create_tables.sql script to create the tables.

Run a docker container with the project.



A chron job is scheduled using:
>CLI> chrontab -e

After the file opens in the editor, the following command should be pasted:
>mm hh  *  *   *  /FULL_PATH_TO_THE_FOLDER/ARK-funds-tracker/cron_execute.sh

This will make the *cron_execute.sh* script running each day at hh:mm.


## Note
Currently all downloads are still saved in 'data' folder as csv files. In a mature stage of the project, only database will be used.

