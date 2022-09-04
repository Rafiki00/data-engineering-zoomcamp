# Learning docker
One of the skills I learnt this week was to do a data ingestion using docker.
The idea was to create a postgres docker container to hold the database and a pgadmin container to manage it.
Docker-compose was used also to ensure communication between both components.
The data pipeline was created using python and it downloads data from a github link and inserts it into the postgres database.

The data can be found here:

https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow
