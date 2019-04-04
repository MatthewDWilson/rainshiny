# rainshiny
R Shiny app which demonstrates near-real time statistical analysis of rain gauge data.

## Database
A PostgreSQL/ PostGIS database server is required. I recommend you use Docker for this, although any server should do fine. A suitable Docker image which includes a database with rainfall data tables (though it's not kept up-to-date) is available at:
https://cloud.docker.com/repository/docker/emdie/rainshinydb

This is a small extension of the camptocamp/docker-postgres image available at:
https://github.com/camptocamp/docker-postgres

