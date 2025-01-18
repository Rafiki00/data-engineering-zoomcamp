
# Week 1: Docker & Terraform

## Question 1. Understanding docker first run

**Question:**

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

**Answer:**
- `24.3.1`

To find the pip version in the image, I ran a Python container in interactive mode and checked the pip version:
```bash
docker run -it python:3.12.8 bash
pip --version
```


## Question 2. Understanding Docker networking and docker-compose

**Question:**

Given the following `docker-compose.yaml`, what is the hostname and port that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

**Answer:**

- `db:5432`

When working with container-to-container communication within the same Docker Compose network, we use the service name as the hostname. While the port mapping `5433:5432` means port 5433 on the host machine maps to 5432 in the container, pgAdmin needs to use the internal Docker network address and port, which is `db:5432`.