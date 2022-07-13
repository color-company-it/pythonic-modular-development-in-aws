from postgres_orm import PostgresORM

if __name__ == "__main__":
    pgorm = PostgresORM(
        secrets={
            "database": "postgres",
            "user": "postgres",
            "password": "postgrespw",
            "host": "localhost",
            "port": "49153",
        },
        config_filepath="configurations/ddl_configs.yml",
    )

    pgorm.run()
