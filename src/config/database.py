import os


class DatabaseConnection:
    def _get_default_connection_string(self) -> str:
        db_config = {
            "ENGINE": "sqlite",
            "NAME": "default_db.sqlite3",
        }
        if db_config["ENGINE"] == "sqlite":
            return f"sqlite:///{db_config['NAME']}"
        raise ValueError("Unsupported database engine for default connection.")
    
    def _get_procfit_connection_string(self) -> str:
        db_config = {
            "ENGINE": "mssql",
            "NAME": os.getenv("PROCFIT_DATABASE_NAME", "procfit_db"),
            "HOST": os.getenv("PROCFIT_DATABASE_HOST", "localhost"),
            "PORT": os.getenv("PROCFIT_DATABASE_PORT", "1433"),
            "USER": os.getenv("PROCFIT_DATABASE_USER", "sa"),
            "PASSWORD": os.getenv("PROCFIT_DATABASE_PASSWORD", "your_password"),
        }
        if db_config["ENGINE"] == "mssql":
            return f"mssql+pyodbc://{db_config['USER']}:{db_config['PASSWORD']}@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}?driver=ODBC+Driver+17+for+SQL+Server"
        raise ValueError("Unsupported database engine for procfit connection.")

    def _get_senior_connection_string(self) -> str:
        db_config = {
            "ENGINE": "oracle",
            "NAME": os.getenv("SENIOR_DATABASE_NAME", "senior_db"),
            "HOST": os.getenv("SENIOR_DATABASE_HOST", "localhost"),
            "PORT": os.getenv("SENIOR_DATABASE_PORT", "5432"),
            "USER": os.getenv("SENIOR_DATABASE_USER", "postgres"),
            "PASSWORD": os.getenv("SENIOR_DATABASE_PASSWORD", "your_password"),
        }
        if db_config["ENGINE"] == "oracle":
            return f"oracle+cx_oracle://{db_config['USER']}:{db_config['PASSWORD']}@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
        raise ValueError("Unsupported database engine for senior connection.")
    
    def get_connection_string(self, db_key: str) -> str:
        match db_key:
            case "default":
                return self._get_default_connection_string()
            case "procfit":
                return self._get_procfit_connection_string()
            case "senior":
                return self._get_senior_connection_string()
            case _:
                raise ValueError(f"Unknown database key: {db_key}")

databases = DatabaseConnection()