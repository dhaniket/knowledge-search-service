from app.db.mongodb import (
    check_database_connection,
)

check_database_connection()

print("MongoDB connection successful")
