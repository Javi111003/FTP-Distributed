import json
import os
from passlib.hash import bcrypt

# Usuarios a crear
users = {
    "admin": {"password": "admin123", "is_admin": True},
    "usuario1": {"password": "pass123", "is_admin": False},
    "usuario2": {"password": "pass456", "is_admin": False}
}

user_data = {}
for username, config in users.items():
    user_data[username] = {
        "username": username,
        "password_hash": bcrypt.hash(config["password"]),
        "home_dir": f"/{username}",
        "is_admin": config["is_admin"]
    }

with open("/data/metadata/users.json", "w") as f:
    json.dump(user_data, f, indent=2)

print("Usuarios creados exitosamente")
