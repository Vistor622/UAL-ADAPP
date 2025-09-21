import bcrypt

password = "ggpro"
hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
print(hashed.decode())  # Guardar esto en usuarios.json
