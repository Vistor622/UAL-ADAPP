import pandas as pd
import mysql.connector as mc

cliente = pd.read_csv("clientes.csv")
usuarios = pd.read_csv("usuarios.csv")

conn = mc.connect(
    host="localhost",  
    user="root",
    password="1234",
    database="crm"
)
cursor = conn.cursor()



for _, row in cliente.iterrows(): # _, sirve para ignorar el valor
    sql="""
    INSERT INTO Clientes(cliente_id,nombre,apellido,email,FechaRegistro)
    values(%s,%s,%s,%s,STR_TO_DATE(%s, '%d/%m/%Y %H:%i'))
    ON DUPLICATE KEY UPDATE
    FechaRegistro = VALUES(FechaRegistro);
    """
    #STR_TO_DATE AYUDA A MODIFICAR EL FORMATO DE FECHA PARA EL TIMESTAMP(se puede excentar si el origen ya viene en ese foemato y solo se usa $s)
    values = (row['cliente_id'],row['nombre'],row['apellido'],row['email'],row['fecha_registro'])
    cursor.execute(sql,values)


conn.commit()
cursor.close()
conn.close()
print("base de datos crm cerrada exitosamente")

