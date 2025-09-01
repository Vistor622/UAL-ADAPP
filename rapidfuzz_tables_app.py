import module_function as mf
import pandas as pd


params_dict = {
    "server": "localhost",
    "database": "dbo",
    "username": "root",
    "password": "1234",
    "server2": "localhost",
    "database2": "crm",
    "username2": "root",
    "password2": "1234",
    "sourceSchema": "dbo",
    "sourceTable": "Usuarios",
    "destSchema": "crm",
    "destTable": "Clientes",
    "src_dest_mappings": {
        "first_name": "nombre",
    }
}

resultados = mf.filter(params_dict, score_cutoff=70)

show = pd.DataFrame(resultados)

print(show)
