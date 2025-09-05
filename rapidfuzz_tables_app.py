import module_function as mf

params_dict = {
    "server": "localhost",
    "database": "dbo",
    "username": "root",
    "password": "",
    "server2": "localhost",
    "database2": "crm",
    "username2": "root",
    "password2": "",
    "sourceSchema": "dbo",
    "sourceTable": "Usuarios",
    "destSchema": "crm",
    "destTable": "Clientes",
    "src_dest_mappings": {
        "first_name": "nombre",
        "last_name":"apellido"
    }
} 
res=1

resultados = mf.filter(params_dict, score_cutoff=70)
show=mf.dfDict(resultados)#diccionario o dataframe0
mf.csv_files(resultados)#cvs
mf.excel(resultados)#excel
while res==1:
    res=mf.upload(params_dict)


print(show)
