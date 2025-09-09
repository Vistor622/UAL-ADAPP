import module_function as mf

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
        "last_name":"apellido"
    }
} 
res=1

resultados = mf.filter(params_dict, score_cutoff=70)
datas=mf.data(params_dict)
print(datas)
#show=mf.dfDict(resultados)#diccionario o dataframe0
#print(resultados)
dfC,nameC=mf.csv_files(resultados)#cvs
dfE,nameE=mf.excel(resultados)#excel
df=None

while res==1:
    res,df=mf.upload(params_dict,df)

#if dfC is not None:
#    name=nameC+".csv"
#    mf.dfDict(dfC,name)

#if dfE is not None:
#    name=nameE+".xslx"
#    mf.dfDict(dfE,name)

#if df is not None:
#    name="Last Import"
#    mf.dfDict(df,name)
