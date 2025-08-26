la primera funcion es connect_to_azure_sql la cual se encarga de conectar con una base de datos externa y utiliza
los siguientes parametros parametros para posteriormente iniciar la llamada a la base de datos;
server(el identificador del servidor), database(nombre de la base de datos), username(nombre del usuario), 
password(contraseña para acceder a la informacion de la base de datos) y devuelve todo el formato de conexion como objeto connection_string

segunda funcion es fuzzy_match la cual se encarga de comparar, con la ayuda libreria fuzz,2 una cadena de texto(queryRecord) con una lista(choises) y de acuerdo a su nivel de igualdad o semejanza devuelve
el mas parecido en choises, utiliza queryRecord(string), choices(es otra lista de diccionario), score_cutoff=0(valor de igualda o semejanza con formato numero) y devuelve 
el best_match que se encontro en las comparaciones

la tercera fución "execute_dynamic_matching" se encarga de llamar primero la primera funcion para conectar la base de datos y llama despues la segunda funcion para hacer un proceso  donde extrae informacion de la base de dato, 
las compara, limpia los datos mas rebundantes y al tener los mas similar los utiliza para hacer unas listas de diccionarios 
utiliza los parametros (params_dict(la cual es un diccionario donde inicializa cada variable de acuerdo al usuario en momento),
 score_cutoff=0(es el valor de semenjanza con formato numero)):
y devuelve el diccionario a travez de matching_records
