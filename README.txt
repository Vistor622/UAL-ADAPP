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

//Actualizacion\\

linea 115
se realizaron los cambios solicitados para que la busqueda o mejor dicho los resultados sena solo los de 
score sean mayor a 70 los cuales se agregaran a la lista final para filtrar los matches poco relevantes

linea 69
El uso de with garantiza que las conexiones a la base de datos se cierren automáticamente 
al finalizar el bloque, mejorando la gestión de recursos y evitando fugas de conexión.

linea 143
Se pasa el valor 70 como parámetro para que el filtro de score se aplique desde el inicio, 
haciendo el código más flexible y fácil de ajustar.

resumen de las modificaciones
Las modificaciones optimizan el manejo de conexiones y filtran los resultados para mostrar 
solo los matches relevantes (score > 70). Esto mejora la eficiencia y la calidad de los resultados.

Resumen de las actualizaciones 05/09/2025
se implemento un stored procedure para poder tener una mejor optimizacion y que el codigo funcione de 
manera mas rapida.
se implemento el stored procedure a todas las funciones que insertaban datos para que el codigo funcionara 
mejor y pudiera estar mejor organizado.



sp_get_match_record_ori_dest_006
→ Usado en la función data() para obtener registros de coincidencia.

sp_insert_scored_006
→ Usado en la función recordHigh97() para insertar registros con puntaje (scored).

sp_getTable_mysql_data_001
→ Usado en la función filter() dos veces:

Para traer datos de la tabla source.

Para traer datos de la tabla dest.

sp_insert_matching_result_ori_dest_005fen
→ Usado en la función filter() para insertar coincidencias encontradas.

sp_importTable_file_mysql_004
→ Usado en la función upload() para crear la tabla de importación.

sp_BulkInsertImport_file_mysql_27177
→ Usado en la función sp_BulkInsertImport_file_mysql_27177() para insertar masivamente los datos del archivo importado.

sp_assign_controlNum
→ Usado en la función assign_control() para asignar número de control a registros.

























