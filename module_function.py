from rapidfuzz import process, fuzz
import mysql.connector as mysql
import pandas as pd
import csv
import openpyxl
import os


def dfDict(matching_records):
        while True:
            num= int(input("¿you want the result in DataFrame (0) or as a Dictionary (1)?"))
            if num == 0:
                matchingDf= pd.DataFrame(matching_records)
                return matchingDf
            
            elif num == 1 :
                return matching_records
            else:
                print("Option incorrect, chose 0 or 1.")





def csv_files(answer):
    x=True
    while x==True:
        i=input("¿Desea crear archivo csv? (y/n)")

        if "y"==i.lower():
            df= pd.DataFrame(answer)
            df=changeFormat(df)
            name=input("Enter the file name: ")
            colum=columns(df)



            while True:
                    newCol=input(f"you want rename the columns?(Y/N)")
                    if "y"==newCol.lower():
                        df,colum=renameCol(df,colum)
                        break
                    elif "n"==newCol.lower():
                        break
                    else:
                        print("Error, please select one of the two options (y/n).")



            while True:
                num = input("How many rows do you want to export? (Enter = all): ")
                if num == "0":
                    print("you cannot create empty file")
                    continue

                if num.strip() == "":  # si presiona Enter
                    exportDf = df[colum]  # todas las filas
                    break
                try:
                    num_int = int(num)
                    if num_int <= 0:
                        continue
                    exportDf = df[colum].head(int(num)) 
                    break   
                            
                except ValueError:
                    print("Please enter a valid number.")


            folder="file_csv"
            if not os.path.exists(folder):
                os.makedirs(folder)

            file = os.path.join(folder,f"{name}.csv")


            keys = exportDf.columns  # Nombres de columnas
            with open(file, "w", newline="", encoding="utf-8") as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(exportDf.to_dict(orient="records"))
                print(f"file '{name}' has been exported successfully!")
                
                x=False
        elif "n" == i.lower():
            return
        else:    
            print("Error, please select one of the two options (y/n).")





def excel(answer):
    x=True
    while x==True:
        i=input("Do you want to export to Excel? (y/n): ")


        if "y"==i.lower():
            df= pd.DataFrame(answer)
            df=changeFormat(df)

            name=input("Enter the file name: ")
            colum=columns(df)

            while True:
                newCol=input(f"you want rename the columns?(Y/N)")
                if "y"==newCol.lower():
                    df,colum=renameCol(df,colum)
                    break
                elif "n"==newCol.lower():
                    break
                else:
                    print("Error, please select one of the two options (y/n).")



            while True:
                num = input("How many rows do you want to export? (Enter = all): ")
                if num == "0":
                    print("you cannot create empty file")
                    continue
            
                if num.strip() == "":  # si presiona Enter
                    exportDf = df[colum] # todas las filas
                    break
                try:
                    num_int = int(num)
                    if num_int <= 0:
                        continue
                    exportDf = df[colum].head(int(num)) 
                    break  

                except ValueError:
                    print("Please enter a valid number.")

            #crea carpeta si no existe
            folder = "file_excel"
            if not os.path.exists(folder):
                os.makedirs(folder)

            file = os.path.join(folder,f"{name}.xlsx",) #se ubica la ruta donde se guardara
            exportDf.to_excel(f"{file}",index=False)    #se crea la exportacion de excel (aqui entra openpyxl)
            print(f"file '{name}' has been exported successfully!")
            x=False
        elif "n" == i.lower():
            return
        else:
            print("Error, please select one of the two options (y/n).")

def changeFormat(df):
    df["name_complet"] = df["first_name"] + " " + df["last_name"]
    df = df.drop(columns=["first_name", "last_name"])

    cols = ["name_complet"] + [c for c in df.columns if c != "name_complet"]
    df = df[cols]


    df["score"] = df["score"].astype(str) + "%"
    return df



def columns(df):
    col_map = {
            1: "name_complet",
            2: "match_query",
            3: "match_result",
            4: "match_result_values",
            5: "destTable",
            6: "sourceTable",
           # 7: "score"
        }
    i=True
    while i==True:
        Columns=input("select the columns want your print, separet whit commas: name_complet(1),match_query(2),match_result(3),match_result_values(4),destTable(5),sourceTable(6),(ENTER=all)")
        if Columns.strip() == "":  
                selected_columns = list(df.columns) #el punto columns es propiedad de pandas, regresa nombre de columnas
                i=False
        else:
            selected_columns = []
            for num in Columns.split(","):
                num = num.strip()
                if num.isdigit() and int(num) in col_map:
                    selected_columns.append(col_map[int(num)])
                    i=False
        if not selected_columns:
                print("No valid columns selected, exporting all by default.")
                

    #Columns=Columns+",7"
    if "score" not in selected_columns:
        selected_columns.append("score")
    return selected_columns



def renameCol(df, selectedColumns):
    rename = {}
    for col in selectedColumns:
        newName = input(f"Enter new name for '{col}' (Enter = keep current name): ")
        if newName.strip() != "":
            rename[col] = newName  # renombrar columna
    if rename:
        df = df.rename(columns=rename)
        selectedColumns = [rename.get(col,col) for col in selectedColumns]
    return df,selectedColumns





def connectMysql(server, database, username, password):
    return mysql.connect(
        host=server,
        database=database,
        user=username,
        password=password
    )



def equality(queryRecord, choices, score_cutoff=0):
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    best_match = None
    best_score = score_cutoff

    for scorer in scorers:
        result = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if result:
            match_value, score, index = result
            if score > best_score:
                matched_item = choices_data[index]
                best_match = {
                    'match_query': queryRecord,
                    'match_result': match_value,
                    'score': score,
                    'match_result_values': matched_item['match_record_values']
                }
                best_score = score
    if not best_match:
        best_match = {
            'match_query': queryRecord,
            'match_result': None,
            'score': 0,
            'match_result_values': {}
        }
    return best_match



def recordHigh(fm,dict_query_records,matching_records,params_dict):
         if fm['score'] >= 97:  # Solo agregar si el score es mayor a 97
            dict_query_records.update(fm)
            dict_query_records.update({
                'destTable': params_dict['destTable'],
                'sourceTable': params_dict['sourceTable']
            })
            matching_records.append(dict_query_records)




def filter(params_dict, score_cutoff=0):
    conn = connectMysql(
        server=params_dict.get("server", ""),
        database=params_dict.get("database", ""),
        username=params_dict.get("username", ""),
        password=params_dict.get("password", "")
    ) 
    cursor = conn.cursor()

    conn2 = connectMysql(
        server=params_dict.get("server2", ""),
        database=params_dict.get("database2", ""),
        username=params_dict.get("username2", ""),
        password=params_dict.get("password2", "")
    ) 
    cursor2 = conn2.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceSchema']}.{params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destSchema']}.{params_dict['destTable']}"

    cursor.execute(sql_source)
    src_rows = cursor.fetchall()
    src_columns = [col[0] for col in cursor.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor2.execute(sql_dest)
    dest_rows = cursor2.fetchall()
    dest_columns = [col[0] for col in cursor2.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    matching_records = []

    for record in source_data:
        dict_query_records = {}
        query = ""

        for src_col in params_dict['src_dest_mappings'].keys():
            val = record.get(src_col)
            query += str(val) if val is not None else ""
            dict_query_records[src_col] = val

        fm = equality(query, dest_data, score_cutoff)
        recordHigh(fm,dict_query_records,matching_records,params_dict)#remplazo
    #    if fm['score'] = 70:  # Solo agregar si el score es mayor a 70
    #         dict_query_records.update(fm)
    #         dict_query_records.update({
    #             'destTable': params_dict['destTable'],
    #             'sourceTable': params_dict['sourceTable']
    #         })
    #         matching_records.append(dict_query_records)
    
    # Cerrar conexiones
    cursor.close()
    conn.close()
    cursor2.close()
    conn2.close()

  
    return matching_records


def upload(params_dict):
    df = None
    res = 1

    while True:
        x = input("you want upload file? (Y/N)")
        if "y" == x.lower():
            y = input("xlsx(1) or  csv(2)")
            while True:
                data = input("Insert the full path of the file: ")
                if not os.path.exists(data):
                    print(f"File '{data}' does not exist. Please check the path.")
                    continue
                else:
                    break
            if "1" == y:
                df = pd.read_excel(data)
                break
            elif "2" == y:
                df = pd.read_csv(data)
                break
            else:
                print("Error, please select one of the two options (1 or 2).")
            while len(df.columns) != len(set(df.columns)):
                print("Duplicate column names detected:")
                dup_cols = [col for col in df.columns if list(df.columns).count(col) > 1]
                print(dup_cols)
                print("Please provide new names for the duplicates.")
                for col in dup_cols:
                    new_name = input(f"Enter a new name for duplicate column '{col}': ")
                    if new_name.strip() != "":
                        occurrences = [i for i, c in enumerate(df.columns) if c == col]
                        for idx in occurrences[1:]:
                            df.columns.values[idx] = new_name
                print("Columns renamed, checking again...")
            break
        elif "n" == x.lower():
            res = 0
            break
        else:
            print("Error, please select one of the two options (y/n).")
            continue

    try:
        conn = connectMysql(
            server=params_dict.get("server", ""),
            database=params_dict.get("database", ""),
            username=params_dict.get("username", ""),
            password=params_dict.get("password", "")
        )
        cursor = conn.cursor()

        table_name = "Import"

        # Verificar si la tabla ya existe
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()

        if result:
            # Tabla existe → obtener columnas actuales
            cursor.execute(f"DESCRIBE {table_name}")
            existing_cols = [col[0] for col in cursor.fetchall() if col[0] != 'id']
            # Rellenar columnas faltantes
            for col in existing_cols:
                if col not in df.columns:
                    df[col] = None
            # Reordenar columnas según tabla
            df = df[[col for col in existing_cols if col in df.columns]]
        else:
            # Tabla no existe → crear con las columnas del primer archivo
            col_defs = ", ".join([f"`{col}` TEXT" for col in df.columns])
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {col_defs}
                )
            """ )

        # Insertar datos usando el procedimiento almacenado
        sp_BulkInsertImport_file_mysql_27177(df, conn)
        print("Datos insertados correctamente.")
        return res

    except Exception as e:
        print(e)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def sp_BulkInsertImport_file_mysql_27177(df, conn):
    col_names = ",".join([f"`{col}`" for col in df.columns])
    values = ",".join(
        [f"({','.join([repr(str(val)) for val in row])})" for row in df.values]
    )
    cursor = conn.cursor()
    cursor.callproc('sp_BulkInsertImport_file_mysql_27177', [col_names, values])
    conn.commit()
    cursor.close()


