from rapidfuzz import fuzz
import mysql.connector as mysql
import pandas as pd
import csv
import openpyxl
import os
import json
import keyboard
import getpass
import datetime
import bcrypt
import re



def inicio_programa():
    """
    Inicio del programa: decide si continuar, hacer login o salir
    """
    while True:
        accion = input("Desea ejecutar el programa? (Enter = sí / 'lb' = login db/ 'lf' = login file / 'h'=historial /'no' = salir): ").strip().lower()
        if accion == "":
            print("✅ Continuando con el programa...")
            return "continuar", None
        
        elif accion == "lb":
            bol,pesos,columnas,nuevo,user,password=alt_a_pressed()
            if bol == True:
                guardar_historial(columnas,pesos,nuevo,user,password)
            return "continuar", None
        
        elif accion == "lf":
            bol,pesos,columnas,nuevo,user,password=modificar_config_pesos() 
            if bol == True:
                guardar_historial(columnas,pesos,nuevo,user,password)
            return "continuar", None
        
        elif accion == "h":
            bol,df = consultar_historial()
            if bol==True:
                df_filtrado = historial(df)
            return "continuar", None
            
        
        elif accion == "no":
            print("👋 Saliendo del programa...")
            return "salir", None
        else:
            print("Opción no válida. Intenta de nuevo.")

#funcion credenciales archivo
def cargar_usuarios():
    with open("usuarios.json", "r") as f:
        return json.load(f)
    
def login_autorizado():
    usuarios = cargar_usuarios()
    intentos = 0
    max_intentos = 3

    while intentos < max_intentos:
        username = input("Usuario: ").strip()
        password = getpass.getpass("Contraseña: ").strip()

        if username in usuarios and bcrypt.checkpw(password.encode(), usuarios[username].encode()):
            print(f"✅ Acceso concedido a {username}")
            return username,password
        else:
            intentos += 1
            print(f"❌ Acceso denegado (Intento {intentos}/{max_intentos})")

    print("⚠️ Has superado el número máximo de intentos.")
    return None,None

def pedir_peso(campo):
        while True:
            try:
                valor = int(input(f"Nuevo peso para {campo} (1-99): "))
                if valor <= 0:
                    print("El valor debe ser mayor que 0.")
                elif valor > 99:
                    print("El valor no puede ser mayor a 99.")
                else:
                    return valor
            except ValueError:
                print("Los valores deben ser números enteros.")



def modificar_config_pesos( archivo="config_pesos.json"):
    user,password = login_autorizado()
    if not user:
        return False,None,None,None,None,None
    # Cargar archivo actual
    try:
                use,columnas=latest()
                columnas = ["first_name", "last_name", "email"]


                if use is True:
                    conn = mysql.connect(
                        host="localhost",
                        user="root",
                        password="1234",
                        database="user"
                    )
                    cur = conn.cursor(dictionary=True)
                    cur.callproc("sp_get_latest_weights")
                    
                    for result in cur.stored_results():
                        row = result.fetchone() 
                        if row:
                            pesos = {col: row[col] for col in columnas}  # 👈 solo valores como en el archivo


                elif use is False:
                    with open(archivo, "r") as f:
                        config = json.load(f)
                    pesos = {col: config[col] for col in columnas if col in config}

                first_name = pedir_peso("first_name")
                last_name = pedir_peso("last_name")
                email = pedir_peso("email")
    except ValueError:
                print("Los valores deben ser números enteros.")
                return "salir", None
    except (FileNotFoundError, json.JSONDecodeError):
                return "salir", None

    new_weights = {"first_name": first_name, "last_name": last_name, "email": email}
    fn,ln,em=impact(new_weights)
    first_name=fn
    last_name=ln
    email=em

    nuevos_pesos = {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "modified_by": "usuario_actual",  # puedes pedir usuario también
                "fecha_modificacion": datetime.datetime.now().isoformat(),
                "activo": 1
            }

    config = {
        "first_name": nuevos_pesos.get("first_name", 2),
        "last_name": nuevos_pesos.get("last_name", 3),
        "email": nuevos_pesos.get("email", 5),
        "modified_by": user,
        "fecha_modificacion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "activo": 1
    }
 

    # Sobrescribir archivo
    with open(archivo, "w") as f:
        json.dump(config, f, indent=4)

    print(f"✅ Archivo {archivo} actualizado por {user}")
    nuevo={"first_name":first_name,
           "last_name":last_name,
           "email":email}
    return True,pesos,columnas,nuevo,user,password






def guardar_historial(columnas, pesos_anteriores, pesos_nuevos, usu,passW):
    conn = mysql.connect(
            host="localhost",
            database="user",
            user="root",
            password=passW
        )
    cursor = conn.cursor()

    fecha_hora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for col in columnas:
        cursor.execute(
            """
            INSERT INTO historial_pesos (columna, peso_anterior, peso_nuevo, fecha_hora)
            VALUES (%s, %s, %s, %s)
            """,
            (col, pesos_anteriores[col], pesos_nuevos[col], fecha_hora,)
        )
    conn.commit()

    df = pd.DataFrame({
        "columna": columnas,
        "peso_anterior": [pesos_anteriores[col] for col in columnas],
        "peso_nuevo": [pesos_nuevos[col] for col in columnas],
        "fecha_hora": [fecha_hora] * len(columnas)
    })


    cursor.close()
    conn.close()
    print("Historial actualizado en la base de datos.")
    print(df)



def consultar_historial():
    username,passw = login_user()
    if username:
        print(f"✅ Logged in as {username}")
        try:
            conn = mysql.connect(
                host="localhost",
                user="root",
                password=passw,
                database="user"
            )
            df = pd.read_sql("SELECT * FROM historial_pesos", conn)
            conn.close()
            return True,df
        except mysql.connector.Error as e:
            print("Error en la base de datos:", e)
            return False,pd.DataFrame()
    else:
        print("Access denied or exit requested.")
        return False



def historial(df):
    
    if df.empty:
        print("No hay datos para mostrar.")
        return df

    col_map = {
        1: "id",
        2: "columna",
        3: "peso_anterior",
        4: "peso_nuevo",
        5: "fecha_hora"
    }

    print("Columnas disponibles:")
    for k, v in col_map.items():
        print(f"{k}: {v}")

    # --- SELECCIÓN DE COLUMNAS ---
    while True:
        col_input = input("Ingrese números de columna a mostrar separados por comas (Enter=todo): ").strip()
        if not col_input:
            cols_to_show = list(col_map.values())
            break

        try:
            cols_to_show = []
            for n in col_input.split(","):
                n = n.strip()
                if not n.isdigit() or int(n) not in col_map:
                    raise ValueError(f"❌ Columna {n} no existe.")
                cols_to_show.append(col_map[int(n)])
            break
        except Exception as e:
            print(e)
            print("⚠️ Intenta de nuevo con columnas válidas.")

    df = df[cols_to_show]

    # --- FILTRO DE FILAS ---
    while True:
        filtro_input = input("Ingrese filtros en formato (nº de columna) = valor,  separados por comas para mas filtros (Enter=sin filtro): ").strip()
        if not filtro_input:
            break

        try:
            
            valido = True
            for f in filtro_input.split(","):
                f = f.strip()
                match = re.match(r"(\d+)(=|>|<|~)(.+)", f)
                if not match:
                    print(f"⚠️ Filtro inválido: {f}. Use formato: num_columna=valor | num_columna>valor | num_columna<valor | num_columna~texto")
                    valido = False
                    break

                num, op, val = match.groups()
                num = int(num)
                if num not in col_map:
                    print(f"⚠️ Columna {num} no existe.")
                    valido = False
                    break

            if valido:
                for f in filtro_input.split(","):
                    num, op, val = re.match(r"(\d+)(=|>|<|~)(.+)", f).groups()
                    num = int(num)
                    col = col_map[num]
                    if op == "=":
                        df = df[df[col].astype(str) == val.strip()]
                    elif op == ">":
                        df = df[pd.to_numeric(df[col], errors="coerce") > float(val)]
                    elif op == "<":
                        df = df[pd.to_numeric(df[col], errors="coerce") < float(val)]
                    elif op == "~":
                        df = df[df[col].astype(str).str.contains(val.strip(), case=False, na=False)]
                break
        except Exception as e:
            print(e)
            print("⚠️ Intenta de nuevo con filtros válidos.")

    # --- ORDEN ---
    while True:
        orden = input("Ingrese números de columna para ordenar separados por comas (Enter=sin orden), agregar '- (numero de columna)' para descendente: ").strip()
        if not orden:
            break

        try:
            sort_cols = []
            ascending_list = []
            for col_num in orden.split(","):
                col_num = col_num.strip()
                if not col_num:
                    continue
                desc = False
                if col_num.startswith("-"):
                    desc = True
                    col_num = col_num[1:].strip()
                if not col_num.isdigit() or int(col_num) not in col_map:
                    raise ValueError(f"❌ Columna {col_num} no existe para ordenar.")
                sort_cols.append(col_map[int(col_num)])
                ascending_list.append(not desc)
            df = df.sort_values(by=sort_cols, ascending=ascending_list)
            break
        except Exception as e:
            print(e)
            print("⚠️ Intenta de nuevo con orden válido.")

    print("\nHistorial filtrado/ordenado:")
    print(df)
    return df




def impact(new_weights=None):
    df_sample = pd.DataFrame([
        {'first_name': 'Evelyn', 'last_name': 'fryda', 'email': 'user8@example.com'},
        {"first_name": "Robert", "last_name": "Davis", "email": "user2@example.com"},
        {"first_name": "Maryna", "last_name": "Wells", "email": "user3@example.com"},
        {"first_name": "Brenda", "last_name": "Meyer", "email": "user5@example.com"},
        {"first_name": "Yvonne", "last_name": "Jesen", "email": "user6@example.com"}
        ])
    
    df_sample2 = pd.DataFrame([
        {'first_name': 'Evelyn', 'last_name': 'fryda', 'email': 'user8@example.com'},
        {"first_name": "Roberto", "last_name": "Daviz", "email": "user2@exampli.com"}, 
        {"first_name": "Matina", "last_name": "Wils", "email": "user3@exonple.com"}, 
        {"first_name": "Brinte", "last_name": "naier", "email": "user5@examion.com"}, 
        {"first_name": "Yviclo", "last_name": "yazin", "email": "vist6@example.com"} 
    ])

    if new_weights is None:
        new_weights = {"first_name": 1, "last_name": 1, "email": 1}

    results = []
    for idx, row in df_sample.iterrows():
        best_score = 0
        best_match = None
        for _, dest_row in df_sample2.iterrows():
            total_score = 0
            total_weight = 0
            for col in ["first_name", "last_name", "email"]:
                score = fuzz.ratio(str(row[col]), str(dest_row[col]))
                weight = new_weights.get(col, 1)
                total_score += score * weight
                total_weight += weight
            weighted_score = total_score / total_weight if total_weight else 0
            if weighted_score > best_score:
                best_score = weighted_score
                best_match = dest_row
        results.append({
            "query": row.to_dict(),
            "match": best_match.to_dict(),
            "weighted_score": round(best_score, 2)
        })


    df_results = pd.DataFrame(results)
    print("\nSimulación de impacto con nuevos pesos:")
    print(df_results[["query", "match", "weighted_score"]])

    while True:
        choice = input("¿Desea cambiar los pesos? (y/n): ").lower()
        if choice == "y":
            first_name = pedir_peso("first_name")
            last_name = pedir_peso("last_name")
            email = pedir_peso("email")
            
            new_weights = {
                "first_name": first_name,
                "last_name": last_name,
                "email": email
            }
            
            # recalculamos con los nuevos pesos
            # aquí sí es recursión, pero controlada
            return  impact(new_weights) # salimos para no seguir duplicando llamadas
        elif choice == "n":
            fn = new_weights["first_name"]
            ln = new_weights["last_name"]
            em = new_weights["email"]
            break
        else:
            print("Opción no válida, ingrese y o n.")
    return fn,ln,em



















#funcion para las credeciales
def alt_a_pressed():
    username,passw = login_user()
    pesos = {} 
    if username:
        print(f"✅ Logged in as {username}")
        
        try:
            use,columnas=latest()
            columnas = ["first_name", "last_name", "email"]
            


            if use is True:
                conn = mysql.connect(
                    host="localhost",
                    user="root",
                    password="1234",
                    database="user"
                )
                cur = conn.cursor(dictionary=True)
                cur.callproc("sp_get_latest_weights")
                
                for result in cur.stored_results():
                        row = result.fetchone() 
                        if row:
                            pesos = {col: row[col] for col in columnas}  # 👈 solo valores como en el archivo


            elif use is False:
                with open("config_pesos.json", "r") as f:
                    config = json.load(f)
                pesos = {col: config[col] for col in columnas if col in config}



            first_name = pedir_peso("first_name")
            last_name = pedir_peso("last_name")
            email = pedir_peso("email")
        except ValueError:
            print("Invalid input. Must be numbers between 0-99.")
            return
        if not all(0 <= val <= 99 for val in [first_name, last_name, email]):
            print("Values must be between 0 and 99.")
            return
        
        new_weights = {"first_name": first_name, "last_name": last_name, "email": email}
        fn,ln,em=impact(new_weights)
        first_name=fn
        last_name=ln
        email=em

        insert_weigth(first_name, last_name, email, username)

        nuevo={"first_name":first_name,
           "last_name":last_name,
           "email":email}
        return True,pesos,columnas,nuevo,username,passw
    else:
        print("Access denied or exit requested.")
        return False,None,None,None,None,None



def login_user():
    attempts = 0
    while attempts < 3:
        username = input("Username: ")
        if username.lower() == "exit":
            print("Exiting...")
            return None,None
        password = getpass.getpass("Password: ")

        try:
            conn = mysql.connect(
                host="localhost",
                user="root",
                password="1234",
                database="user"
            )
            cursor = conn.cursor(dictionary=True)
            cursor.callproc("sp_login_user", [username, password])
            result = None
            for r in cursor.stored_results():
                result = r.fetchone()
            cursor.close()
            conn.close()

            if result:
                print(f"✅ Welcome {result['username']} (role: {result['rol']})")
                return result['username'],password
            else:
                attempts += 1
                print("❌ Incorrect username or password. Try again.")

        except mysql.connector.Error as e:
            print("Database error:", e)
            return None,None

    print("❌ Too many failed attempts.")
    return None,None


def insert_weigth(first_name, last_name, email, username):
    try:
        conn = mysql.connect(
            host="localhost",
            user="root",
            password="1234",
            database="user"
        )
        cursor = conn.cursor()
        cursor.callproc("sp_insert_weight", [first_name, last_name, email, username])
        conn.commit()
        print(f"✅ Insert successful by {username}")
        cursor.close()
        conn.close()
    except mysql.connector.Error as e:
        print("Database error:", e)

# Pesos para cada columna
def columnWeights():
    conn = mysql.connect(
        host="localhost",
        user="root",
        password="1234",
        database="user"
    )
    cursor = conn.cursor(dictionary=True)
    cursor.callproc("sp_get_column_weights")
    weights = {}
    for result in cursor.stored_results():
        row = result.fetchone()
        if row:
            # Tomamos solo las columnas que nos interesan
            weights["first_name"] = row["first_name"]
            weights["last_name"] = row["last_name"]
            weights["email"] = row["email"]
            weights["fecha_modificacion"] = row["fecha_modificacion"]
    cursor.close()
    conn.close()
    return weights



def cargar_config_pesos(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"❌ Archivo '{file_path}' no encontrado.")
        return {}
    except json.JSONDecodeError:
        print(f"❌ Error leyendo '{file_path}', formato JSON inválido.")
        return {}
    

def latest():
    use=False
    db_weights = columnWeights()
    file_weights = cargar_config_pesos("config_pesos.json")  # {"first_name": 12, "last_name": 22, "email": 28, "fecha_actualizacion": "2025-09-18 12:30:00"}

    db_date = db_weights.get("fecha_modificacion")
    file_date = file_weights.get("fecha_modificacion")

    if db_date and isinstance(db_date, str):
        db_date = datetime.datetime.fromisoformat(db_date)

    if file_date and isinstance(file_date, str):
        file_date = datetime.datetime.fromisoformat(file_date)

    if db_date > file_date:
        COLUMN_WEIGHTS = db_weights
        print("Se usan los pesos de la base de datos (más recientes).")
        use=True
    else:
        COLUMN_WEIGHTS = file_weights
        print("Se usan los pesos del archivo de configuración (más recientes).")
        use=False
        
    print(COLUMN_WEIGHTS) 
    return use,COLUMN_WEIGHTS

use,COLUMN_WEIGHTS=latest()



def data(params_dict):
    conn = mysql.connect(
        host=params_dict.get("server", ""),
        database=params_dict.get("database", ""),
        user=params_dict.get("username", ""),
        password=params_dict.get("password", "")
    )
    cursor = conn.cursor(dictionary=True)
    cursor.callproc("sp_get_match_record_ori_dest_006")
    rows = []
    for result in cursor.stored_results():
        rows.extend(result.fetchall())
    cursor.close()
    conn.close()
    df = pd.DataFrame(rows)
    while True:
        try:
            num = int(input("¿you want the result in DataFrame (0) or as a Dictionary (1)? "))
        except ValueError:
            print("Option incorrect, chose 0 or 1.")
            continue
        if num == 0:
            return df
        elif num == 1:
            return df.to_dict(orient="records")
        else:
            print("Option incorrect, chose 0 or 1.")

def dfDict(df, name):
    while True:
        num = int(input(f"¿you want the result of {name} in DataFrame (0) or as a Dictionary (1)?"))
        if num == 0:
            return print(df)
        elif num == 1:
            dic = df.to_dict(orient="records")
            return print(dic)
        else:
            print("Option incorrect, chose 0 or 1.")

def csv_files(answer):
    x = True
    while x:
        i = input("¿Desea crear archivo csv? (y/n)")
        if "y" == i.lower():
            df = pd.DataFrame(answer)
            df = changeFormat(df)
            name = input("Enter the file name: ")
            colum = columns(df)
            while True:
                newCol = input(f"you want rename the columns?(Y/N)")
                if "y" == newCol.lower():
                    df, colum = renameCol(df, colum)
                    break
                elif "n" == newCol.lower():
                    break
                else:
                    print("Error, please select one of the two options (y/n).")
            while True:
                num = input("How many rows do you want to export? (Enter = all): ")
                if num == "0":
                    print("you cannot create empty file")
                    continue
                if num.strip() == "":
                    exportDf = df[colum]
                    break
                try:
                    num_int = int(num)
                    if num_int <= 0:
                        continue
                    exportDf = df[colum].head(int(num))
                    break
                except ValueError:
                    print("Please enter a valid number.")
            folder = "file_csv"
            if not os.path.exists(folder):
                os.makedirs(folder)
            file = os.path.join(folder, f"{name}.csv")
            keys = exportDf.columns
            with open(file, "w", newline="", encoding="utf-8") as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(exportDf.to_dict(orient="records"))
                print(f"file '{name}' has been exported successfully!")
                x = False
                return exportDf, name
        elif "n" == i.lower():
            return None, None
        else:
            print("Error, please select one of the two options (y/n).")

def excel(answer):
    x = True
    while x:
        i = input("Do you want to export to Excel? (y/n): ")
        if "y" == i.lower():
            df = pd.DataFrame(answer)
            df = changeFormat(df)
            name = input("Enter the file name: ")
            colum = columns(df)
            while True:
                newCol = input(f"you want rename the columns?(Y/N)")
                if "y" == newCol.lower():
                    df, colum = renameCol(df, colum)
                    break
                elif "n" == newCol.lower():
                    break
                else:
                    print("Error, please select one of the two options (y/n).")
            while True:
                num = input("How many rows do you want to export? (Enter = all): ")
                if num == "0":
                    print("you cannot create empty file")
                    continue
                if num.strip() == "":
                    exportDf = df[colum]
                    break
                try:
                    num_int = int(num)
                    if num_int <= 0:
                        continue
                    exportDf = df[colum].head(int(num))
                    break
                except ValueError:
                    print("Please enter a valid number.")
            folder = "file_excel"
            if not os.path.exists(folder):
                os.makedirs(folder)
            file = os.path.join(folder, f"{name}.xlsx")
            exportDf.to_excel(f"{file}", index=False)
            print(f"file '{name}' has been exported successfully!")
            return exportDf, name
            x = False
        elif "n" == i.lower():
            return None, None
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
        2: "email",  # <-- agregamos email aquí
        3: "match_query",
        4: "match_result",
        5: "match_result_values",
        6: "destTable",
        7: "sourceTable",
    }
    i = True
    while i:
        Columns = input(
            "select the columns you want to print, separate with commas: "
            "name_complet(1), email(2), match_query(3), match_result(4), "
            "match_result_values(5), destTable(6), sourceTable(7) (ENTER=all)"
        )
        if Columns.strip() == "":
            selected_columns = list(df.columns)
            i = False
        else:
            selected_columns = []
            for num in Columns.split(","):
                num = num.strip()
                if num.isdigit() and int(num) in col_map:
                    selected_columns.append(col_map[int(num)])
                    i = False
        if not selected_columns:
            print("No valid columns selected, exporting all by default.")
    if "score" not in selected_columns:
        selected_columns.append("score")
    return selected_columns

def renameCol(df, selectedColumns):
    rename = {}
    for col in selectedColumns:
        newName = input(f"Enter new name for '{col}' (Enter = keep current name): ")
        if newName.strip() != "":
            rename[col] = newName
    if rename:
        df = df.rename(columns=rename)
        selectedColumns = [rename.get(col, col) for col in selectedColumns]
    return df, selectedColumns

def connectMysql(server, database, username, password):
    return mysql.connect(
        host=server,
        database=database,
        user=username,
        password=password
    )

def weighted_fuzzy_match(query_record, choices, src_dest_mappings, score_cutoff=0):
    processor = lambda x: str(x).lower() if x is not None else ""
    best_match = None
    best_score = score_cutoff
    for choice in choices:
        total_score = 0
        total_weight = 0
        match_result_values = {}
        for src_col, dest_col in src_dest_mappings.items():
            q_val = processor(query_record.get(src_col))
            c_val = processor(choice.get(dest_col))
            weight = COLUMN_WEIGHTS.get(src_col, 1)
            score = fuzz.ratio(q_val, c_val)
            total_score += score * weight
            total_weight += weight
            match_result_values[dest_col] = choice.get(dest_col)
        weighted_score = total_score / total_weight if total_weight else 0
        if weighted_score > best_score:
            best_score = weighted_score
            best_match = {
                'match_query': {k: query_record.get(k) for k in src_dest_mappings.keys()},
                'match_result': {k: choice.get(v) for k, v in src_dest_mappings.items()},
                'score': weighted_score,
                'match_result_values': match_result_values
            }
    if not best_match:
        best_match = {
            'match_query': {k: query_record.get(k) for k in src_dest_mappings.keys()},
            'match_result': None,
            'score': 0,
            'match_result_values': {}
        }
    return best_match

def recordHigh(fm, dict_query_records, matching_records, params_dict):
    if fm['score'] >= 70:
        dict_query_records.update(fm)
        dict_query_records.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        matching_records.append(dict_query_records)

def recordHigh97(fm, dict_query_records, params_dict, conn, table_name, lista_filtrada):
    if not lista_filtrada:
        return
    newControl = assign_control(params_dict, table_name)
    cursor = conn.cursor()
    for fm, dict_query_records in lista_filtrada:
        cursor.callproc(
            "sp_insert_scored_006",
            [
                dict_query_records.get("first_name"),
                dict_query_records.get("last_name"),
                dict_query_records.get("email"), 
                json.dumps(fm.get("match_query")),    # convertir a string
                json.dumps(fm.get("match_result")),   
                fm.get("score"),
                params_dict["destTable"],
                params_dict["sourceTable"]
            ]
        )
    cursor.callproc("sp_update_last_scored_control", [newControl])
    print(f"has been successfully inserted into the table {table_name} of dbo database, control number:{newControl} ")
    conn.commit()
    cursor.close()

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
    cursor.callproc("sp_getTable_mysql_data_001", [params_dict['sourceSchema'], params_dict['sourceTable'], src_cols])
    for result in cursor.stored_results():
        src_rows = result.fetchall()
        src_columns = [col[0] for col in result.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]
    cursor2.callproc("sp_getTable_mysql_data_001", [params_dict['destSchema'], params_dict['destTable'], dest_cols])
    for result in cursor2.stored_results():
        dest_rows = result.fetchall()
        dest_columns = [col[0] for col in result.description]

    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]
    matching_records = []
    lista_filtrada_97 = []
    
    for record in source_data:
        dict_query_records = {src_col: record.get(src_col) for src_col in params_dict['src_dest_mappings'].keys()}
        fm = weighted_fuzzy_match(record, dest_data, params_dict['src_dest_mappings'], score_cutoff)
        recordHigh(fm, dict_query_records, matching_records, params_dict)
        if fm['score'] >= 97:
            lista_filtrada_97.append((fm, dict_query_records))
    table_name97 = "scored"
    recordHigh97(fm, dict_query_records, params_dict, conn, table_name97, lista_filtrada_97)

    newControl = assign_control(params_dict, table_name97) 

    insert_cursor = conn.cursor()
    for match in matching_records:
        insert_cursor.callproc(
            "sp_insert_matching_result_ori_dest_005fen",
            [
                match.get('first_name'),
                match.get('last_name'),
                dict_query_records.get("email"), 
                json.dumps(fm.get("match_query")),    # convertir a string
                json.dumps(fm.get("match_result")),   
                match.get('score'),
                json.dumps(match.get('match_result_values')),
                params_dict['destTable'],
                params_dict['sourceTable'],
                newControl
            ]
        )
    table_name = "match_record"
    #control = assign_control(params_dict, table_name)
    print(f"has been successfully inserted into table {table_name} of dbo database, control number:{newControl}")
    conn.commit()
    insert_cursor.close()
    cursor.close()
    conn.close()
    cursor2.close()
    conn2.close()
    return matching_records

def upload(params_dict, df):
    res = 1
    df_result = df
    while True:
        x = input("you want upload file? (Y/N)")
        if "y" == x.lower():
            y = input("xlsx(1) or  csv(2)")


            if "1" == y:
                while True:
                    data = input("Insert the full path of the file: ")
                    if not os.path.exists(data):
                        print(f"File '{data}' does not exist. Please check the path.")
                        continue
                    else:
                        break
                df = pd.read_excel(data)
                break
            elif "2" == y:
                while True:
                    data = input("Insert the full path of the file: ")
                    if not os.path.exists(data):
                        print(f"File '{data}' does not exist. Please check the path.")
                        continue
                    else:
                        break
                df = pd.read_csv(data)
                break
            else:
                print("Error, please select one of the two options (1 or 2).")
                return res, df
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
            return res, df
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
        col_defs = ", ".join([f"`{col}` TEXT" for col in df.columns])
        cursor.callproc("sp_importTable_file_mysql_004", [col_defs])
        conn.commit()

        table_cols = get_table_columns(conn, "Import")  # función que obtiene columnas de la tabla
        df = df[[c for c in df.columns if c in table_cols]]  # solo columnas válidas

        table_name = "Import"
        control = assign_control(params_dict, table_name)
        df["numeroControl"] = control

        sp_BulkInsertImport_file_mysql_27177(df, conn)
        
        print(f"has been successfully inserted into the table {table_name} of dbo database, control number:{control}")
        
        return res, df
    except Exception as e:
        print(e)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
        return res, df_result
    
def get_table_columns(conn, table_name):
    cursor = conn.cursor()
    cursor.callproc("sp_get_table_columns", [table_name])
    cols = []
    for result in cursor.stored_results():
        cols.extend([row[0] for row in result.fetchall()])
    cursor.close()
    return cols



def sp_BulkInsertImport_file_mysql_27177(df, conn):
    cursor = conn.cursor()

    # Columnas separadas por comas
    col_names = ",".join(df.columns)

    # Valores, convirtiendo NaN a NULL
    values_list = []
    for row in df.values:
        row_vals = []
        for val in row:
            if pd.isna(val):
                row_vals.append("NULL")
            else:
                safe_val = str(val).replace("'", "''")
                row_vals.append(f"'{safe_val}'")
        values_list.append(f"({','.join(row_vals)})")
    col_values = ",".join(values_list)

    # Llamar al SP
    cursor.callproc("sp_BulkInsertImport_file_mysql_27177", [col_names, col_values])
    conn.commit()
    cursor.close()



def assign_control(params_dict, table_name):
    conn = None
    cursor = None
    try:
        conn = mysql.connect(
            host=params_dict.get("server", ""),
            database=params_dict.get("database", ""),
            user=params_dict.get("username", ""),
            password=params_dict.get("password", "")
        )
        cursor = conn.cursor(dictionary=True)
        cursor.callproc("sp_get_last_id", [table_name])
        record_id = None
        for result in cursor.stored_results():
            row = result.fetchone()
            if row:
                record_id = row["id"]
                
        if not row:
            print("No hay registros en la tabla.")
            return None
        
        cursor.callproc("sp_assign_controlNum", [table_name, record_id])
        conn.commit()
        for result in cursor.stored_results():
            row = result.fetchone()
            new_control = row["numeroControl"]
        return new_control
    except Exception as e:
        print("Error:", e)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()