from rapidfuzz import process, fuzz
import mysql.connector as mysql
import pandas as pd
import csv
import openpyxl
import os
import json

# Pesos para cada columna
COLUMN_WEIGHTS = {
    "first_name": 2,
    "last_name": 3,
    "email": 5
}

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
        2: "match_query",
        3: "match_result",
        4: "match_result_values",
        5: "destTable",
        6: "sourceTable",
    }
    i = True
    while i:
        Columns = input("select the columns want your print, separet whit commas: name_complet(1),match_query(2),match_result(3),match_result_values(4),destTable(5),sourceTable(6),(ENTER=all)")
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
                fm.get("match_query"),
                fm.get("match_result"),
                fm.get("score"),
                params_dict["destTable"],
                params_dict["sourceTable"]
            ]
        )
        cursor.execute(f"UPDATE scored SET numeroControl = '{newControl}' WHERE id = LAST_INSERT_ID()")
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
    insert_cursor = conn.cursor()
    for match in matching_records:
        insert_cursor.callproc(
            "sp_insert_matching_result_ori_dest_005fen",
            [
                match.get('first_name'),
                match.get('last_name'),
                match.get('match_query'),
                match.get('match_result'),
                match.get('score'),
                json.dumps(match.get('match_result_values')),
                params_dict['destTable'],
                params_dict['sourceTable']
            ]
        )
    table_name = "match_record"
    control = assign_control(params_dict, table_name)
    print(f"has been successfully inserted into table {table_name} of dbo database, control number:{control}")
    conn.commit()
    insert_cursor.close()
    cursor.close()
    conn.close()
    cursor2.close()
    conn2.close()
    return matching_records

def upload(params_dict, df):
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
        sp_BulkInsertImport_file_mysql_27177(df, conn)
        table_name = "Import"
        control = assign_control(params_dict, table_name)
        print(f"has been successfully inserted into the table {table_name} of dbo database, control number:{control}")
        return res, df
    except Exception as e:
        print(e)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def sp_BulkInsertImport_file_mysql_27177(df, conn):
    col_names = ",".join([f"`{col}`" for col in df.columns])
    values = []
    for row in df.values:
        row_vals = []
        for val in row:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                row_vals.append("NULL")
            else:
                safe_val = str(val).replace("'", "''")
                row_vals.append(f"'{safe_val}'")
        values.append(f"({','.join(row_vals)})")
    col_values = ",".join(values)
    cursor = conn.cursor()
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
        cursor.execute(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("No hay registros en la tabla.")
            return None
        record_id = row["id"]
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