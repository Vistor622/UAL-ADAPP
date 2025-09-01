from rapidfuzz import process, fuzz
import mysql.connector as mysql

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
        if fm['score'] > 70:  # Solo agregar si el score es mayor a 70
            dict_query_records.update(fm)
            dict_query_records.update({
                'destTable': params_dict['destTable'],
                'sourceTable': params_dict['sourceTable']
            })
            matching_records.append(dict_query_records)

    # Cerrar conexiones
    cursor.close()
    conn.close()
    cursor2.close()
    conn2.close()

    return matching_records
