import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import uuid
import bcrypt
from config import sql_server_url, postgres_url

engine_src = create_engine(sql_server_url)
engine_dest = create_engine(postgres_url)

def clean_text(text_val):
    if not text_val: return ""
    return " ".join(str(text_val).split())

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def sync_active_only():
    print("🚀 Sincronización Final: Solo Activos + Usuarios por DNI (Sin Correo)...")
    print(f"[DB] Conectando a: {postgres_url.split('@')[1]}")
    
    query = """
    SELECT 
        RTRIM(dcIdSGHH) as emp_ext_id,
        RTRIM(dcIdTrabajador) as person_ext_id,
        RTRIM(dcRucEmpresa) as company_ruc,
        dcDesCargo as position_raw,
        ddFechaIngreso as start_date,
        fnCodEstado as status
    FROM [dbo].[V_PERSONA_AGRUPAMIENTO_3]
    WHERE fnCodEstado = 1
    """
    df_active = pd.read_sql(query, engine_src)
    print(f"📥 Se extrajeron {len(df_active)} empleados activos de SQL Server")
    
    # LIMPIAR DATA ANTES DE USAR
    df_active['person_ext_id'] = df_active['person_ext_id'].astype(str).str.strip()
    df_active['company_ruc'] = df_active['company_ruc'].astype(str).str.strip()
    df_active['position_raw'] = df_active['position_raw'].astype(str).str.strip()
    df_active['position_clean'] = df_active['position_raw'].apply(clean_text)

    # CONTEO ANTES
    with engine_dest.connect() as temp_conn:
        usuarios_antes = temp_conn.execute(text("SELECT COUNT(*) FROM public.users")).scalar()
        print(f"📊 Usuarios ANTES: {usuarios_antes}")
    
    # TRANSACCIÓN PRINCIPAL (nueva conexión)
    with engine_dest.connect() as conn:
        with conn.begin():
            # 1. CARGOS - Bulk Insert
            unique_positions = df_active['position_clean'].dropna().unique()
            print(f"🏢 Sincronizando {len(unique_positions)} cargos...")
            pos_records = [{'n': p, 't': datetime.now()} for p in unique_positions]
            if pos_records:
                conn.execute(text("""
                    INSERT INTO public.positions (name, description, updated_at) 
                    VALUES (:n, :n, :t) 
                    ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
                """), pos_records)

            # 2. EMPLEADOS ACTIVOS - Pre-cargar referencias en memoria
            print("📋 Pre-cargando referencias de Personas, Empresas y Cargos...")
            
            # Obtener todas las personas relevantes
            person_ext_ids = df_active['person_ext_id'].unique().tolist()
            person_placeholders = ','.join([f':p{i}' for i in range(len(person_ext_ids))])
            person_params = {f'p{i}': pid for i, pid in enumerate(person_ext_ids)}
            person_ids = conn.execute(text(f"""
                SELECT external_id, id_person FROM public.persons 
                WHERE external_id IN ({person_placeholders})
            """), person_params).fetchall()
            person_map = {p[0]: p[1] for p in person_ids}
            
            # Obtener todas las empresas relevantes
            company_rucs = df_active['company_ruc'].unique().tolist()
            company_placeholders = ','.join([f':c{i}' for i in range(len(company_rucs))])
            company_params = {f'c{i}': ruc for i, ruc in enumerate(company_rucs)}
            company_ids = conn.execute(text(f"""
                SELECT ruc, id_company FROM public.companies 
                WHERE ruc IN ({company_placeholders})
            """), company_params).fetchall()
            company_map = {c[0]: c[1] for c in company_ids}
            
            # Obtener todos los cargos relevantes
            position_placeholders = ','.join([f':pos{i}' for i in range(len(unique_positions))])
            position_params = {f'pos{i}': pos for i, pos in enumerate(unique_positions)}
            position_ids = conn.execute(text(f"""
                SELECT name, id_position FROM public.positions 
                WHERE name IN ({position_placeholders})
            """), position_params).fetchall()
            position_map = {pos[0]: pos[1] for pos in position_ids}
            
            print(f"  ✅ Personas encontradas: {len(person_map)}")
            print(f"  ✅ Empresas encontradas: {len(company_map)}")
            print(f"  ✅ Cargos encontrados: {len(position_map)}")

            # 3. Preparar empleados para inserción en batch
            print("👥 Preparando empleados para inserción en batch...")
            employee_records = []
            active_person_ids = []
            skipped = 0
            
            for _, row in df_active.iterrows():
                person_id = person_map.get(row['person_ext_id'])
                company_id = company_map.get(row['company_ruc'])
                position_id = position_map.get(row['position_clean'])
                
                if person_id and company_id and position_id:
                    active_person_ids.append(person_id)
                    employee_records.append({
                        'start': row['start_date'],
                        'pos_id': position_id,
                        'comp_id': company_id,
                        'pers_id': person_id,
                        'ext_id': row['emp_ext_id']
                    })
                else:
                    skipped += 1
            
            # 4. Insertar empleados por batch
            if employee_records:
                print(f"📤 Insertando {len(employee_records)} empleados (se saltaron {skipped})...")
                batch_size = 1000
                for i in range(0, len(employee_records), batch_size):
                    batch = employee_records[i:i+batch_size]
                    conn.execute(text("""
                        INSERT INTO public.employees (start_date, status, position_id, company_id, person_id, external_id, updated_at)
                        VALUES (:start, '1', :pos_id, :comp_id, :pers_id, :ext_id, NOW())
                        ON CONFLICT (external_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """), batch)
                    print(f"  ✅ Lote {i//batch_size + 1}: {len(batch)} registros")

            # 5. DESACTIVAR USUARIOS QUE YA NO ESTÁN ACTIVOS
            if active_person_ids:
                print(f"📋 Desactivando usuarios no activos. IDs activos: {len(active_person_ids)}")
                placeholders = ','.join([f':id{i}' for i in range(len(active_person_ids))])
                params = {f'id{i}': pid for i, pid in enumerate(active_person_ids)}
                conn.execute(text(f"""
                    UPDATE public.users SET status = False, updated_at = NOW()
                    WHERE person_id NOT IN ({placeholders}) AND status = True
                """), params)

            # 6. CREAR/ACTUALIZAR USUARIOS - Batch Insert
            print("\n🔐 Paso 6: Creando/Actualizando usuarios por batch...")
            nuevos = conn.execute(text("""
                SELECT DISTINCT p.id_person, p.document_number
                FROM public.persons p
                INNER JOIN public.employees e ON p.id_person = e.person_id
                WHERE e.status = '1'
            """)).fetchall()
            
            print(f"📊 Se encontraron {len(nuevos)} usuarios a procesar")
            
            if nuevos:
                user_records = []
                for n in nuevos:
                    person_id = n[0]
                    document_number = n[1]
                    hashed_pass = hash_password(document_number)
                    user_records.append({
                        'id': str(uuid.uuid4()),
                        'user': document_number,
                        'pass': hashed_pass,
                        'pid': person_id
                    })
                
                # Insertar usuarios por batch
                batch_size = 1000
                usuarios_insertados = 0
                for i in range(0, len(user_records), batch_size):
                    batch = user_records[i:i+batch_size]
                    try:
                        conn.execute(text("""
                            INSERT INTO public.users (id_user, username, password, person_id, status, role, updated_at)
                            VALUES (:id, :user, :pass, :pid, True, 'USER', NOW())
                            ON CONFLICT (person_id) DO UPDATE SET
                                username = EXCLUDED.username,
                                password = EXCLUDED.password,
                                status = True,
                                updated_at = EXCLUDED.updated_at
                        """), batch)
                        usuarios_insertados += len(batch)
                        print(f"  ✅ Lote {i//batch_size + 1}: {len(batch)} usuarios")
                    except Exception as e:
                        error_msg = str(e).split('\n')[0]
                        print(f"⚠️ Error en lote {i//batch_size + 1}: {error_msg}")
        
        # El commit ocurre automáticamente al salir del with conn.begin()
        print(f"\n✅ COMMIT realizado.")
    
    # CONTEO DESPUÉS
    with engine_dest.connect() as temp_conn:
        usuarios_despues = temp_conn.execute(text("SELECT COUNT(*) FROM public.users")).scalar()
        print(f"📊 Usuarios DESPUÉS: {usuarios_despues}")
        print(f"📈 Diferencia: +{usuarios_despues - usuarios_antes}")

    print(f"\n✨ ¡Sincronización completada!")

if __name__ == "__main__":
    sync_active_only()