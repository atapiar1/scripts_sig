import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
import uuid
import bcrypt
import time
import os
from concurrent.futures import ThreadPoolExecutor
from config import sql_server_url, postgres_url

engine_src = create_engine(sql_server_url, pool_timeout=30, connect_args={"timeout": 30})

# 1. MOTOR OPTIMIZADO PARA AWS Y BATCH INSERTS MASIVOS
engine_dest = create_engine(
    postgres_url,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    use_insertmanyvalues=True,
    echo=False,
    connect_args={
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000"
    }
)


def cleanup_stale_transactions():
    """Termina transacciones abandonadas que puedan dejar bloqueos en Postgres."""
    try:
        with engine_dest.connect() as conn:
            conn.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND state = 'idle in transaction'
                  AND xact_start IS NOT NULL
                  AND now() - xact_start > INTERVAL '30 seconds'
            """))
            conn.commit()
            print("[DB] Transacciones inactivas viejas terminadas.")
    except Exception as exc:
        print(f"[WARN] No se pudieron limpiar transacciones viejas: {exc}")

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    if not password:
        raise ValueError("Password no puede estar vacío.")
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def build_user_record(user_data):
    person_id, document_number, email = user_data
    return {
        'id': str(uuid.uuid4()),
        'user': document_number,
        'pass': hash_password(document_number),
        'pid': person_id,
        'email': email
    }

def sync_active_only():
    print("🚀 Sincronización Final: Solo Activos + Control de Cambios de Cargo (Alta Velocidad)...")
    print(f"[DB] Conectando a destino en AWS...")
    cleanup_stale_transactions()

    # 2. EXTRACCIÓN MÁS RÁPIDA DE SQL SERVER
    query = """
    SELECT 
        RTRIM(dcIdSGHH) as emp_ext_id,
        RTRIM(dcIdTrabajador) as person_ext_id,
        RTRIM(dcRucEmpresa) as company_ruc,
        dcDesCargo as position_raw,
        ddFechaIngreso as start_date,
        fnCodEstado as status,
        u.email as user_email,
        RTRIM(a.desAreaOrg) as department_name,
        CAST(a.id_areaOrg AS varchar(50)) as source_department_id,
        tsa.fechaMod as department_mod
    FROM [dbo].[V_PERSONA_AGRUPAMIENTO_3] v
    LEFT JOIN [RUNAPROD_V2].[RunaUser].[Persona] p
        ON LTRIM(RTRIM(v.dcIdTrabajador)) = LTRIM(RTRIM(p.codSghh))
    LEFT JOIN [RUNAPROD_V2].[RunaUser].[Usuario] u
        ON u.persona_id = p.id_persona
    LEFT JOIN [RUNAPROD_V2].[RunaUser].[TrabajadorxSubAreaOrg] tsa
        ON tsa.persona_id = p.id_persona
    LEFT JOIN [RUNAPROD_V2].[RunaUser].[AreaOrg] a
        ON a.id_areaOrg = tsa.subAreaOrg_id
    WHERE v.fnCodEstado = 1
    """
    df_active = pd.read_sql(query, engine_src)
    print(f"📥 Se extrajeron {len(df_active)} empleados activos de SQL Server")
    
    if df_active.empty:
        print("⚠️ No hay empleados activos que procesar.")
        return
    
    # 3. LIMPIEZA VECTORIZADA COMPLETA (Evita bucles pesados de Pandas)
    df_active = df_active.fillna('')
    df_active['department_mod'] = pd.to_datetime(df_active['department_mod'], errors='coerce')
    df_active = df_active.sort_values(['person_ext_id', 'department_mod'], ascending=[True, False], na_position='last')
    df_active = df_active.drop_duplicates(subset=['person_ext_id'], keep='first').copy()
    df_active = df_active.drop_duplicates(subset=['emp_ext_id', 'person_ext_id', 'company_ruc', 'position_raw', 'department_name']).copy()
    df_active['person_ext_id'] = df_active['person_ext_id'].astype(str).str.strip()
    df_active['company_ruc'] = df_active['company_ruc'].astype(str).str.strip()
    df_active['position_raw'] = df_active['position_raw'].astype(str).str.strip()
    df_active['user_email'] = df_active['user_email'].astype(str).str.strip()
    df_active['department_name'] = df_active['department_name'].astype(str).str.strip()
    df_active['position_clean'] = df_active['position_raw'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df_active = df_active[df_active['person_ext_id'] != '']

    # 4. CONTEO AISLADO (Evita el error 'InvalidRequestError' de transacciones duplicadas)
    with engine_dest.connect() as temp_conn:
        usuarios_antes = temp_conn.execute(text("SELECT COUNT(*) FROM public.users")).scalar()
        print(f"📊 Usuarios ANTES: {usuarios_antes}")
    
    # CONEXIÓN PRINCIPAL CON TRANSACCIÓN ÚNICA
    with engine_dest.connect() as conn:
        with conn.begin():
            
            # --- PASO 1: SINCRONIZAR DEPARTAMENTOS Y CARGOS ---
            print("📋 Pre-cargando tablas de referencia en diccionarios...")
            person_map = dict(conn.execute(text("SELECT external_id, id_person FROM public.persons")).fetchall())
            company_map = dict(conn.execute(text("SELECT ruc, id_company FROM public.companies")).fetchall())

            department_candidates = (
                df_active.loc[df_active['department_name'] != '', ['department_name']]
                .drop_duplicates()
                .copy()
            )
            department_candidates['department_name'] = department_candidates['department_name'].astype(str).str.strip()
            department_candidates = department_candidates[department_candidates['department_name'] != '']
            print(f"📊 Departamentos detectados en origen: {len(department_candidates)}")
            print(department_candidates['department_name'].head(20).to_list())

            department_rows = []
            seen_departments = set()
            for _, row in department_candidates.iterrows():
                department_name = row['department_name'].strip()
                if not department_name:
                    continue
                if department_name in seen_departments:
                    continue
                seen_departments.add(department_name)
                department_rows.append({
                    'company_id': 1,
                    'name': department_name
                })

            if department_rows:
                print(f"🏢 Sincronizando {len(department_rows)} departamentos desde AreaOrg para company_id=1...")
                for dept in department_rows:
                    existing = conn.execute(text("""
                        SELECT id_department
                        FROM public.departments
                        WHERE company_id = :company_id AND name = :name
                        LIMIT 1
                    """), dept).fetchone()
                    if existing is None:
                        conn.execute(text("""
                            INSERT INTO public.departments (name, company_id, updated_at)
                            VALUES (:name, :company_id, NOW())
                        """), dept)

            department_pairs = conn.execute(text("SELECT company_id, name, id_department FROM public.departments WHERE company_id = 1")).fetchall()
            department_map = {
                name: id_department
                for company_id, name, id_department in department_pairs
            }

            df_active['pers_id'] = df_active['person_ext_id'].map(person_map)
            df_active['comp_id'] = df_active['company_ruc'].map(company_map)
            df_active['department_id'] = df_active.apply(
                lambda row: department_map.get(row['department_name']) if row['department_name'] else None,
                axis=1
            )
            df_active['department_id'] = df_active['department_id'].where(pd.notna(df_active['department_id']), None)

            unique_positions = (
                df_active.loc[df_active['position_clean'] != '', ['position_clean', 'department_id']]
                .drop_duplicates()
                .to_dict('records')
            )
            print(f"🏢 Sincronizando {len(unique_positions)} cargos...")

            def normalize_department_value(value):
                if pd.isna(value):
                    return None
                return int(value)

            pos_records = [
                {
                    'n': p['position_clean'],
                    'd': normalize_department_value(p['department_id']),
                    't': datetime.now()
                }
                for p in unique_positions
            ]
            if pos_records:
                batch_size = int(os.getenv('POSITIONS_BATCH_SIZE', '100'))
                for idx in range(0, len(pos_records), batch_size):
                    batch = pos_records[idx:idx + batch_size]
                    conn.execute(text("""
                        INSERT INTO public.positions (name, description, department_id, updated_at)
                        VALUES (:n, :n, :d, :t)
                        ON CONFLICT (name) DO UPDATE SET
                            department_id = EXCLUDED.department_id,
                            updated_at = EXCLUDED.updated_at
                    """), batch)
                    print(f"    [positions batch {idx // batch_size + 1}] {len(batch)} cargos procesados")

            position_map = dict(conn.execute(text("SELECT name, id_position FROM public.positions")).fetchall())

            # --- PASO 2: RELACIONAR EN MEMORIA (Cruzado instantáneo con Pandas) ---
            print("👥 Generando relaciones de llaves foráneas...")
            df_active['pos_id'] = df_active['position_clean'].map(position_map)

            # El email se sincroniza para toda persona encontrada, aunque el
            # empleado no tenga empresa o cargo válido para el upsert.
            valid_email_mask = df_active['user_email'].str.match(
                r'^[^@\s]+@[^@\s]+\.[^@\s]+$', na=False
            ) & df_active['pers_id'].notna()
            email_candidates = (
                df_active.loc[valid_email_mask, ['pers_id', 'user_email']]
                .drop_duplicates(subset=['pers_id'], keep='last')
                .copy()
            )
            email_by_person = (
                email_candidates
                .set_index('pers_id')['user_email']
                .to_dict()
            )
            if len(email_by_person) < valid_email_mask.sum():
                print("⚠️ Emails repetidos detectados; se conservará un único email por persona.")
            
            # Filtrar registros válidos
            df_valid = df_active.dropna(subset=['pers_id', 'comp_id', 'pos_id']).copy()
            skipped = len(df_active) - len(df_valid)
            
            # Variables fijas requeridas por la BD de destino
            df_valid['status'] = '1'
            df_valid['updated_at'] = datetime.now()

            # Renombrar columnas para encajar con los nombres reales de la tabla destino
            employee_records = df_valid[['start_date', 'status', 'pos_id', 'comp_id', 'pers_id', 'emp_ext_id', 'updated_at']].rename(
                columns={
                    'pos_id': 'position_id',
                    'comp_id': 'company_id',
                    'pers_id': 'person_id',
                    'emp_ext_id': 'external_id'
                }
            ).to_dict('records')
            
            active_person_ids = list(dict.fromkeys(df_valid['pers_id'].tolist()))

            # --- PASO 4: UPSERT DE EMPLEADOS (CON ACTUALIZACIÓN DE CARGO) ---
            if employee_records:
                print(f"📤 Insertando/Actualizando {len(employee_records)} empleados (se saltaron {skipped})...")

                metadata = MetaData()
                employees_table = Table('employees', metadata, autoload_with=conn)

                stmt = pg_insert(employees_table)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['external_id'],
                    set_={
                        'status': '1',
                        'position_id': stmt.excluded.position_id,
                        'company_id': stmt.excluded.company_id,
                        'start_date': stmt.excluded.start_date,
                        'updated_at': stmt.excluded.updated_at
                    }
                )

                # Reduce la presión del UPSERT masivo para evitar bloqueos por locks.
                batch_size = int(os.getenv('EMPLOYEES_BATCH_SIZE', '500'))
                for idx in range(0, len(employee_records), batch_size):
                    batch = employee_records[idx:idx + batch_size]
                    conn.execute(upsert_stmt, batch)
                    print(f"    [batch {idx // batch_size + 1}] {len(batch)} empleados procesados")
                print("  ✅ Empleados sincronizados con éxito.")

            # --- PASO 5: DESACTIVAR USUARIOS INACTIVOS (una sola operación SQL) ---
            print(f"📋 Revisando usuarios para desactivar...")
            if active_person_ids:
                deactivated = conn.execute(text("""
                    UPDATE public.users
                    SET status = False, updated_at = NOW()
                    WHERE status = True
                      AND NOT (person_id = ANY(:pids))
                """), {"pids": active_person_ids})
            else:
                deactivated = conn.execute(text("""
                    UPDATE public.users
                    SET status = False, updated_at = NOW()
                    WHERE status = True
                """))
            if deactivated.rowcount:
                print(f"🔌 Desactivados {deactivated.rowcount} usuarios inactivos.")

            # --- PASO 6: CREAR / REACTIVAR USUARIOS (Evita el cuello de botella de bcrypt) ---
            print("\n🔐 Procesando creación y reactivación de credenciales...")
            nuevos = conn.execute(text("""
                SELECT DISTINCT p.id_person, p.document_number
                FROM public.persons p
                INNER JOIN public.employees e ON p.id_person = e.person_id
                LEFT JOIN public.users u ON u.person_id = p.id_person
                WHERE e.status = '1' AND u.person_id IS NULL
            """)).fetchall()

            # SOLO HASHEAMOS USUARIOS NUEVOS. bcrypt libera el GIL, por lo que
            # varios hashes pueden ejecutarse en paralelo en los vCPU de AWS.
            users_to_insert = []
            if nuevos:
                existing_email_rows = conn.execute(text("""
                    SELECT email, person_id
                    FROM public.users
                    WHERE email = ANY(:emails)
                """), {'emails': list(email_by_person.values())}).fetchall() if email_by_person else []
                emails_used_by_other = {
                    email for email, person_id in existing_email_rows
                    if email_by_person.get(person_id) != email
                }
                insert_email_by_person = {
                    person_id: email
                    for person_id, email in email_by_person.items()
                    if email not in emails_used_by_other
                }
                max_workers = min(8, os.cpu_count() or 1)
                new_user_data = [
                    (
                        person_id,
                        document_number,
                        insert_email_by_person.get(person_id)
                    )
                    for person_id, document_number in nuevos
                ]
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    users_to_insert = list(executor.map(build_user_record, new_user_data))

            if users_to_insert:
                print(f"✨ Creando {len(users_to_insert)} usuarios NUEVOS en la plataforma...")
                conn.execute(text("""
                    INSERT INTO public.users (
                        id_user, username, password, email, person_id,
                        status, role, updated_at
                    )
                    VALUES (:id, :user, :pass, :email, :pid, True, 'USER', NOW())
                """), users_to_insert)

            # Actualiza usuarios existentes con el email real del origen.
            if email_by_person:
                email_update = conn.execute(text("""
                    UPDATE public.users u
                    SET email = source.email, updated_at = NOW()
                    FROM unnest(
                        CAST(:pids AS integer[]),
                        CAST(:emails AS text[])
                    ) AS source(person_id, email)
                    WHERE u.person_id = source.person_id
                                            AND NOT EXISTS (
                                                    SELECT 1
                                                    FROM public.users other
                                                    WHERE other.email = source.email
                                                        AND other.person_id <> source.person_id
                                            )
                """), {
                      'pids': [int(person_id) for person_id in email_by_person],
                      'emails': list(email_by_person.values())
                })
                print(f"✉️ Emails actualizados: {email_update.rowcount}; "
                      f"emails válidos en origen: {len(email_by_person)}")
            
            if active_person_ids:
                reactivated = conn.execute(text("""
                    UPDATE public.users
                    SET status = True, updated_at = NOW()
                    WHERE status = False AND person_id = ANY(:pids)
                """), {"pids": active_person_ids})
                if reactivated.rowcount:
                    print(f"♻️ Reactivados {reactivated.rowcount} usuarios existentes.")

        print(f"\n✅ COMMIT transaccional realizado con éxito.")
    
    # CONTEO DESPUÉS
    with engine_dest.connect() as temp_conn:
        usuarios_despues = temp_conn.execute(text("SELECT COUNT(*) FROM public.users")).scalar()
        print(f"📊 Usuarios DESPUÉS: {usuarios_despues}")
        print(f"📈 Diferencia: +{usuarios_despues - usuarios_antes}")

    print(f"\n✨ ¡Sincronización masiva de alto rendimiento completada!")

if __name__ == "__main__":
    sync_active_only()