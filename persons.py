import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from config import sql_server_url, postgres_url
import os
import time

engine_src = create_engine(sql_server_url)

# 1. MOTOR OPTIMIZADO PARA AWS Y BATCH INSERTS
engine_dest = create_engine(
    postgres_url,
    pool_size=5,
    max_overflow=10, 
    pool_pre_ping=True,
    use_insertmanyvalues=True,  # <- CRÍTICO: Fuerza el batch insert real
    echo=False
)


def is_unique_violation(error):
    return getattr(error.orig, 'pgcode', None) == '23505'

def sync_persons():
    print("[START] Iniciando sincronización de personas con procesamiento por lotes...")
    print(f"[TIME] Timestamp: {datetime.now().isoformat()}")
    
    query = """
    SELECT DISTINCT
        RTRIM(dcIdTrabajador) as ext_id,
        dcNombres as fn,
        dcPaterno as lnp,
        dcMaterno as lnm,
        email,
        telefonos as phone,
        dcFecNaci as bday,
        RTRIM(dnNroDoc) as dnum,
        direccion as addr,
        sexo as gender,
        estadocivil as civil_status
    FROM [dbo].[V_PERSONA_AGRUPAMIENTO_3]
    WHERE dnNroDoc IS NOT NULL AND RTRIM(dnNroDoc) <> '' AND fnCodEstado = 1
    """
    
    upsert_query = text("""
        INSERT INTO public.persons (
            first_name, father_last_name, mother_last_name, 
            email, phone, birth_date, document_type, 
            document_number, address, gender, civil_status, external_id
        ) VALUES (
            :fn, :lnp, :lnm, :email, :phone, :bday, :dtype, :dnum, :addr, :gender, :civil_status, :ext_id
        )
        ON CONFLICT (external_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            father_last_name = EXCLUDED.father_last_name,
            mother_last_name = EXCLUDED.mother_last_name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            birth_date = EXCLUDED.birth_date,
            document_type = EXCLUDED.document_type,
            document_number = EXCLUDED.document_number,
            address = EXCLUDED.address,
            gender = EXCLUDED.gender,
            civil_status = EXCLUDED.civil_status;
    """)

    success_count = 0
    # 2. AUMENTO DE CHUNK SIZE
    chunk_size = int(os.getenv('PERSONS_CHUNK_SIZE', '25000'))
    max_retries = 3
    retry_delay = 1

    try:
        print("[DB] Conectando a bases de datos...")
        with engine_dest.connect() as conn:
            print("[OK] Conexión establecida. Iniciando lectura de datos...")
            chunk_number = 0
            
            for chunk in pd.read_sql(query, engine_src, chunksize=chunk_size):
                chunk_number += 1
                print(f"\n[BATCH {chunk_number}] Procesando {len(chunk)} registros...")
                
                print(f"  [CLEAN] Iniciando limpieza de datos en memoria...")
                chunk = chunk.fillna('')
                
                # 3. OPTIMIZACIÓN DE PANDAS: Mutación in-place (mucho más rápido)
                # Limpieza de strings general
                str_cols = ['fn', 'lnp', 'lnm', 'email', 'phone', 'dnum', 'addr', 'gender', 'civil_status', 'ext_id']
                for col in str_cols:
                    chunk[col] = chunk[col].astype(str).str.strip()
                
                # Truncados específicos
                chunk['phone'] = chunk['phone'].str[:50]
                chunk['addr'] = chunk['addr'].str[:255]
                
                # Manejo de Emails
                bad_emails = ~chunk['email'].str.contains('@', na=False)
                chunk.loc[bad_emails, 'email'] = "user_" + chunk.loc[bad_emails, 'ext_id'] + "@sistema.local"
                
                # Manejo de Fechas
                chunk['bday'] = pd.to_datetime(chunk['bday'], errors='coerce')
                chunk.loc[chunk['bday'] < '1900-01-02', 'bday'] = None
                chunk['bday'] = chunk['bday'].replace({pd.NaT: None}) # SQLAlchemy prefiere None sobre NaT
                
                # Campo constante
                chunk['dtype'] = 'DNI'

                # --- EJECUCIÓN POR LOTES ---
                print(f"  [EXEC] Iniciando UPSERT masivo en PostgreSQL...")
                
                # Extraemos solo las columnas necesarias, el orden no importa para diccionarios
                records = chunk[['fn', 'lnp', 'lnm', 'email', 'phone', 'bday', 'dtype', 'dnum', 'addr', 'gender', 'civil_status', 'ext_id']].to_dict('records')
                
                retry_count = 0
                inserted = False
                while retry_count < max_retries and not inserted:
                    try:
                        conn.execute(upsert_query, records)
                        conn.commit()
                        success_count += len(records)
                        print(f"  [OK] Lote #{chunk_number} sincronizado exitosamente. Total: {success_count}")
                        inserted = True
                    except Exception as e:
                        if isinstance(e, IntegrityError) and is_unique_violation(e):
                            conn.rollback()
                            inserted_count = 0
                            skipped_count = 0

                            for record in records:
                                try:
                                    with conn.begin_nested():
                                        conn.execute(upsert_query, record)
                                    inserted_count += 1
                                except IntegrityError as row_error:
                                    if not is_unique_violation(row_error):
                                        raise
                                    skipped_count += 1

                            conn.commit()
                            success_count += inserted_count
                            print(
                                f"  [OK] Lote #{chunk_number} sincronizado con "
                                f"{skipped_count} duplicado(s) omitido(s). Total: {success_count}"
                            )
                            inserted = True
                        else:
                            retry_count += 1
                            error_msg = str(e)[:150]
                            if retry_count < max_retries:
                                print(f"  [WARN] Error en lote #{chunk_number} (intento {retry_count}/{max_retries}): {error_msg}")
                                time.sleep(retry_delay)
                                conn.rollback()
                            else:
                                print(f"  [ERR] Lote #{chunk_number} falló tras {max_retries} intentos: {error_msg}")
                                conn.rollback()
                    
    except Exception as e:
        print(f"[ERR] Error crítico durante la sincronización: {e}")

    print(f"\n{'='*60}")
    print(f"[DONE] Sincronización terminada.")
    print(f"[TOTAL] Total de registros procesados: {success_count}")
    print(f"[TIME] Finalización: {datetime.now().isoformat()}")
    print(f"{'='*60}")

if __name__ == "__main__":
    sync_persons()