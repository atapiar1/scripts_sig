import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from config import sql_server_url, postgres_url
import time

engine_src = create_engine(sql_server_url)
# PostgreSQL optimizado para Neon
engine_dest = create_engine(
    postgres_url,
    pool_size=5,  # Más conexiones activas para mejor concurrencia
    max_overflow=5,  # Permite overflow para picos de carga
    pool_pre_ping=True,  # Verifica conexión antes de usar
    echo=False
)

def sync_persons():
    print("[START] Iniciando sincronización de personas con procesamiento por lotes...")
    print(f"[TIME] Timestamp: {datetime.now().isoformat()}")
    
    query = """
    SELECT DISTINCT
        RTRIM(dcIdTrabajador) as external_id,
        dcNombres as first_name,
        dcPaterno as father_last_name,
        dcMaterno as mother_last_name,
        email,
        telefonos as phone,
        dcFecNaci as birth_date,
        RTRIM(dnNroDoc) as document_number,
        direccion as address,
        sexo as gender,
        estadocivil as civil_status
    FROM [dbo].[V_PERSONA_AGRUPAMIENTO_3]
    WHERE dnNroDoc IS NOT NULL AND RTRIM(dnNroDoc) <> '' AND fnCodEstado = 1
    """
    
    # Query de UPSERT (igual que la tuya)
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
    chunk_size = 1000  # Aumentado a 1000 para Neon
    max_retries = 3
    retry_delay = 1  # Reducido a 1 segundo

    try:
        print("[DB] Conectando a bases de datos...")
        with engine_dest.connect() as conn:
            print("[OK] Conexión establecida. Iniciando lectura de datos...")
            # pd.read_sql con chunksize devuelve un generador
            chunk_number = 0
            for chunk in pd.read_sql(query, engine_src, chunksize=chunk_size):
                chunk_number += 1
                print(f"\n[BATCH {chunk_number}] Procesando {len(chunk)} registros...")
                
                # --- 1. LIMPIEZA VECTORIZADA CON PANDAS ---
                print(f"  [CLEAN] Iniciando limpieza de datos...")
                chunk = chunk.fillna('')
                
                # Fechas
                chunk['birth_date'] = pd.to_datetime(chunk['birth_date'], errors='coerce')
                chunk.loc[chunk['birth_date'] < '1900-01-02', 'birth_date'] = None
                
                # Limpieza de textos y recortes
                ext_ids = chunk['external_id'].astype(str).str.strip()
                
                # Emails: Crear máscara donde no hay '@'
                emails = chunk['email'].astype(str).str.strip()
                bad_emails = ~emails.str.contains('@', na=False)
                emails.loc[bad_emails] = "user_" + ext_ids.loc[bad_emails] + "@sistema.local"
                print(f"  [OK] Limpieza completada. Emails inválidos corregidos: {bad_emails.sum()}")

                # --- 2. PREPARACIÓN DE DICCIONARIO PARA BATCH UPSERT ---
                print(f"  [PREP] Preparando parámetros para UPSERT...")
                # Mapeamos las columnas exactamente a los nombres de tus parámetros SQL (:fn, :lnp, etc.)
                params_df = pd.DataFrame({
                    'fn': chunk['first_name'].astype(str).str.strip(),
                    'lnp': chunk['father_last_name'].astype(str).str.strip(),
                    'lnm': chunk['mother_last_name'].astype(str).str.strip(),
                    'email': emails,
                    'phone': chunk['phone'].astype(str).str.strip().str[:50],
                    'bday': chunk['birth_date'].where(pd.notnull(chunk['birth_date']), None),
                    'dtype': 'DNI',
                    'dnum': chunk['document_number'].astype(str).str.strip(),
                    'addr': chunk['address'].astype(str).str.strip().str[:255],
                    'gender': chunk['gender'].astype(str).str.strip(),
                    'civil_status': chunk['civil_status'].astype(str).str.strip(),
                    'ext_id': ext_ids
                })

                # --- 3. EJECUCIÓN POR LOTES (BATCH EXECUTION) ---
                print(f"  [EXEC] Iniciando UPSERT en PostgreSQL...")
                # Pasarle una lista de diccionarios a SQLAlchemy dispara `executemany` automáticamente
                records = params_df.to_dict('records')
                
                retry_count = 0
                inserted = False
                while retry_count < max_retries and not inserted:
                    try:
                        conn.execute(upsert_query, records)
                        conn.commit() # Commit por cada bloque completado
                        success_count += len(records)
                        print(f"[OK] Lote #{chunk_number}: {len(records)} registros sincronizado. Total acumulado: {success_count}")
                        inserted = True
                    except Exception as e:
                        retry_count += 1
                        error_msg = str(e)[:150]
                        if retry_count < max_retries:
                            print(f"[WARN] Error en lote #{chunk_number} (intento {retry_count}/{max_retries}): {error_msg}")
                            print(f"   [WAIT] Esperando {retry_delay} segundos antes de reintentar...")
                            time.sleep(retry_delay)
                            conn.rollback()
                        else:
                            print(f"[ERR] Lote #{chunk_number} falló después de {max_retries} intentos: {error_msg}")
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