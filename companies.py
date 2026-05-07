import pandas as pd
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

def sync_companies():
    print("[START] Iniciando sincronización de compañías...")
    print(f"[TIME] Timestamp: {datetime.now().isoformat()}")
    
    # 1. Extraer empresas únicas de la vista
    # Agrupamos por RUC para no procesar duplicados innecesariamente
    query = """
    SELECT DISTINCT 
        dcRucEmpresa as ruc, 
        dcDesEmpresa as legal_name
    FROM [dbo].[V_PERSONA_AGRUPAMIENTO_3]
    WHERE dcRucEmpresa IS NOT NULL
    """
    print("[DB] Conectando a SQL Server...")
    df_companies = pd.read_sql(query, engine_src)
    print(f"[OK] Se extrajeron {len(df_companies)} compañías")

    # 2. Limpieza básica
    print("[CLEAN] Limpiando datos...")
    df_companies['legal_name'] = df_companies['legal_name'].str.strip()
    df_companies['ruc'] = df_companies['ruc'].str.strip()
    
    # Filtrar empresas con nombres en blanco
    df_companies = df_companies[df_companies['legal_name'] != '']
    print(f"[OK] Después de limpieza: {len(df_companies)} compañías")

    # 3. Upsert en PostgreSQL por lotes
    chunk_size = 500  # Aumentado a 500 para Neon
    max_retries = 3
    retry_delay = 1  # Reducido a 1 segundo
    
    print(f"[EXEC] Iniciando UPSERT en PostgreSQL (lotes de {chunk_size})...")
    
    upsert_query = text("""
        INSERT INTO public.companies (
            legal_name, 
            ruc, 
            address, 
            city, 
            updated_at, 
            status
        ) VALUES (
            :legal_name, 
            :ruc, 
            'Dirección pendiente',
            'Ciudad pendiente',
            :updated_at, 
            '1'
        )
        ON CONFLICT (ruc) DO UPDATE SET
            legal_name = EXCLUDED.legal_name,
            updated_at = EXCLUDED.updated_at
    """)
    
    success_count = 0
    with engine_dest.connect() as conn:
        # Procesar por lotes
        for i in range(0, len(df_companies), chunk_size):
            chunk = df_companies.iloc[i:i+chunk_size]
            batch_num = i // chunk_size + 1
            print(f"\n[BATCH {batch_num}] Procesando {len(chunk)} registros...")
            
            batch_success = 0
            batch_failed = 0
            
            # Insertar cada registro de forma individual con reintentos
            for idx, (_, row) in enumerate(chunk.iterrows()):
                retry_count = 0
                inserted = False
                
                while retry_count < max_retries and not inserted:
                    try:
                        conn.execute(
                            upsert_query,
                            {
                                'legal_name': row['legal_name'],
                                'ruc': row['ruc'],
                                'updated_at': datetime.now()
                            }
                        )
                        conn.commit()
                        batch_success += 1
                        inserted = True
                    except Exception as e:
                        retry_count += 1
                        error_msg = str(e)[:150]
                        if retry_count < max_retries:
                            time.sleep(retry_delay)
                            conn.rollback()
                        else:
                            batch_failed += 1
                            conn.rollback()
                            print(f"  [ERR] Registro {idx+1} (RUC: {row['ruc']}) falló: {error_msg}")
                            inserted = True
            
            success_count += batch_success
            print(f"[OK] Lote #{batch_num}: {batch_success} exitosos, {batch_failed} fallidos. Total acumulado: {success_count}")
    
    print(f"\n{'='*60}")
    print(f"[DONE] Sincronización de compañías terminada.")
    print(f"[TOTAL] Total de compañías procesadas: {success_count}")
    print(f"[TIME] Finalización: {datetime.now().isoformat()}")
    print(f"{'='*60}")

if __name__ == "__main__":
    sync_companies()