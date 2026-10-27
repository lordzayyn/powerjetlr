# -*- coding: utf-8 -*-
"""
db.py - Gerenciador do Banco de Dados Relacional Híbrido (PostgreSQL na Nuvem / SQLite Local)
Oferece persistência estruturada permanente no Render (PostgreSQL, Supabase, Neon ou SQLite com WAL).
"""

import os
import sqlite3
import json
import time
from datetime import datetime, timedelta

# URL de fallback do Render
DEFAULT_RENDER_PG_URL = "postgresql://dc_bauru_db_user:opbS3AWCwu8U6VYSwQueJ1qHudNZJ1nD@dpg-dap3co6k1f9s739amlk0-a/dc_bauru_db"

def get_database_url():
    """Recupera e normaliza a URL do banco PostgreSQL de variáveis de ambiente."""
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or os.environ.get("RENDER_DATABASE_URL")
    if not url and (os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID")):
        url = DEFAULT_RENDER_PG_URL
    if url and url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

DATABASE_URL = get_database_url()
IS_POSTGRES = bool(DATABASE_URL)
_last_pg_error_time = 0
_current_engine = "SQLite"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logistica_bi.db")

class UniversalCursor:
    def __init__(self, cursor, is_postgres=False):
        self._cur = cursor
        self.is_postgres = is_postgres

    def execute(self, sql, params=None):
        cleaned_sql = sql
        if self.is_postgres:
            # Compatibilidade de tipos SQLite -> PostgreSQL
            cleaned_sql = cleaned_sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            cleaned_sql = cleaned_sql.replace("INTEGER PRIMARY KEY AUTO_INCREMENT", "SERIAL PRIMARY KEY")
            # Substitui marcadores posicionais '?' por '%s' para o driver psycopg2
            cleaned_sql = cleaned_sql.replace("?", "%s")
            
        if params is not None:
            if self.is_postgres and isinstance(params, list):
                params = tuple(params)
            return self._cur.execute(cleaned_sql, params)
        else:
            return self._cur.execute(cleaned_sql)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self):
        return self._cur.rowcount

class UniversalConnection:
    def __init__(self, conn, is_postgres=False, keep_open=False):
        self._conn = conn
        self.is_postgres = is_postgres
        self.keep_open = keep_open

    def cursor(self):
        return UniversalCursor(self._conn.cursor(), self.is_postgres)

    def commit(self):
        try:
            return self._conn.commit()
        except Exception:
            pass

    def rollback(self):
        try:
            return self._conn.rollback()
        except Exception:
            pass

    def close(self):
        if not self.keep_open:
            try:
                return self._conn.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        if not self.keep_open:
            self.close()

_shared_pg_conn = None

def get_connection():
    """Retorna uma conexÃ£o Universal pronta para PostgreSQL na nuvem ou SQLite local com pool de conexÃ£o."""
    global _last_pg_error_time, _current_engine, _shared_pg_conn
    target_url = get_database_url()
    
    # Tenta PostgreSQL com reuso de conexÃ£o persistente de alta performance
    if target_url and (time.time() - _last_pg_error_time > 15):
        try:
            import psycopg2
            import psycopg2.extras
            
            if _shared_pg_conn is not None:
                try:
                    if not _shared_pg_conn.closed:
                        with _shared_pg_conn.cursor() as test_cur:
                            test_cur.execute("SELECT 1;")
                        _current_engine = "PostgreSQL"
                        return UniversalConnection(_shared_pg_conn, is_postgres=True, keep_open=True)
                except Exception:
                    try:
                        _shared_pg_conn.close()
                    except Exception:
                        pass
                    _shared_pg_conn = None

            conn_args = {
                "cursor_factory": psycopg2.extras.RealDictCursor,
                "connect_timeout": 6
            }
            if "sslmode" not in target_url and ("localhost" not in target_url and "127.0.0.1" not in target_url):
                if "dpg-" in target_url and "." not in target_url.split("@")[-1].split("/")[0]:
                    pass
                else:
                    conn_args["sslmode"] = "require"

            _shared_pg_conn = psycopg2.connect(target_url, **conn_args)
            _shared_pg_conn.autocommit = True
            _current_engine = "PostgreSQL"
            return UniversalConnection(_shared_pg_conn, is_postgres=True, keep_open=True)
        except Exception as e:
            _last_pg_error_time = time.time()
            _current_engine = "SQLite"
            _shared_pg_conn = None
            print(f"[DB] Falha na conexÃ£o PostgreSQL ({e}). Usando SQLite local.")
            
    # Fallback SQLite WAL
    _current_engine = "SQLite"
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return UniversalConnection(conn, is_postgres=False, keep_open=False)

def get_db_status():
    """Retorna o status atual do motor de banco de dados para a API e dashboard."""
    global _current_engine
    db_url = get_database_url()
    is_cloud_configured = bool(db_url)
    
    # Testa conexão ativa
    connected = False
    active_engine = _current_engine
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            connected = True
            active_engine = "PostgreSQL" if conn.is_postgres else "SQLite"
    except Exception as e:
        connected = False

    return {
        "connected": connected,
        "engine": active_engine,
        "is_postgres": active_engine == "PostgreSQL",
        "has_database_url": is_cloud_configured,
        "db_path": DB_PATH if active_engine == "SQLite" else None,
        "message": "Conectado ao PostgreSQL Permanente na Nuvem" if active_engine == "PostgreSQL" else "Operando em SQLite local (atenção no Render: configure DATABASE_URL para persistência entre reinícios)"
    }

_db_initialized = False

def init_db(force=False):
    global _db_initialized
    if _db_initialized and not force:
        return
    """Cria a estrutura de tabelas e índices se ainda não existirem."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. Tabela de Volumetria Diária - Viagens
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_volumetria_viagens (
                id_transporte TEXT PRIMARY KEY,
                data_ref TEXT NOT NULL,
                pdd_saida TEXT,
                status_viagem TEXT,
                pedido_sistema INTEGER DEFAULT 0,
                previsao_chegada TEXT DEFAULT '',
                chegada_real TEXT DEFAULT '',
                inicio_coleta TEXT DEFAULT '',
                fim_coleta TEXT DEFAULT '',
                status TEXT DEFAULT '',
                obs TEXT DEFAULT '',
                bipado INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vol_data_ref ON tb_volumetria_viagens(data_ref);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vol_pdd ON tb_volumetria_viagens(pdd_saida);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vol_status_viagem ON tb_volumetria_viagens(status_viagem);")
        except Exception:
            pass

        # 2. Tabela de Volumetria Diária - KPIs do Piso
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_volumetria_kpis (
                data_ref TEXT PRIMARY KEY,
                previsto INTEGER DEFAULT 0,
                bipados INTEGER DEFAULT 0,
                coletas INTEGER DEFAULT 0,
                total_no_piso INTEGER DEFAULT 0,
                diferenca INTEGER DEFAULT 0,
                eficiencia REAL DEFAULT 100.0,
                total_viagens INTEGER DEFAULT 0,
                viagens_finalizadas INTEGER DEFAULT 0,
                updated_at TEXT
            );
        """)

        # 3. Tabela de Frota de PDAs (PDA-00 a PDA-50)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_pda_frota (
                codigo TEXT PRIMARY KEY,
                status TEXT DEFAULT 'Disponível',
                obs TEXT DEFAULT 'OK',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_frota_status ON tb_pda_frota(status);")
        except Exception:
            pass

        # 4. Tabela de Responsáveis Autorizados por Turno
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_pda_autorizados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                turno TEXT NOT NULL,
                nome TEXT NOT NULL,
                UNIQUE(turno, nome)
            );
        """)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_aut_turno ON tb_pda_autorizados(turno);")
        except Exception:
            pass

        # 5. Tabela de Registros de Retirada, Recebimento e Devolução de PDAs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_pda_registros (
                id TEXT PRIMARY KEY,
                pda TEXT NOT NULL,
                turno TEXT NOT NULL,
                responsavel TEXT NOT NULL,
                setor TEXT NOT NULL,
                data_retirada TEXT NOT NULL,
                data_devolucao TEXT,
                status TEXT NOT NULL,
                obs_retirada TEXT,
                obs_devolucao TEXT,
                estado_devolucao TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_reg_pda ON tb_pda_registros(pda);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_reg_status ON tb_pda_registros(status);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_reg_turno ON tb_pda_registros(turno);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pda_reg_retirada ON tb_pda_registros(data_retirada);")
        except Exception:
            pass

        # 6. Tabela de Report de Início da Operação
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tb_pda_report_inicio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_ref TEXT NOT NULL,
                backlog_inicial TEXT DEFAULT '692',
                volume_previsto TEXT DEFAULT '65.129',
                inicio_operacao TEXT DEFAULT '15:11',
                clts_presentes_18h TEXT DEFAULT '32',
                absenteismo_18h TEXT DEFAULT '4',
                clts_presentes_23h TEXT DEFAULT '— 60',
                absenteismo_23h TEXT DEFAULT '— 4',
                diaristas_solicitados_15h TEXT DEFAULT '20',
                diaristas_presentes_15h TEXT DEFAULT '18',
                diaristas_solicitados_20h TEXT DEFAULT '—',
                diaristas_presentes_20h TEXT DEFAULT '—',
                phd_previsto TEXT DEFAULT '—',
                obs_falta_injustificada TEXT DEFAULT '—',
                obs_atestados TEXT DEFAULT '-',
                obs_pontos_atencao TEXT DEFAULT 'N/A',
                raw_text TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

# ==========================================================
# OPERAÇÕES DE VOLUMETRIA DIÁRIA (HORÁRIOS & STATUS)
# ==========================================================

def _normalize_date_iso(d_str):
    if not d_str:
        return datetime.now().strftime("%Y-%m-%d")
    s = str(d_str).strip()
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3 and len(parts[2]) == 4:
            return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return datetime.now().strftime("%Y-%m-%d")

def _format_date_br(d_str):
    if not d_str:
        return datetime.now().strftime("%d/%m/%Y")
    s = str(d_str).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        parts = s[:10].split("-")
        return f"{parts[2]}/{parts[1]}/{parts[0]}"
    if "/" in s:
        return s
    return datetime.now().strftime("%d/%m/%Y")

def save_volumetria_data(data_dict, replace_all=False):
    """Salva viagens e KPIs de Volumetria Diária no banco com persistência permanente dos horários e status."""
    if not data_dict:
        return False
        
    raw_date = data_dict.get("data_referencia") or datetime.now().strftime("%Y-%m-%d")
    data_ref = _normalize_date_iso(raw_date)
    updated_at = data_dict.get("updated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    records = data_dict.get("records") or []
    kpis = data_dict.get("kpis") or {}
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        if replace_all:
            # Ao importar nova planilha, busca horários e status que já foram preenchidos anteriormente
            # para NÃO perder nada do que os operadores já preencheram!
            cursor.execute("SELECT id_transporte, chegada_real, inicio_coleta, fim_coleta, status, obs, bipado FROM tb_volumetria_viagens;")
            existing_map = {row["id_transporte"]: row for row in cursor.fetchall()}
            
            # Mescla dados existentes nos novos registros
            for r in records:
                id_tr = str(r.get("id_transporte") or "").strip()
                if id_tr in existing_map:
                    ex = existing_map[id_tr]
                    if not r.get("chegada_real") and ex["chegada_real"]:
                        r["chegada_real"] = ex["chegada_real"]
                    if not r.get("inicio_coleta") and ex["inicio_coleta"]:
                        r["inicio_coleta"] = ex["inicio_coleta"]
                    if not r.get("fim_coleta") and ex["fim_coleta"]:
                        r["fim_coleta"] = ex["fim_coleta"]
                    if not r.get("status") and ex["status"]:
                        r["status"] = ex["status"]
                    if not r.get("obs") and ex["obs"]:
                        r["obs"] = ex["obs"]
                    if (not r.get("bipado") or int(r.get("bipado") or 0) == 0) and ex["bipado"]:
                        r["bipado"] = ex["bipado"]
            
            cursor.execute("DELETE FROM tb_volumetria_viagens;")
            cursor.execute("DELETE FROM tb_volumetria_kpis;")

        # Upsert Viagens garantindo que todos os horários e status persistam
        for r in records:
            id_transporte = str(r.get("id_transporte") or "").strip()
            if not id_transporte:
                continue
                
            cursor.execute("""
                INSERT INTO tb_volumetria_viagens (
                    id_transporte, data_ref, pdd_saida, status_viagem,
                    pedido_sistema, previsao_chegada, chegada_real, inicio_coleta, fim_coleta,
                    status, obs, bipado, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id_transporte) DO UPDATE SET
                    data_ref = EXCLUDED.data_ref,
                    pdd_saida = EXCLUDED.pdd_saida,
                    status_viagem = EXCLUDED.status_viagem,
                    pedido_sistema = EXCLUDED.pedido_sistema,
                    previsao_chegada = CASE WHEN EXCLUDED.previsao_chegada != '' THEN EXCLUDED.previsao_chegada ELSE tb_volumetria_viagens.previsao_chegada END,
                    chegada_real = CASE WHEN EXCLUDED.chegada_real != '' THEN EXCLUDED.chegada_real ELSE tb_volumetria_viagens.chegada_real END,
                    inicio_coleta = CASE WHEN EXCLUDED.inicio_coleta != '' THEN EXCLUDED.inicio_coleta ELSE tb_volumetria_viagens.inicio_coleta END,
                    fim_coleta = CASE WHEN EXCLUDED.fim_coleta != '' THEN EXCLUDED.fim_coleta ELSE tb_volumetria_viagens.fim_coleta END,
                    status = CASE WHEN EXCLUDED.status != '' THEN EXCLUDED.status ELSE tb_volumetria_viagens.status END,
                    obs = CASE WHEN EXCLUDED.obs != '' THEN EXCLUDED.obs ELSE tb_volumetria_viagens.obs END,
                    bipado = CASE WHEN EXCLUDED.bipado > 0 THEN EXCLUDED.bipado ELSE tb_volumetria_viagens.bipado END,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                id_transporte,
                data_ref,
                r.get("pdd_saida") or "",
                r.get("status_viagem") or "",
                int(r.get("pedido_sistema") or 0),
                r.get("previsao_chegada") or "",
                r.get("chegada_real") or "",
                r.get("inicio_coleta") or "",
                r.get("fim_coleta") or "",
                r.get("status") if r.get("status") is not None else "",
                r.get("obs") or "",
                int(r.get("bipado") or 0)
            ))
            
        # Upsert KPIs
        sum_prev = sum(int(r.get("pedido_sistema") or 0) for r in records)
        previsto = int(kpis.get("previsto")) if (kpis.get("previsto") is not None and int(kpis.get("previsto")) > 0) else sum_prev
        bipados = sum(int(r.get("bipado") or 0) for r in records)
        
        if data_dict.get("coletas") is not None:
            coletas = int(data_dict.get("coletas") or 0)
        elif kpis.get("coletas") is not None:
            coletas = int(kpis.get("coletas") or 0)
        else:
            coletas = 10790
            
        total_piso = bipados + coletas
        diferenca = bipados - previsto
        eficiencia = round((bipados / previsto * 100), 2) if previsto > 0 else 100.0
        total_viagens = len(records)
        viagens_finalizadas = sum(1 for r in records if "FINALIZADO" in str(r.get("status", "")).upper())
        
        cursor.execute("""
            INSERT INTO tb_volumetria_kpis (
                data_ref, previsto, bipados, coletas, total_no_piso, diferenca,
                eficiencia, total_viagens, viagens_finalizadas, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(data_ref) DO UPDATE SET
                previsto = EXCLUDED.previsto,
                bipados = EXCLUDED.bipados,
                coletas = EXCLUDED.coletas,
                total_no_piso = EXCLUDED.total_no_piso,
                diferenca = EXCLUDED.diferenca,
                eficiencia = EXCLUDED.eficiencia,
                total_viagens = EXCLUDED.total_viagens,
                viagens_finalizadas = EXCLUDED.viagens_finalizadas,
                updated_at = EXCLUDED.updated_at;
        """, (
            data_ref, previsto, bipados, coletas, total_piso, diferenca,
            eficiencia, total_viagens, viagens_finalizadas, updated_at
        ))
        
        conn.commit()
    return True

def _sort_volumetria_records(records):
    """Ordena os registros da Volumetria Diária agrupando por base (SP BRE, SN RAO, IBG, EXT) e horário."""
    if not records:
        return []
        
    def sort_key(r):
        pdd = str(r.get("pdd_saida") or "").strip().upper()
        if "SP BRE" in pdd or "BRE" in pdd:
            pdd_prio = 1
        elif "SN RAO" in pdd or "RAO" in pdd:
            pdd_prio = 2
        elif "IBG" in pdd:
            pdd_prio = 3
        elif "EXT" in pdd:
            pdd_prio = 4
        else:
            pdd_prio = 5
        
        prev = str(r.get("previsao_chegada") or "").strip()
        id_tr = str(r.get("id_transporte") or "")
        return (pdd_prio, pdd, prev, id_tr)

    return sorted(records, key=sort_key)

def get_volumetria_data(data_ref=None):
    """Obtém os dados consolidados de Volumetria Diária do banco de dados relacional."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM tb_volumetria_viagens ORDER BY id_transporte;")
        records_rows = cursor.fetchall()
        
        cursor.execute("SELECT * FROM tb_volumetria_kpis ORDER BY updated_at DESC LIMIT 1;")
        kpi_row = cursor.fetchone()
        
        if not records_rows and not kpi_row:
            return None
            
        data_ref_val = None
        if kpi_row and "data_ref" in kpi_row.keys():
            data_ref_val = kpi_row["data_ref"]
        elif records_rows and len(records_rows) > 0 and "data_ref" in records_rows[0].keys():
            data_ref_val = records_rows[0]["data_ref"]
            
        if not data_ref_val:
            data_ref_val = datetime.now().strftime("%Y-%m-%d")
            
        records = []
        status_viagem_dist = {}
        status_operacional_dist = {}
        
        for r in records_rows:
            rec = {
                "id_transporte": r["id_transporte"],
                "pdd_saida": r["pdd_saida"],
                "status_viagem": r["status_viagem"],
                "pedido_sistema": r["pedido_sistema"],
                "previsao_chegada": (r["previsao_chegada"] if "previsao_chegada" in r.keys() else "") or "",
                "chegada_real": (r["chegada_real"] if "chegada_real" in r.keys() else "") or "",
                "inicio_coleta": (r["inicio_coleta"] if "inicio_coleta" in r.keys() else "") or "",
                "fim_coleta": (r["fim_coleta"] if "fim_coleta" in r.keys() else "") or "",
                "status": (r["status"] if "status" in r.keys() else "") or "",
                "obs": (r["obs"] if "obs" in r.keys() else "") or "",
                "bipado": r["bipado"] or 0
            }
            records.append(rec)
            
            sv = r["status_viagem"] or "Outro"
            if "conclu" in sv.lower():
                sv_key = "Concluído"
            elif "trânsito" in sv.lower() or "transito" in sv.lower():
                sv_key = "Em Trânsito"
            elif "programa" in sv.lower():
                sv_key = "Programado"
            else:
                sv_key = sv
            status_viagem_dist[sv_key] = status_viagem_dist.get(sv_key, 0) + 1
            
            st_op = (r["status"] or "EM BRANCO").strip().upper()
            status_operacional_dist[st_op] = status_operacional_dist.get(st_op, 0) + 1
            
        if kpi_row:
            kpis = {
                "previsto": kpi_row["previsto"],
                "bipados": kpi_row["bipados"],
                "coletas": kpi_row["coletas"],
                "total_no_piso": kpi_row["total_no_piso"],
                "diferenca": kpi_row["diferenca"],
                "eficiencia": kpi_row["eficiencia"],
                "total_viagens": kpi_row["total_viagens"],
                "viagens_finalizadas": kpi_row["viagens_finalizadas"]
            }
            updated_at = kpi_row["updated_at"]
            coletas_val = kpi_row["coletas"]
        else:
            prev = sum(r["pedido_sistema"] for r in records)
            bip = sum(r["bipado"] for r in records)
            coletas_val = 10790
            kpis = {
                "previsto": prev,
                "bipados": bip,
                "coletas": coletas_val,
                "total_no_piso": bip + coletas_val,
                "diferenca": bip - prev,
                "eficiencia": round((bip / prev * 100), 2) if prev > 0 else 100.0,
                "total_viagens": len(records),
                "viagens_finalizadas": sum(1 for r in records if "FINALIZADO" in str(r.get("status", "")).upper())
            }
            updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        records = _sort_volumetria_records(records)
        return {
            "data_referencia": _format_date_br(data_ref_val),
            "updated_at": updated_at,
            "coletas": coletas_val,
            "kpis": kpis,
            "records": records,
            "status_viagem_dist": status_viagem_dist,
            "status_operacional_dist": status_operacional_dist
        }

def update_volumetria_obs(id_transporte, obs):
    """Atualiza a observação de uma viagem no banco."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tb_volumetria_viagens
            SET obs = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id_transporte = ?;
        """, (obs, id_transporte))
        conn.commit()
        return cursor.rowcount > 0

def delete_volumetria_viagem(id_transporte):
    """Exclui uma viagem de volumetria pelo ID de Transporte."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tb_volumetria_viagens WHERE id_transporte = ?;", (id_transporte,))
        conn.commit()
        return cursor.rowcount > 0

# ==========================================================
# OPERAÇÕES DE RANKING E PRODUTIVIDADE HORA A HORA
# ==========================================================

RANKING_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ranking_data.json")

def _get_empty_ranking_template():
    horas_padrao = {str(h).zfill(2): 0 for h in range(24)}
    return {
        "data_referencia": datetime.now().strftime("%d/%m/%Y"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kpis": {
            "previsto": 28511,
            "bipado": 0,
            "total_bipes": 0,
            "total_coleta": 0,
            "total_lote": 0,
            "total_expedicao": 0,
            "total_operadores": 0,
            "top_operador": "-",
            "top_bipes": 0,
            "media_bipes_op": 0.0,
            "pico_hora": "--:--",
            "pico_volume": 0
        },
        "hora_a_hora": {
            "total": horas_padrao.copy(),
            "coleta": horas_padrao.copy(),
            "lote": horas_padrao.copy(),
            "expedicao": horas_padrao.copy()
        },
        "operadores": []
    }

def clear_ranking_data():
    empty = _get_empty_ranking_template()
    try:
        with open(RANKING_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(empty, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return True

def save_ranking_data(data_dict, replace_all=False):
    try:
        with open(RANKING_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

def get_ranking_data(data_ref=None, check_expiration=True):
    empty_result = _get_empty_ranking_template()
    if not os.path.exists(RANKING_JSON_PATH):
        return empty_result
    try:
        with open(RANKING_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return empty_result

# ==========================================================
# OPERAÇÕES DO CONTROLE DE PDAs (RECEBIMENTOS & DEVOLUÇÕES)
# ==========================================================

DEFAULT_PDA_AUTORIZADOS = {
    '08h': ['GIOVANNY', 'JOAO MENDES'],
    '14h': ['Vinicius Nascimento'],
    '15h': ['PABLO', 'CRISTIANE', 'ISABELLA', 'ANGELA', 'BRUNO', 'GABRIEL', 'JULIANA', 'VITORIA', 'LUAN'],
    '23h': ['Matheus Nascimento', 'Kaue pedrozo', 'Amanda', 'Gabriel oliveira', 'William', 'João Pedro']
}

DEFAULT_PDA_SETORES = [
    'Expedição',
    'Lacre',
    'Esteira',
    'Recebimento',
    'Avaria',
    'Erro de triagem',
    'Criação de lote',
    'DROP'
]

def format_pda_report_inicio_text(d):
    data_val = d.get("data_ref", "*06/10/2026*")
    if not str(data_val).startswith("*") and not str(data_val).endswith("*"):
        data_val = f"*{data_val}*"
        
    return f"""📊 REPORT DE INÍCIO DA OPERAÇÃO

📅 Data: {data_val}

📦 Backlog Inicial: {d.get('backlog_inicial', '692')}
📦 Volume previsto: {d.get('volume_previsto', '53.903')}
🕒 Início da operação: {d.get('inicio_operacao', '15:11')}
👥 CLTs presentes 18hrs: {d.get('clts_presentes_18h', '32')}
❌ Absenteísmo 18hrs: {d.get('absenteismo_18h', '4')}
👥 CLTs presentes 22hrs: {d.get('clts_presentes_22h', d.get('clts_presentes_23h', '—'))}
❌ Absenteísmo 22hrs: {d.get('absenteismo_22h', d.get('absenteismo_23h', '—'))}
👷 Diaristas solicitados (15:00): {d.get('diaristas_solicitados_15h', '20')}
✅ Diaristas presentes (15:00): {d.get('diaristas_presentes_15h', '18')}
👷 Diaristas solicitados (20:00): {d.get('diaristas_solicitados_20h', '10')}
✅ Diaristas presentes (20:00): {d.get('diaristas_presentes_20h', '—')}
📈 PHD previsto: {d.get('phd_previsto', '—')}


📝 Observações:

- Falta injustificada: {d.get('obs_falta_injustificada', '4 — GIOVANA TEIXEIRA GRILLO DAMADA DAVI CARLOS DE PINHO BATISTA LISLAINE CHAVES DE CAMILO LUIS CARLOS PERES DA SILVA')}

- Atestados: {d.get('obs_atestados', 'N/A')}

- Pontos de atenção: {d.get('obs_pontos_atencao', 'N/A')}"""

def get_default_pda_report_inicio():
    d = {
        "data_ref": "*06/10/2026*",
        "backlog_inicial": "692",
        "volume_previsto": "53.903",
        "inicio_operacao": "15:11",
        "clts_presentes_18h": "32",
        "absenteismo_18h": "4",
        "clts_presentes_22h": "—",
        "absenteismo_22h": "—",
        "clts_presentes_23h": "—",
        "absenteismo_23h": "—",
        "diaristas_solicitados_15h": "20",
        "diaristas_presentes_15h": "18",
        "diaristas_solicitados_20h": "10",
        "diaristas_presentes_20h": "—",
        "phd_previsto": "—",
        "obs_falta_injustificada": "4 — GIOVANA TEIXEIRA GRILLO DAMADA DAVI CARLOS DE PINHO BATISTA LISLAINE CHAVES DE CAMILO LUIS CARLOS PERES DA SILVA",
        "obs_atestados": "N/A",
        "obs_pontos_atencao": "N/A"
    }
    d["raw_text"] = format_pda_report_inicio_text(d)
    return d

def normalize_pda_codigo(cod):
    """Padroniza o código do PDA estritamente no formato PDA-XX (PDA-00 a PDA-50)."""
    if not cod:
        return ""
    s = str(cod).strip().upper()
    digits = "".join(filter(str.isdigit, s))
    if digits != "":
        val = int(digits)
        return f"PDA-{val:02d}"
    return s

def normalize_pda_status(st):
    """Normaliza o status do PDA para garantir compatibilidade."""
    if not st:
        return "Disponível"
    s = str(st).strip().lower()
    if "devol" in s or "entreg" in s:
        return "Devolvido"
    if "uso" in s or "ocupad" in s or "retirad" in s:
        return "Em Uso"
    if "avar" in s or "manut" in s or "quebr" in s or "defe" in s:
        return "Avaria"
    if "disp" in s or "livr" in s or "ok" in s:
        return "Disponível"
    return "Disponível"

def _backup_pda_json(data):
    """Grava cópias de backup do estado em JSON para máxima resiliência."""
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        paths = [
            os.path.join(base_dir, "pda_data.json"),
            os.path.join(base_dir, "controle-pda", "pda_data.json")
        ]
        payload = {
            "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            **data
        }
        for p in paths:
            try:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
    except Exception:
        pass

def get_pda_report_inicio():
    """Retorna o último Report de Início da Operação gravado no banco."""
    init_db()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tb_pda_report_inicio ORDER BY id DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            rep = {
                "id": row["id"],
                "data_ref": row["data_ref"],
                "backlog_inicial": row["backlog_inicial"],
                "volume_previsto": row["volume_previsto"],
                "inicio_operacao": row["inicio_operacao"],
                "clts_presentes_18h": row["clts_presentes_18h"],
                "absenteismo_18h": row["absenteismo_18h"],
                "clts_presentes_23h": row["clts_presentes_23h"],
                "absenteismo_23h": row["absenteismo_23h"],
                "diaristas_solicitados_15h": row["diaristas_solicitados_15h"],
                "diaristas_presentes_15h": row["diaristas_presentes_15h"],
                "diaristas_solicitados_20h": row["diaristas_solicitados_20h"],
                "diaristas_presentes_20h": row["diaristas_presentes_20h"],
                "phd_previsto": row["phd_previsto"],
                "obs_falta_injustificada": row["obs_falta_injustificada"],
                "obs_atestados": row["obs_atestados"],
                "obs_pontos_atencao": row["obs_pontos_atencao"],
                "raw_text": row["raw_text"] or format_pda_report_inicio_text(row)
            }
            return rep
        else:
            return get_default_pda_report_inicio()

def save_pda_report_inicio(report_dict):    invalidate_pda_cache()
    """Salva ou atualiza os dados do Report de Início da Operação no banco."""
    if not report_dict:
        report_dict = get_default_pda_report_inicio()
    init_db()
    data_ref = report_dict.get("data_ref", "*06/10/2026*")
    backlog_ini = str(report_dict.get("backlog_inicial", "692"))
    vol_prev = str(report_dict.get("volume_previsto", "53.903"))
    ini_op = str(report_dict.get("inicio_operacao", "15:11"))
    clt_18 = str(report_dict.get("clts_presentes_18h", "32"))
    abs_18 = str(report_dict.get("absenteismo_18h", "4"))
    clt_23 = str(report_dict.get("clts_presentes_22h", report_dict.get("clts_presentes_23h", "—")))
    abs_23 = str(report_dict.get("absenteismo_22h", report_dict.get("absenteismo_23h", "—")))
    dia_sol_15 = str(report_dict.get("diaristas_solicitados_15h", "20"))
    dia_pres_15 = str(report_dict.get("diaristas_presentes_15h", "18"))
    dia_sol_20 = str(report_dict.get("diaristas_solicitados_20h", "10"))
    dia_pres_20 = str(report_dict.get("diaristas_presentes_20h", "—"))
    phd = str(report_dict.get("phd_previsto", "—"))
    obs_falta = str(report_dict.get("obs_falta_injustificada", "4 — GIOVANA TEIXEIRA GRILLO DAMADA DAVI CARLOS DE PINHO BATISTA LISLAINE CHAVES DE CAMILO LUIS CARLOS PERES DA SILVA"))
    obs_atest = str(report_dict.get("obs_atestados", "N/A"))
    obs_aten = str(report_dict.get("obs_pontos_atencao", "N/A"))
    raw_text = report_dict.get("raw_text") or format_pda_report_inicio_text(report_dict)

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tb_pda_report_inicio ORDER BY id DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            rep_id = row["id"]
            cursor.execute("""
                UPDATE tb_pda_report_inicio SET
                    data_ref = ?, backlog_inicial = ?, volume_previsto = ?, inicio_operacao = ?,
                    clts_presentes_18h = ?, absenteismo_18h = ?, clts_presentes_23h = ?, absenteismo_23h = ?,
                    diaristas_solicitados_15h = ?, diaristas_presentes_15h = ?,
                    diaristas_solicitados_20h = ?, diaristas_presentes_20h = ?,
                    phd_previsto = ?, obs_falta_injustificada = ?, obs_atestados = ?, obs_pontos_atencao = ?,
                    raw_text = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?;
            """, (data_ref, backlog_ini, vol_prev, ini_op, clt_18, abs_18, clt_23, abs_23,
                  dia_sol_15, dia_pres_15, dia_sol_20, dia_pres_20, phd, obs_falta, obs_atest, obs_aten, raw_text, rep_id))
        else:
            cursor.execute("""
                INSERT INTO tb_pda_report_inicio (
                    data_ref, backlog_inicial, volume_previsto, inicio_operacao,
                    clts_presentes_18h, absenteismo_18h, clts_presentes_23h, absenteismo_23h,
                    diaristas_solicitados_15h, diaristas_presentes_15h,
                    diaristas_solicitados_20h, diaristas_presentes_20h,
                    phd_previsto, obs_falta_injustificada, obs_atestados, obs_pontos_atencao,
                    raw_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (data_ref, backlog_ini, vol_prev, ini_op, clt_18, abs_18, clt_23, abs_23,
                  dia_sol_15, dia_pres_15, dia_sol_20, dia_pres_20, phd, obs_falta, obs_atest, obs_aten, raw_text))
        conn.commit()

    return get_pda_report_inicio()

_pda_cache_data = None
_pda_cache_time = 0

def invalidate_pda_cache():
    global _pda_cache_data, _pda_cache_time
    _pda_cache_data = None
    _pda_cache_time = 0

def get_pda_data():
    """Retorna todo o estado do Controle de PDAs persistido no banco de dados."""
    global _pda_cache_data, _pda_cache_time
    now = time.time()
    if _pda_cache_data is not None and (now - _pda_cache_time < 3.0):
        return _pda_cache_data
    init_db()
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. Frota
        cursor.execute("SELECT codigo, status, obs FROM tb_pda_frota;")
        frota_rows = cursor.fetchall()
        
        if len(frota_rows) < 51:
            seed_pda_initial_data()
            cursor.execute("SELECT codigo, status, obs FROM tb_pda_frota;")
            frota_rows = cursor.fetchall()

        frota_map = {}
        for r in frota_rows:
            cod = normalize_pda_codigo(r["codigo"])
            if cod:
                st = normalize_pda_status(r["status"])
                if cod not in frota_map or st in ("Em Uso", "Avaria"):
                    frota_map[cod] = {
                        "codigo": cod,
                        "status": st,
                        "obs": r["obs"] or "OK"
                    }

        # Garante exatamente de PDA-00 a PDA-50
        for i in range(0, 51):
            cod = f"PDA-{i:02d}"
            if cod not in frota_map:
                frota_map[cod] = {
                    "codigo": cod,
                    "status": "Disponível",
                    "obs": "OK"
                }

        frota = list(frota_map.values())
        frota.sort(key=lambda p: int("".join(filter(str.isdigit, p["codigo"])) or 0))
            
        # 2. Autorizados
        cursor.execute("SELECT turno, nome FROM tb_pda_autorizados ORDER BY turno, id;")
        aut_rows = cursor.fetchall()
        autorizados = {"08h": [], "14h": [], "15h": [], "23h": []}
        for r in aut_rows:
            t = r["turno"]
            if t not in autorizados:
                autorizados[t] = []
            if r["nome"] not in autorizados[t]:
                autorizados[t].append(r["nome"])
                
        # 3. Registros de Retirada, Recebimento e Devolução
        cursor.execute("""
            SELECT id, pda, turno, responsavel, setor, data_retirada, data_devolucao, status, obs_retirada, obs_devolucao, estado_devolucao
            FROM tb_pda_registros
            ORDER BY data_retirada DESC;
        """)
        reg_rows = cursor.fetchall()
        registros = []
        for r in reg_rows:
            registros.append({
                "id": r["id"],
                "pda": normalize_pda_codigo(r["pda"]),
                "turno": r["turno"],
                "responsavel": r["responsavel"],
                "setor": r["setor"],
                "dataRetirada": r["data_retirada"],
                "dataDevolucao": r["data_devolucao"],
                "status": normalize_pda_status(r["status"]),
                "obsRetirada": r["obs_retirada"],
                "obsDevolucao": r["obs_devolucao"],
                "estadoDevolucao": r["estado_devolucao"]
            })
            
        # 4. Report de Início
        cursor.execute("SELECT * FROM tb_pda_report_inicio ORDER BY id DESC LIMIT 1;")
        rep_row = cursor.fetchone()
        report_inicio = None
        if rep_row:
            report_inicio = {
                "id": rep_row["id"],
                "data_ref": rep_row["data_ref"],
                "backlog_inicial": rep_row["backlog_inicial"],
                "volume_previsto": rep_row["volume_previsto"],
                "inicio_operacao": rep_row["inicio_operacao"],
                "clts_presentes_18h": rep_row["clts_presentes_18h"],
                "absenteismo_18h": rep_row["absenteismo_18h"],
                "clts_presentes_23h": rep_row["clts_presentes_23h"],
                "absenteismo_23h": rep_row["absenteismo_23h"],
                "diaristas_solicitados_15h": rep_row["diaristas_solicitados_15h"],
                "diaristas_presentes_15h": rep_row["diaristas_presentes_15h"],
                "diaristas_solicitados_20h": rep_row["diaristas_solicitados_20h"],
                "diaristas_presentes_20h": rep_row["diaristas_presentes_20h"],
                "phd_previsto": rep_row["phd_previsto"],
                "obs_falta_injustificada": rep_row["obs_falta_injustificada"],
                "obs_atestados": rep_row["obs_atestados"],
                "obs_pontos_atencao": rep_row["obs_pontos_atencao"],
                "raw_text": rep_row["raw_text"] or format_pda_report_inicio_text(rep_row)
            }
        else:
            report_inicio = get_default_pda_report_inicio()

        return {
            "frota": frota,
            "autorizados": autorizados,
            "registros": registros,
            "report_inicio": report_inicio
        }

def save_pda_data(payload):    invalidate_pda_cache()
    """Salva estado completo ou sincronização no banco de dados garantindo que registros nunca sejam perdidos."""
    if not payload:
        return get_pda_data()
    init_db()
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Frota
        if "frota" in payload and isinstance(payload["frota"], list):
            for item in payload["frota"]:
                cod = normalize_pda_codigo(item.get("codigo", ""))
                st = normalize_pda_status(item.get("status", "Disponível"))
                obs = item.get("obs", "OK")
                if cod:
                    cursor.execute("""
                        INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(codigo) DO UPDATE SET
                            status = EXCLUDED.status,
                            obs = EXCLUDED.obs,
                            updated_at = CURRENT_TIMESTAMP;
                    """, (cod, st, obs))
                    
        # Autorizados
        if "autorizados" in payload and isinstance(payload["autorizados"], dict):
            cursor.execute("DELETE FROM tb_pda_autorizados;")
            for turno, nomes in payload["autorizados"].items():
                if isinstance(nomes, list):
                    for nome in nomes:
                        n = str(nome).strip()
                        if n:
                            try:
                                cursor.execute("INSERT INTO tb_pda_autorizados (turno, nome) VALUES (?, ?);", (str(turno).strip(), n))
                            except Exception:
                                pass
                                
        # Registros de Retirada, Recebimento e Devolução
        if "registros" in payload and isinstance(payload["registros"], list):
            for reg in payload["registros"]:
                reg_id = str(reg.get("id", "")).strip()
                pda = normalize_pda_codigo(reg.get("pda", ""))
                if not reg_id or not pda:
                    continue
                turno = reg.get("turno", "15h")
                resp = reg.get("responsavel", "")
                setor = reg.get("setor", "Expedição")
                data_ret = reg.get("dataRetirada") or datetime.now().isoformat()
                data_dev = reg.get("dataDevolucao")
                st = normalize_pda_status(reg.get("status", "Em Uso"))
                obs_ret = reg.get("obsRetirada")
                obs_dev = reg.get("obsDevolucao")
                est_dev = reg.get("estadoDevolucao")
                
                cursor.execute("""
                    INSERT INTO tb_pda_registros (id, pda, turno, responsavel, setor, data_retirada, data_devolucao, status, obs_retirada, obs_devolucao, estado_devolucao, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        pda = EXCLUDED.pda,
                        turno = EXCLUDED.turno,
                        responsavel = EXCLUDED.responsavel,
                        setor = EXCLUDED.setor,
                        data_retirada = EXCLUDED.data_retirada,
                        data_devolucao = EXCLUDED.data_devolucao,
                        status = EXCLUDED.status,
                        obs_retirada = EXCLUDED.obs_retirada,
                        obs_devolucao = EXCLUDED.obs_devolucao,
                        estado_devolucao = EXCLUDED.estado_devolucao,
                        updated_at = CURRENT_TIMESTAMP;
                """, (reg_id, pda, turno, resp, setor, data_ret, data_dev, st, obs_ret, obs_dev, est_dev))

        conn.commit()

    if "report_inicio" in payload and isinstance(payload["report_inicio"], dict):
        save_pda_report_inicio(payload["report_inicio"])

    data = get_pda_data()
    _backup_pda_json(data)
    return data

def save_pda_registro(reg_dict):    invalidate_pda_cache()
    """Salva/atualiza uma retirada, recebimento ou devolução no banco e sincroniza a frota."""
    if not reg_dict:
        return None
    init_db()
    reg_id = str(reg_dict.get("id") or f"REG-{int(datetime.now().timestamp() * 1000)}").strip()
    pda = normalize_pda_codigo(reg_dict.get("pda", ""))
    turno = reg_dict.get("turno", "15h")
    resp = reg_dict.get("responsavel", "")
    setor = reg_dict.get("setor", "Expedição")
    data_ret = reg_dict.get("dataRetirada") or datetime.now().isoformat()
    data_dev = reg_dict.get("dataDevolucao")
    st = normalize_pda_status(reg_dict.get("status", "Em Uso"))
    obs_ret = reg_dict.get("obsRetirada")
    obs_dev = reg_dict.get("obsDevolucao")
    est_dev = reg_dict.get("estadoDevolucao")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tb_pda_registros (id, pda, turno, responsavel, setor, data_retirada, data_devolucao, status, obs_retirada, obs_devolucao, estado_devolucao, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                pda = EXCLUDED.pda,
                turno = EXCLUDED.turno,
                responsavel = EXCLUDED.responsavel,
                setor = EXCLUDED.setor,
                data_retirada = EXCLUDED.data_retirada,
                data_devolucao = EXCLUDED.data_devolucao,
                status = EXCLUDED.status,
                obs_retirada = EXCLUDED.obs_retirada,
                obs_devolucao = EXCLUDED.obs_devolucao,
                estado_devolucao = EXCLUDED.estado_devolucao,
                updated_at = CURRENT_TIMESTAMP;
        """, (reg_id, pda, turno, resp, setor, data_ret, data_dev, st, obs_ret, obs_dev, est_dev))

        if st == 'Em Uso':
            cursor.execute("""
                INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
                VALUES (?, 'Em Uso', 'Em Uso', CURRENT_TIMESTAMP)
                ON CONFLICT(codigo) DO UPDATE SET status = 'Em Uso', updated_at = CURRENT_TIMESTAMP;
            """, (pda,))
        elif st == 'Avaria':
            cursor.execute("""
                INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
                VALUES (?, 'Avaria', ?, CURRENT_TIMESTAMP)
                ON CONFLICT(codigo) DO UPDATE SET status = 'Avaria', obs = EXCLUDED.obs, updated_at = CURRENT_TIMESTAMP;
            """, (pda, obs_dev or "Danificado na devolução"))
        elif st == 'Devolvido' or st == 'Disponível':
            cursor.execute("SELECT COUNT(*) as cnt FROM tb_pda_registros WHERE pda = ? AND status = 'Em Uso' AND id != ?;", (pda, reg_id))
            row = cursor.fetchone()
            cnt = row["cnt"] if row else 0
            if cnt == 0:
                cursor.execute("""
                    INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
                    VALUES (?, 'Disponível', 'OK', CURRENT_TIMESTAMP)
                    ON CONFLICT(codigo) DO UPDATE SET status = 'Disponível', obs = 'OK', updated_at = CURRENT_TIMESTAMP;
                """, (pda,))
        conn.commit()

    data = get_pda_data()
    _backup_pda_json(data)
    return reg_dict

def delete_pda_registro(reg_id):
    """Exclui um registro do histórico e atualiza a frota se necessário."""
    init_db()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT pda, status FROM tb_pda_registros WHERE id = ?;", (reg_id,))
        row = cursor.fetchone()
        pda = normalize_pda_codigo(row["pda"]) if row else None
        
        cursor.execute("DELETE FROM tb_pda_registros WHERE id = ?;", (reg_id,))
        
        if pda:
            cursor.execute("SELECT COUNT(*) as cnt FROM tb_pda_registros WHERE pda = ? AND status = 'Em Uso';", (pda,))
            c_row = cursor.fetchone()
            cnt = c_row["cnt"] if c_row else 0
            if cnt == 0:
                cursor.execute("UPDATE tb_pda_frota SET status = 'Disponível', obs = 'OK', updated_at = CURRENT_TIMESTAMP WHERE codigo = ? AND status = 'Em Uso';", (pda,))
        conn.commit()

    data = get_pda_data()
    _backup_pda_json(data)
    return True

def save_pda_frota_item(codigo,    invalidate_pda_cache() status="Disponível", obs="OK"):
    """Cadastra ou altera o status de um PDA específico."""
    init_db()
    cod = normalize_pda_codigo(codigo)
    st = normalize_pda_status(status)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(codigo) DO UPDATE SET status = EXCLUDED.status, obs = EXCLUDED.obs, updated_at = CURRENT_TIMESTAMP;
        """, (cod, st, obs))
        conn.commit()
    data = get_pda_data()
    _backup_pda_json(data)
    return True

def seed_pda_initial_data():
    """Inicializa e padroniza automaticamente os 51 PDAs (PDA-00 a PDA-50), autorizados e migra pda_data.json se vazio."""
    init_db()
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Garante todos os 51 PDAs de PDA-00 a PDA-50
        for i in range(0, 51):
            cod = f"PDA-{i:02d}"
            cursor.execute("""
                INSERT INTO tb_pda_frota (codigo, status, obs, updated_at)
                VALUES (?, 'Disponível', 'OK', CURRENT_TIMESTAMP)
                ON CONFLICT(codigo) DO NOTHING;
            """, (cod,))
            
        # Autorizados padrão
        cursor.execute("SELECT COUNT(*) as count FROM tb_pda_autorizados;")
        aut_count = cursor.fetchone()["count"]
        if aut_count == 0:
            for t, nomes in DEFAULT_PDA_AUTORIZADOS.items():
                for n in nomes:
                    try:
                        cursor.execute("INSERT INTO tb_pda_autorizados (turno, nome) VALUES (?, ?);", (t, n))
                    except Exception:
                        pass
                        
        # Migração automática: se tb_pda_registros estiver vazia, importa do pda_data.json
        cursor.execute("SELECT COUNT(*) as count FROM tb_pda_registros;")
        reg_count = cursor.fetchone()["count"]
        if reg_count == 0:
            json_candidates = [
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "pda_data.json"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "controle-pda", "pda_data.json")
            ]
            for j_path in json_candidates:
                if os.path.exists(j_path):
                    try:
                        with open(j_path, "r", encoding="utf-8") as f:
                            cached = json.load(f)
                        if cached and "registros" in cached and len(cached["registros"]) > 0:
                            for reg in cached["registros"]:
                                reg_id = str(reg.get("id") or "").strip()
                                pda = normalize_pda_codigo(reg.get("pda", ""))
                                if reg_id and pda:
                                    cursor.execute("""
                                        INSERT INTO tb_pda_registros (id, pda, turno, responsavel, setor, data_retirada, data_devolucao, status, obs_retirada, obs_devolucao, estado_devolucao, updated_at)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                        ON CONFLICT(id) DO NOTHING;
                                    """, (
                                        reg_id, pda, reg.get("turno", "15h"), reg.get("responsavel", ""),
                                        reg.get("setor", "Expedição"), reg.get("dataRetirada") or datetime.now().isoformat(),
                                        reg.get("dataDevolucao"), normalize_pda_status(reg.get("status", "Em Uso")),
                                        reg.get("obsRetirada"), reg.get("obsDevolucao"), reg.get("estadoDevolucao")
                                    ))
                            print("--> [DB] Registros existentes migrados com sucesso para o banco de dados.")
                            break
                    except Exception as e:
                        print(f"--> [DB] Aviso ao migrar pda_data.json inicial: {e}")
        conn.commit()
    
    rep = get_pda_report_inicio()
    if not rep:
        save_pda_report_inicio(get_default_pda_report_inicio())
    
    data = get_pda_data()
    _backup_pda_json(data)
    return True

def log_upload(modulo, nome_arquivo, total_registros=0, status="SUCESSO"):
    """Mantido para compatibilidade sem travar a aplicação."""
    pass

def seed_initial_data():
    """Migra automaticamente os dados iniciais para o banco se as tabelas estiverem vazias."""
    init_db()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. Seed Volumetria Diária
        cursor.execute("SELECT COUNT(*) as count FROM tb_volumetria_viagens;")
        if cursor.fetchone()["count"] == 0:
            json_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "volumetria_data.json")
            if os.path.exists(json_file):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    save_volumetria_data(data)
                    print("--> [DB] Volumetria Diária migrada com sucesso para o banco.")
                except Exception as e:
                    print(f"--> [DB] Erro ao migrar volumetria_data.json: {e}")

    # 2. Seed Controle de PDAs
    try:
        seed_pda_initial_data()
    except Exception as e:
        print(f"--> [DB] Erro no seed do Controle de PDAs: {e}")

# Executa inicialização automática ao carregar o módulo
try:
    init_db()
    seed_initial_data()
except Exception as e:
    print(f"[DB] Aviso na inicialização do banco: {e}")
