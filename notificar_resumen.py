"""
Script standalone para mandar a CADA comercial un correo individual con
su resumen de pendientes:
  - Por agendar (incluye los del 2025 o antes como urgentes)
  - Canceladas pendientes del mes
  - Candidatos a juzgado

Corre LOCAL (no en Render) porque Render free tier bloquea SMTP saliente.

Ejecucion manual:
    python notificar_resumen.py

Para auto-ejecutarlo:
    crea notificar_resumen.bat con:
        @echo off
        cd /d "C:\\Users\\sofianoguera_habi\\agenda-visitas"
        set GOOGLE_APPLICATION_CREDENTIALS=%APPDATA%\\gcloud\\application_default_credentials.json
        python notificar_resumen.py >> notificar_resumen.log 2>&1
    y agregalo a Task Scheduler de Windows con la frecuencia que quieras.

Flags:
    --dry-run   no envia, solo imprime el resumen
    --max N     envia solo a los primeros N comerciales (para probar)
"""
import os
import sys
import smtplib
import subprocess
import json
import csv
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Misma config que notificar.py
SMTP_USER = os.environ.get("SMTP_USER", "sofianoguera@habi.co")
SMTP_PASS = os.environ.get("SMTP_PASS", "nort eggi kzbc iotb")
PAGE_URL = "https://agenda-visitas-wcdm.onrender.com"


def bq_query(sql):
    """Corre la query via bq CLI y devuelve list[dict] (parsea CSV)."""
    cmd = ('bq query --project_id=papyrus-data --use_legacy_sql=false '
           '--format=csv --max_rows=10000 --quiet')
    proc = subprocess.run(
        cmd, shell=True,
        input=sql.encode("utf-8"),  # bytes para evitar problemas de codificacion
        capture_output=True, timeout=120,
    )
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(f"bq query fallo (rc={proc.returncode}): {stderr[:800] or stdout[:400]}")
    return list(csv.DictReader(io.StringIO(stdout)))

# Flags simples
DRY_RUN = "--dry-run" in sys.argv
MAX_N = None
if "--max" in sys.argv:
    try:
        MAX_N = int(sys.argv[sys.argv.index("--max") + 1])
    except (ValueError, IndexError):
        pass


def resumen_por_comercial():
    """Devuelve dict email_comercial -> contadores de pendientes."""
    primer_dia_mes = datetime.now().strftime("%Y-%m-01")

    q_por_agendar = """
    WITH bubble_unica AS (
      SELECT * FROM (
        SELECT b.nid, b.status, b.modified_date,
          ROW_NUMBER() OVER (PARTITION BY b.nid ORDER BY b.modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co` b
        WHERE b.visit_category = 'Habi Inmobiliaria'
      ) WHERE rn = 1
    ),
    tiene_fotos_cliente AS (
      SELECT DISTINCT pc.nid
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
        ON pc.id = pi.property_card_id
      WHERE pi.source_image_id = 3
    ),
    nids_con_finalizado AS (
      SELECT DISTINCT nid
      FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
      WHERE visit_category = 'Habi Inmobiliaria' AND status = 'Finalizado'
    ),
    gravamen_sellers AS (
      SELECT nid, ANY_VALUE(gravamenes_del_apartamento) AS gravamen
      FROM `papyrus-data.habi_wh_inmobiliaria.habiinmobiliaria_sellers_gestion`
      GROUP BY nid
    )
    SELECT
      LOWER(TRIM(COALESCE(NULLIF(h.hubspot_owner_id, ''),
                          NULLIF(cd.c_comercial_captacion, '')))) AS comercial,
      ANY_VALUE(COALESCE(NULLIF(h.equipo_sellers, ''),
                         NULLIF(cd.c_equipo_seller, ''), '')) AS equipo,
      COUNT(*) AS total,
      COUNTIF(EXTRACT(YEAR FROM cd.c_fecha_captacion) <= 2025) AS de_2025_o_antes
    FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
    LEFT JOIN bubble_unica b ON CAST(cd.nid AS STRING) = b.nid
    LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    LEFT JOIN tiene_fotos_cliente fc ON cd.nid = fc.nid
    LEFT JOIN gravamen_sellers gs ON cd.nid = gs.nid
    WHERE cd.c_fecha_captacion IS NOT NULL
      AND cd.fecha_desistio_inmobiliaria IS NULL
      AND h.fecha_desistio_inmobiliaria IS NULL
      AND cd.v_fecha_venta IS NULL
      AND (h.dealstage IS NULL OR h.dealstage != '1182117639')
      AND (cd.date_publication IS NULL
           OR (fc.nid IS NOT NULL AND DATE(cd.date_publication) >= DATE '2026-04-13'))
      AND CAST(cd.nid AS STRING) NOT IN (SELECT nid FROM nids_con_finalizado)
      AND (b.nid IS NULL OR b.status NOT IN ('Agendado', 'Cerrado'))
      AND (
        d.estado_patrimonio IN ('Sin patrimonio', 'Patrimonio levantado')
        OR (d.estado_patrimonio IS NULL
            AND (gs.gravamen IS NULL
                 OR gs.gravamen NOT IN ('Hipoteca + Patrimonio con hijos', 'Patrimonio hijos')))
      )
      AND LOWER(COALESCE(cd.ciudad, '')) NOT LIKE '%jamundi%'
      AND LOWER(COALESCE(cd.ciudad, '')) NOT LIKE '%jamundí%'
    GROUP BY comercial
    HAVING comercial IS NOT NULL AND comercial != ''
    """

    q_canceladas = f"""
    WITH ultimo_registro AS (
      SELECT nid, status, modified_date,
        ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
      FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
      WHERE nid != 'nan' AND nid IS NOT NULL
        AND visit_type = 'Habi Inmobiliaria'
    )
    SELECT
      LOWER(TRIM(COALESCE(NULLIF(h.hubspot_owner_id, ''),
                          NULLIF(c.c_comercial_captacion, '')))) AS comercial,
      COUNT(*) AS total
    FROM ultimo_registro v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
      ON v.nid = CAST(c.nid AS STRING)
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(v.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    WHERE v.rn = 1
      AND v.status IN ('Cancelado', 'No realizada', 'Cerrado')
      AND v.modified_date >= '{primer_dia_mes}'
      AND c.fecha_desistio_inmobiliaria IS NULL
      AND c.v_fecha_venta IS NULL
    GROUP BY comercial
    HAVING comercial IS NOT NULL AND comercial != ''
    """

    q_juzgado = """
    WITH ya_gestionados AS (
      -- NIDs cuya ULTIMA fase en pipefy esta activa (no Onhold/cerrado).
      SELECT nid FROM (
        SELECT
          CAST(nid AS STRING) AS nid,
          phase_name AS fase,
          ROW_NUMBER() OVER (PARTITION BY nid ORDER BY first_time_in_phase DESC) AS rn
        FROM `papyrus-master.pipefy_streamhabi_tramite.pipefy_history_global`
        WHERE pipe_id IN ('306710579', '306725945')
      )
      WHERE rn = 1
        AND fase NOT IN ('Onhold', 'Asignación', 'Desistido', 'Desistidos',
                         'Finalizadas', 'Finalizado Alianza (Silencio Adm)')
    ),
    desfavorables AS (
      SELECT * FROM (
        SELECT
          CAST(ct.nid AS STRING) AS nid,
          ROW_NUMBER() OVER (
            PARTITION BY ct.nid
            ORDER BY ct.v_fecha_concepto_del_defensor_de_familia DESC
          ) AS rn
        FROM `papyrus-delivery-data.operaciones_global.control_tower_saneamiento_co_bi` ct
        WHERE LOWER(ct.v_concepto_del_defensor_de_familia) = 'no favorable'
          AND DATE(ct.v_fecha_concepto_del_defensor_de_familia)
              >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      ) WHERE rn = 1
    )
    SELECT
      LOWER(TRIM(COALESCE(NULLIF(h.hubspot_owner_id, ''),
                          NULLIF(cd.c_comercial_captacion, '')))) AS comercial,
      COUNT(*) AS total
    FROM desfavorables d
    LEFT JOIN ya_gestionados yg ON yg.nid = d.nid
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
      ON CAST(cd.nid AS STRING) = d.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(d.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    WHERE yg.nid IS NULL
    GROUP BY comercial
    HAVING comercial IS NOT NULL AND comercial != ''
    """

    def _empty():
        return {"por_agendar": 0, "por_agendar_2025": 0,
                "canceladas": 0, "juzgado": 0, "equipo": ""}

    resumen = {}
    for row in bq_query(q_por_agendar):
        c = (row.get("comercial") or "").strip()
        if not c or "@" not in c:
            continue
        resumen.setdefault(c, _empty())
        resumen[c]["por_agendar"] = int(row.get("total") or 0)
        resumen[c]["por_agendar_2025"] = int(row.get("de_2025_o_antes") or 0)
        resumen[c]["equipo"] = row.get("equipo") or ""
    for row in bq_query(q_canceladas):
        c = (row.get("comercial") or "").strip()
        if not c or "@" not in c:
            continue
        resumen.setdefault(c, _empty())
        resumen[c]["canceladas"] = int(row.get("total") or 0)
    for row in bq_query(q_juzgado):
        c = (row.get("comercial") or "").strip()
        if not c or "@" not in c:
            continue
        resumen.setdefault(c, _empty())
        resumen[c]["juzgado"] = int(row.get("total") or 0)
    return resumen


def construir_correo(email_comercial, datos):
    total = datos["por_agendar"] + datos["canceladas"] + datos["juzgado"]
    asunto = f"Resumen Inmobiliaria — tienes {total} pendiente(s) por gestionar"
    cuerpo = "Hola,\n\nTe compartimos tu resumen de pendientes:\n\n"
    if datos["por_agendar"] > 0:
        cuerpo += f"  • Negocios POR AGENDAR: {datos['por_agendar']}\n"
        if datos["por_agendar_2025"] > 0:
            cuerpo += (f"      ↳ De los cuales {datos['por_agendar_2025']} "
                       f"son del 2025 o antes — URGENTE confirmar si "
                       f"desistir o agendar.\n")
    if datos["canceladas"] > 0:
        cuerpo += f"  • Canceladas POR REAGENDAR (este mes): {datos['canceladas']}\n"
    if datos["juzgado"] > 0:
        cuerpo += f"  • Candidatos para JUZGADO: {datos['juzgado']}\n"
    cuerpo += (f"\nIngresa al portal para verlos en detalle por NID y "
               f"gestionarlos:\n{PAGE_URL}\n\n"
               f"Saludos,\nEquipo Habi Inmobiliaria")
    return asunto, cuerpo


def main():
    print(f"[{datetime.now()}] === RESUMEN POR COMERCIAL ===")
    if DRY_RUN:
        print("MODO DRY-RUN: no se envian correos")
    if MAX_N is not None:
        print(f"MAX N: solo los primeros {MAX_N} comerciales")
    print()

    resumen = resumen_por_comercial()
    print(f"Comerciales con pendientes: {len(resumen)}")

    accionables = []
    for email, datos in sorted(resumen.items()):
        total = datos["por_agendar"] + datos["canceladas"] + datos["juzgado"]
        if total > 0:
            accionables.append((email, datos))
    print(f"Comerciales con al menos 1 pendiente accionable: {len(accionables)}")

    if MAX_N is not None:
        accionables = accionables[:MAX_N]

    if DRY_RUN:
        for email, datos in accionables:
            asunto, cuerpo = construir_correo(email, datos)
            print(f"\n--- {email} ---")
            print(f"Asunto: {asunto}")
            print(cuerpo)
        return

    # Envio real con una sola conexion SMTP reutilizada
    enviados = 0
    errores = 0
    print(f"\nConectando a SMTP gmail.com...")
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        print(f"Conectado. Enviando {len(accionables)} correos...")
        for i, (email, datos) in enumerate(accionables, 1):
            asunto, cuerpo = construir_correo(email, datos)
            try:
                msg = MIMEMultipart()
                msg["From"] = f"Habi Inmobiliaria <{SMTP_USER}>"
                msg["To"] = email
                msg["Subject"] = asunto
                msg.attach(MIMEText(cuerpo, "plain"))
                server.send_message(msg)
                enviados += 1
                print(f"  [{i:>3}/{len(accionables)}] OK    {email} "
                      f"(pa={datos['por_agendar']}, can={datos['canceladas']}, juz={datos['juzgado']})")
            except Exception as e:
                errores += 1
                print(f"  [{i:>3}/{len(accionables)}] ERROR {email}: {e}")
    print(f"\n=== RESUMEN ===")
    print(f"Enviados: {enviados}")
    print(f"Errores:  {errores}")
    print(f"Total accionados: {len(accionables)}")


if __name__ == "__main__":
    main()
