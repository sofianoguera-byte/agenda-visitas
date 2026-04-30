import sys
import os

# Asegurar que la carpeta del proyecto esté en sys.path para que
# `from notificar import ...` funcione bajo gunicorn en Render.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from flask import Flask, render_template, jsonify, request
from google.cloud import bigquery
from google.oauth2 import credentials as oauth2_credentials
from datetime import datetime, timedelta
from urllib.parse import quote
import imaplib
import email as email_lib
import re
import csv
import io
import json
import tempfile
import requests as http_requests
from bs4 import BeautifulSoup

app = Flask(__name__)

# En producción (Render), usa credenciales desde variable de entorno
creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if creds_json:
    creds_path = os.path.join(tempfile.gettempdir(), "gcloud_creds.json")
    with open(creds_path, "w") as f:
        f.write(creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

client = bigquery.Client(project="papyrus-data")

# Credenciales correo (desde env vars - NUNCA hardcodear)
EMAIL_USER = os.environ.get("EMAIL_USER", "")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")


def get_fecha_manana():
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def get_fecha_hoy():
    return datetime.now().strftime("%Y-%m-%d")


def clean(val):
    v = str(val) if val else ""
    return "" if v in ("nan", "None") else v


def utc_to_colombia(iso_str):
    if not iso_str or "T" not in str(iso_str):
        return ""
    try:
        utc_time = datetime.strptime(str(iso_str)[:19], "%Y-%m-%dT%H:%M:%S")
        col_time = utc_time - timedelta(hours=5)
        return col_time.strftime("%I:%M %p")
    except Exception:
        return str(iso_str).split("T")[1][:5]


def get_visitas_manana():
    tomorrow = get_fecha_manana()
    # IMPORTANTE: primero deduplicar por NID quedando con el registro mas reciente,
    # despues filtrar por fecha. Asi, si una visita se reagendo a otro dia, no se
    # queda mostrando el registro viejo (bug detectado con NID 57874705512: estaba
    # reagendado al miercoles pero seguia apareciendo en martes/mañana porque el
    # filtro de fecha se aplicaba ANTES del dedup y descartaba el registro nuevo).
    query = f"""
    WITH ultimo_registro AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE nid != 'nan'
            AND nid IS NOT NULL
            AND visit_type = 'Habi Inmobiliaria'
    ),
    visitas_manana AS (
        SELECT * FROM ultimo_registro
        WHERE rn = 1
            AND status IN ('Agendado', 'Cerrado')
            AND fecha_fin LIKE '{tomorrow}%'
    )
    SELECT
        v.nid, v.fecha_fin, v.fecha_inicio, v.ciudad_muni, v.zona,
        v.direccion, v.torre_apto, v.conjunto, v.visit_type, v.visit_category,
        v.nombre_agendador, v.email_agendador, v.nombre_visitador, v.email_visitador,
        COALESCE(h.hubspot_owner_id, c.c_comercial_captacion) AS c_comercial_captacion,
        c.tel_fono_del_cliente_1 AS telefono_cliente,
        COALESCE(h.equipo_sellers, c.c_equipo_seller) AS equipo
    FROM visitas_manana v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
        ON SAFE_CAST(v.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    ORDER BY v.fecha_inicio
    """
    try:
        results = client.query(query).result()
        visitas = []
        for row in results:
            ciudad = clean(row.ciudad_muni)
            comercial = clean(row.c_comercial_captacion) or clean(row.email_agendador)
            visitas.append({
                "nid": clean(row.nid),
                "fecha_fin": clean(row.fecha_fin),
                "hora": utc_to_colombia(row.fecha_inicio),
                "c_comercial_captacion": comercial,
                "nombre_agendador": clean(row.nombre_agendador),
                "ciudad": ciudad.title() if ciudad else "",
                "zona": clean(row.zona),
                "direccion": clean(row.direccion),
                "torre_apto": clean(row.torre_apto),
                "conjunto": clean(row.conjunto),
                "visit_type": clean(row.visit_type),
                "visit_category": clean(row.visit_category),
                "telefono_cliente": clean(row.telefono_cliente),
                "equipo": clean(row.equipo),
            })
        return visitas
    except Exception as e:
        print(f"Error consultando BigQuery visitas: {e}")
        return []


_MESES_REPORTE = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parse_fecha_reporte_to_date(fecha_str):
    """Convierte '23 de abril' a datetime.date usando el año actual (o año anterior si el mes ya pasó hace tiempo)."""
    if not fecha_str:
        return None
    m = re.match(r"(\d+)\s+de\s+([a-záéíóúñ]+)", fecha_str.lower().strip())
    if not m:
        return None
    dia = int(m.group(1))
    mes_nombre = m.group(2)
    # tolera typos comparando letras ordenadas
    mes_num = _MESES_REPORTE.get(mes_nombre)
    if not mes_num:
        key = "".join(sorted(mes_nombre))
        for nombre, num in _MESES_REPORTE.items():
            if "".join(sorted(nombre)) == key:
                mes_num = num
                break
    if not mes_num:
        return None
    hoy = datetime.now().date()
    anio = hoy.year
    try:
        candidato = datetime(anio, mes_num, dia).date()
    except ValueError:
        return None
    # si el reporte sale como futuro, usar año anterior
    if candidato > hoy + timedelta(days=2):
        candidato = datetime(anio - 1, mes_num, dia).date()
    return candidato


def parsear_correo_completo(msg):
    """Extrae todos los NIDs del correo, separados en completados y cancelados."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                break
    else:
        body = msg.get_payload(decode=True).decode("utf-8", errors="replace")

    if not body:
        return [], [], ""

    fecha_match = re.search(r"reporte del registro fotogr.fico del (\d+ de \w+)", body)
    fecha_reporte = fecha_match.group(1) if fecha_match else ""

    soup = BeautifulSoup(body, "html.parser")
    table = soup.find("table")
    if not table:
        return [], [], fecha_reporte

    rows = table.find_all("tr")
    cancelados = []
    completados = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 4:
            nid = cells[0].get_text(strip=True)
            ciudad = cells[1].get_text(strip=True)
            fotografo = cells[2].get_text(strip=True)
            resultado = cells[3].get_text(strip=True)
            if nid and nid.isdigit():
                entry = {
                    "nid": nid,
                    "ciudad_correo": ciudad,
                    "fotografo": fotografo,
                    "fecha_reporte": fecha_reporte,
                }
                if resultado.upper().strip() == "SI":
                    completados.append(entry)
                else:
                    entry["motivo"] = resultado
                    cancelados.append(entry)

    return cancelados, completados, fecha_reporte


def parsear_correo(msg):
    """Wrapper para compatibilidad - solo devuelve cancelados."""
    cancelados, _, fecha_reporte = parsear_correo_completo(msg)
    return cancelados, fecha_reporte


def leer_completados_correo():
    """Lee correos del mes y extrae los NIDs visitados (SI)."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        desde = datetime.now().replace(day=1).strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'FROM "gerencia@cleaningms.com.co" SINCE {desde}')
        ids = messages[0].split()
        if not ids:
            mail.logout()
            return []

        todos_completados = []
        for msg_id in ids:
            status, data = mail.fetch(msg_id, "(RFC822)")
            msg = email_lib.message_from_bytes(data[0][1])
            _, completados, fecha_reporte = parsear_correo_completo(msg)
            for c in completados:
                c["fecha_reporte"] = fecha_reporte
            todos_completados.extend(completados)

        mail.logout()
        return todos_completados
    except Exception as e:
        print(f"Error leyendo completados: {e}")
        return []


def leer_canceladas_correo(dias=7):
    """Lee correos de gerencia@cleaningms.com.co y extrae canceladas."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        desde = (datetime.now() - timedelta(days=dias)).strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'FROM "gerencia@cleaningms.com.co" SINCE {desde}')
        ids = messages[0].split()
        if not ids:
            mail.logout()
            return [], []

        # Leer todos los correos de la semana
        todas_canceladas = []
        fechas_reporte = []

        for msg_id in ids:
            status, data = mail.fetch(msg_id, "(RFC822)")
            msg = email_lib.message_from_bytes(data[0][1])
            cancelados, fecha_reporte = parsear_correo(msg)
            if fecha_reporte and fecha_reporte not in fechas_reporte:
                fechas_reporte.append(fecha_reporte)
            fecha_reporte_date = _parse_fecha_reporte_to_date(fecha_reporte)
            for c in cancelados:
                c["_fecha_reporte_date"] = fecha_reporte_date
            todas_canceladas.extend(cancelados)

        mail.logout()

        if not todas_canceladas:
            return [], fechas_reporte

        # Por NID: quedarse con la fecha_reporte mas reciente entre todos los correos
        ultima_fecha_reporte_por_nid = {}
        for c in todas_canceladas:
            frd = c.get("_fecha_reporte_date")
            nid = c["nid"]
            if frd and (nid not in ultima_fecha_reporte_por_nid or frd > ultima_fecha_reporte_por_nid[nid]):
                ultima_fecha_reporte_por_nid[nid] = frd

        # Enriquecer con BigQuery
        nids_unicos = list(set(n["nid"] for n in todas_canceladas))
        nids_str = ",".join(f"'{nid}'" for nid in nids_unicos)

        # 1. Determinar si bubble refleja la cancelación:
        #    - Si último estado bubble es Cancelado/No realizada -> incluir
        #    - Si bubble está desactualizado (modified_date anterior a fecha del reporte)
        #      y estado NO es Finalizado -> confiar en el correo, incluir
        #    - Si bubble tiene estado posterior (Finalizado u otro) -> excluir
        query_status = f"""
        WITH ultimo AS (
            SELECT nid, status, modified_date,
                ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
            FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
            WHERE nid IN ({nids_str}) AND visit_type = 'Habi Inmobiliaria'
        )
        SELECT nid, status, modified_date FROM ultimo WHERE rn = 1
        """
        nids_realmente_cancelados = set()
        nids_modified = {}
        nids_bubble_status = {}
        nids_bubble_modified_date = {}
        try:
            for row in client.query(query_status).result():
                nid_s = str(row.nid)
                nids_bubble_status[nid_s] = row.status
                mod = str(row.modified_date) if row.modified_date else ""
                mod_date_obj = None
                if mod and "T" in mod:
                    try:
                        mod_date_obj = datetime.strptime(mod.split("T")[0], "%Y-%m-%d").date()
                    except ValueError:
                        mod_date_obj = None
                    mod = mod.split("T")[0]
                nids_modified[nid_s] = mod
                nids_bubble_modified_date[nid_s] = mod_date_obj

                if row.status in ("Cancelado", "No realizada"):
                    nids_realmente_cancelados.add(nid_s)
                    continue
                # Bubble desactualizado: confiar en el correo si el reporte es
                # posterior a la última modificación de bubble y el estado no
                # indica visita completada (Finalizado).
                fecha_rep = ultima_fecha_reporte_por_nid.get(nid_s)
                if (
                    row.status != "Finalizado"
                    and fecha_rep is not None
                    and mod_date_obj is not None
                    and fecha_rep > mod_date_obj
                ):
                    nids_realmente_cancelados.add(nid_s)
        except Exception as e:
            print(f"Error verificando status: {e}")

        # Filtrar: solo los que realmente están cancelados en bubble
        todas_canceladas = [n for n in todas_canceladas if n["nid"] in nids_realmente_cancelados]
        if not todas_canceladas:
            return [], fechas_reporte

        # Filtrar: excluir NIDs que ya tienen fotos 360 (ya no hay que reagendar)
        nids_pendientes_str = ",".join(f"'{n['nid']}'" for n in todas_canceladas)
        query_360 = f"""
        SELECT DISTINCT CAST(pc.nid AS STRING) AS nid
        FROM `papyrus-data.habi_brokers_listing.property_card` pc
        INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
          ON pc.id = pi.property_card_id
        WHERE pi.source_image_id = 1
          AND CAST(pc.nid AS STRING) IN ({nids_pendientes_str})
        """
        nids_con_360 = set()
        try:
            for row in client.query(query_360).result():
                nids_con_360.add(str(row.nid))
        except Exception as e:
            print(f"Error verificando 360: {e}")

        todas_canceladas = [n for n in todas_canceladas if n["nid"] not in nids_con_360]
        if not todas_canceladas:
            return [], fechas_reporte

        # Recalcular nids después de filtrar
        nids_unicos = list(set(n["nid"] for n in todas_canceladas))
        nids_str = ",".join(f"'{nid}'" for nid in nids_unicos)

        # 2. Traer fecha agendada del último registro en bubble
        query_bubble = f"""
        WITH ultimo AS (
            SELECT nid, fecha_inicio,
                ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
            FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
            WHERE nid IN ({nids_str}) AND visit_type = 'Habi Inmobiliaria'
        )
        SELECT nid, fecha_inicio FROM ultimo WHERE rn = 1
        """
        # 3. Traer comercial + teléfono de consolidado + hubspot
        query_consolidado = f"""
        SELECT
            CAST(c.nid AS STRING) AS nid,
            COALESCE(h.hubspot_owner_id, c.c_comercial_captacion) AS c_comercial_captacion,
            c.tel_fono_del_cliente_1 AS telefono_cliente,
            c.direccion,
            c.ciudad,
            COALESCE(h.equipo_sellers, c.c_equipo_seller) AS equipo
        FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
            ON SAFE_CAST(c.nid AS INT64) = h.nid AND h.pipeline = '803674753'
        WHERE CAST(c.nid AS STRING) IN ({nids_str})
        """
        bq_data = {}
        bubble_fechas = {}
        try:
            for row in client.query(query_bubble).result():
                fecha_ag = str(row.fecha_inicio) if row.fecha_inicio else ""
                fecha_corta = ""
                if fecha_ag and "T" in fecha_ag:
                    fecha_corta = fecha_ag.split("T")[0] + " " + utc_to_colombia(fecha_ag)
                bubble_fechas[str(row.nid)] = fecha_corta
            for row in client.query(query_consolidado).result():
                bq_data[str(row.nid)] = {
                    "c_comercial_captacion": row.c_comercial_captacion or "",
                    "telefono_cliente": str(row.telefono_cliente) if row.telefono_cliente else "",
                    "direccion": row.direccion or "",
                    "ciudad_bq": row.ciudad or "",
                    "equipo": row.equipo or "",
                }
        except Exception as e:
            print(f"Error enriqueciendo con BQ: {e}")

        # Deduplicar por (nid, fecha_reporte): cuando llegan 2 correos con el
        # mismo reporte, no mostrar la cancelacion repetida.
        vistos = set()
        dedupe = []
        for n in todas_canceladas:
            key = (n["nid"], n.get("fecha_reporte", ""))
            if key in vistos:
                continue
            vistos.add(key)
            dedupe.append(n)
        todas_canceladas = dedupe

        # Armar resultado final
        resultado = []
        for n in todas_canceladas:
            bq = bq_data.get(n["nid"], {})
            ciudad = bq.get("ciudad_bq", "") or n["ciudad_correo"]
            fecha_agendada = bubble_fechas.get(n["nid"], "")
            resultado.append({
                "nid": n["nid"],
                "ciudad": ciudad.title() if ciudad else "",
                "fotografo": n["fotografo"],
                "motivo_cancelacion": n["motivo"],
                "c_comercial_captacion": bq.get("c_comercial_captacion", ""),
                "telefono_cliente": bq.get("telefono_cliente", ""),
                "direccion": bq.get("direccion", ""),
                "fecha_reporte": n["fecha_reporte"],
                "fecha_agendada": fecha_agendada,
                "equipo": bq.get("equipo", ""),
                "ultima_actualizacion": nids_modified.get(n["nid"], ""),
            })

        return resultado, fechas_reporte

    except Exception as e:
        print(f"Error leyendo correo: {e}")
        return [], []


@app.route("/")
def index():
    return render_template("index.html", fecha_manana=get_fecha_manana(), fecha_hoy=get_fecha_hoy())


@app.route("/api/visitas")
def api_visitas():
    visitas = get_visitas_manana()
    if request.args.get("test") == "1":
        visitas.insert(0, {
            "nid": "99999999999",
            "fecha_fin": get_fecha_manana() + "T20:00:00.000Z",
            "hora": "03:00 PM",
            "c_comercial_captacion": "sofianoguera@habi.co",
            "nombre_agendador": "Sofia Noguera (PRUEBA)",
            "ciudad": "Bogota",
            "zona": "Norte",
            "direccion": "Calle 100 # 15-20",
            "torre_apto": "Torre 3 Apto 501",
            "conjunto": "Conjunto Prueba",
            "visit_type": "Habi Inmobiliaria",
            "visit_category": "Habi Inmobiliaria",
            "telefono_cliente": "+573219829960",
            "equipo": "Zona Norte",
        })
    return jsonify(visitas)


@app.route("/api/canceladas")
def api_canceladas():
    dias = request.args.get("dias", 30, type=int)  # default 30 dias hacia atras
    canceladas, fechas_reporte = leer_canceladas_correo(dias)
    return jsonify({"canceladas": canceladas, "fechas_reporte": fechas_reporte})


@app.route("/api/juzgado")
def api_juzgado():
    """Inmuebles del pipeline Inmo con concepto DESFAVORABLE del Defensor de Familia.
    Candidatos a la opcion de levantamiento via Juzgado.
    """
    query = """
    WITH ult_etapa AS (
      SELECT CAST(nid AS STRING) AS nid, dealstage, fecha_desistio_inmobiliaria,
        ROW_NUMBER() OVER (PARTITION BY nid ORDER BY hs_lastmodifieddate DESC) AS rn
      FROM `papyrus-master.squad_bi_global.hubspot_deal`
      WHERE pipeline = '803674753' AND nid IS NOT NULL
    ),
    inmo_activo AS (
      SELECT DISTINCT nid FROM ult_etapa
      WHERE rn = 1 AND fecha_desistio_inmobiliaria IS NULL
        AND dealstage NOT IN ('1182117639','closedwon','closedlost')
    ),
    -- control_tower trae el concepto del defensor + nombres y telefono en plano
    desfavorables AS (
      SELECT * FROM (
        SELECT
          CAST(ct.nid AS STRING) AS nid,
          DATE(ct.v_fecha_concepto_del_defensor_de_familia) AS fecha_desfavorable,
          ct.v_concepto_del_defensor_de_familia AS concepto,
          ct.v_nombre_cliente_1 AS nombre_cliente_plano,
          ct.v_numero_telefonico_cliente_1 AS telefono_plano,
          ROW_NUMBER() OVER (
            PARTITION BY ct.nid
            ORDER BY ct.v_fecha_concepto_del_defensor_de_familia DESC
          ) AS rn
        FROM `papyrus-delivery-data.operaciones_global.control_tower_saneamiento_co_bi` ct
        WHERE LOWER(ct.v_concepto_del_defensor_de_familia) = 'no favorable'
      ) WHERE rn = 1
    )
    SELECT
      cd.nid,
      d.fecha_desfavorable,
      DATE(cd.c_fecha_captacion) AS fecha_captacion,
      COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS comercial,
      COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS equipo,
      cd.ciudad,
      -- preferir telefono plano de control_tower; fallback al de consolidado
      COALESCE(d.telefono_plano, cd.tel_fono_del_cliente_1) AS telefono_cliente,
      d.nombre_cliente_plano AS nombre_cliente
    FROM desfavorables d
    JOIN inmo_activo ia ON ia.nid = d.nid
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
      ON CAST(cd.nid AS STRING) = d.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(d.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    WHERE cd.v_fecha_venta IS NULL
      AND cd.fecha_desistio_inmobiliaria IS NULL
    ORDER BY d.fecha_desfavorable DESC
    """
    try:
        results = client.query(query).result()
        out = []
        seen = set()
        for row in results:
            nid = str(row.nid) if row.nid else ""
            if not nid or nid in seen:
                continue
            seen.add(nid)
            out.append({
                "nid": nid,
                "fecha_desfavorable": str(row.fecha_desfavorable) if row.fecha_desfavorable else "",
                "fecha_captacion": str(row.fecha_captacion) if row.fecha_captacion else "",
                "comercial": clean(row.comercial),
                "equipo": clean(row.equipo),
                "ciudad": (row.ciudad or "").title() if row.ciudad else "",
                "telefono_cliente": clean(row.telefono_cliente),
                "nombre_cliente": clean(row.nombre_cliente),
            })
        return jsonify(out)
    except Exception as e:
        print(f"Error consultando juzgado: {e}")
        return jsonify([])


def _send_email(destinatario, asunto, cuerpo, _from_name="Habi Inmobiliaria"):
    """Envia un correo. Usa SendGrid HTTPS si esta configurado (Render bloquea SMTP),
    cae a SMTP si no hay API key (entorno local)."""
    sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_email = os.environ.get("EMAIL_FROM", "sofianoguera@habi.co").strip()

    if sg_key:
        # SendGrid Web API v3 — HTTPS, no usa SMTP
        payload = {
            "personalizations": [{"to": [{"email": destinatario}]}],
            "from": {"email": from_email, "name": _from_name},
            "subject": asunto,
            "content": [{"type": "text/plain", "value": cuerpo}],
        }
        r = http_requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {sg_key}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        if r.status_code >= 300:
            raise RuntimeError(f"SendGrid HTTP {r.status_code}: {r.text[:300]}")
        return

    # Fallback SMTP (entorno local). En Render fallara por bloqueo de red saliente.
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    smtp_user = os.environ.get("SMTP_USER", from_email)
    smtp_pass = os.environ.get("SMTP_PASS", "nort eggi kzbc iotb")
    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.attach(MIMEText(cuerpo, "plain"))
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def _notificar_canceladas_reagendar(forzar=False):
    """Inlined version del cron de notificar.py para que sea autocontenido en
    el deploy. Misma logica: nuevas (ayer/lunes 3 dias) + pendientes del mes,
    1 correo por comercial con cancelaciones nuevas."""
    PAGE_URL = "https://agenda-visitas-wcdm.onrender.com"
    hoy = datetime.now()
    dia_semana = hoy.weekday()
    resumen = {"enviados": 0, "errores": 0, "comerciales": [], "skipped": False, "motivo": ""}
    if dia_semana in (5, 6) and not forzar:
        resumen["skipped"] = True
        resumen["motivo"] = "fin_de_semana"
        return resumen
    dias_atras = 3 if dia_semana == 0 else 1
    fecha_desde_nuevas = (hoy - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
    primer_dia_mes = hoy.strftime("%Y-%m-01")
    query = f"""
    WITH ultimo_registro AS (
        SELECT nid, status, fecha_inicio, modified_date, nombre_agendador, email_agendador,
            ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE nid != 'nan' AND nid IS NOT NULL AND visit_type = 'Habi Inmobiliaria'
    ),
    canceladas AS (
        SELECT nid, fecha_inicio AS fecha_agendada, modified_date, nombre_agendador,
               email_agendador, status
        FROM ultimo_registro
        WHERE rn = 1 AND status IN ('Cancelado', 'No realizada', 'Cerrado')
    )
    SELECT v.*, c.c_comercial_captacion,
        CASE WHEN v.modified_date >= '{fecha_desde_nuevas}' THEN 'nueva' ELSE 'pendiente' END AS tipo
    FROM canceladas v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    WHERE v.modified_date >= '{primer_dia_mes}'
    """
    results = client.query(query).result()
    mapa = {}
    hay_nuevas = False
    for row in results:
        email = row.c_comercial_captacion or row.email_agendador or ""
        if not email or email == "nan":
            continue
        if email not in mapa:
            mapa[email] = {"email": email, "nombre": row.nombre_agendador or email,
                           "nuevas": [], "pendientes": []}
        fecha_ag = str(row.fecha_agendada).split("T")[0] if row.fecha_agendada else ""
        entry = {"nid": str(row.nid), "fecha": fecha_ag}
        if row.tipo == "nueva":
            mapa[email]["nuevas"].append(entry); hay_nuevas = True
        else:
            mapa[email]["pendientes"].append(entry)
    if not hay_nuevas:
        resumen["motivo"] = "sin_canceladas_nuevas"
        return resumen
    comerciales = [c for c in mapa.values() if c["nuevas"]]

    for c in comerciales:
        nuevas_list = "\n".join(f"  - NID {n['nid']} (estaba agendado para {n['fecha']})" for n in c["nuevas"])
        cuerpo = (
            f"Hola {c['nombre']},\n\n"
            f"Las siguientes visitas que tenias agendadas para ayer fueron canceladas, reagendalas:\n\n"
            f"{nuevas_list}\n\n"
        )
        if c["pendientes"]:
            pend = "\n".join(f"  - NID {n['nid']} (agendado para {n['fecha']})" for n in c["pendientes"])
            cuerpo += f"Ademas, tienes estas canceladas de la semana pendientes por reagendar:\n\n{pend}\n\n"
        cuerpo += f"Ingresa aqui para gestionarlas:\n{PAGE_URL}\n\nSaludos,\nEquipo Habi"
        asunto = f"Visita(s) cancelada(s) ayer - Reagendar ({len(c['nuevas'])} nueva(s))"
        item = {"email": c["email"], "nombre": c["nombre"],
                "nuevas": len(c["nuevas"]), "pendientes": len(c["pendientes"]),
                "ok": False, "error": ""}
        try:
            _send_email(c["email"], asunto, cuerpo)
            item["ok"] = True
            resumen["enviados"] += 1
        except Exception as e:
            item["error"] = str(e)
            resumen["errores"] += 1
        resumen["comerciales"].append(item)
    return resumen


@app.route("/api/notificar/canceladas", methods=["POST"])
def api_notificar_canceladas():
    """Dispara on-demand el envio de correos de canceladas a comerciales.
    Usa la misma logica que el cron pero forzando aunque sea fin de semana."""
    try:
        resumen = _notificar_canceladas_reagendar(forzar=True)
        return jsonify({"ok": True, **resumen})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/1ZvSbRye1Mq-mv6iIW1IyaFbG97aJsXffBc0Gkd-mylk/export?format=csv"


def get_links_publicacion():
    """Lee el Google Sheet y devuelve un dict nid -> link_publicacion.

    Defensivo: prueba varias variantes del nombre de columna por si la tilde
    de "Publicación" se rompe por encoding/BOM en algunos entornos.
    """
    try:
        r = http_requests.get(SHEETS_CSV_URL, timeout=15)
        # Forzar utf-8 (Google a veces no setea encoding correctamente)
        r.encoding = "utf-8"
        text = r.text
        # Quitar BOM si existe
        if text.startswith("﻿"):
            text = text[1:]
        reader = csv.DictReader(io.StringIO(text))
        # Detectar el nombre real de la columna de NID y de link en los headers
        fieldnames = reader.fieldnames or []
        def _find(*keys):
            for k in keys:
                for f in fieldnames:
                    if (f or "").strip().lower() == k.lower():
                        return f
            return None
        nid_col = _find("NID", "nid")
        link_col = _find("Link Publicación", "Link Publicacion", "link_publicacion",
                         "Link publicación", "link publicacion")
        if not nid_col or not link_col:
            print(f"[links] columnas no encontradas. headers={fieldnames}")
            return {}
        links = {}
        for row in reader:
            nid = str(row.get(nid_col, "")).strip()
            link = (row.get(link_col) or "").strip()
            if nid and link:
                links[nid] = link
        return links
    except Exception as e:
        print(f"Error leyendo Google Sheet: {e}")
        return {}


@app.route("/api/links")
def api_links():
    """Inmuebles del pipeline Inmo PUBLICADOS ACTIVOS con su link del Sheet.

    Reglas para 'publicado actualmente':
      property_card.active = 1  AND  property_state.current_state_id = 2
    (cruce de las dos tablas; pc.active solo es insuficiente).
    """
    query = """
    WITH inmo AS (
      SELECT DISTINCT CAST(nid AS STRING) AS nid
      FROM `papyrus-master.squad_bi_global.hubspot_deal`
      WHERE pipeline = '803674753' AND nid IS NOT NULL
    ),
    publicados_activos AS (
      -- NIDs actualmente publicados (ambas reglas obligatorias)
      SELECT DISTINCT
        CAST(pc.nid AS STRING) AS nid,
        DATE(ps.date_publication) AS fecha_publicacion
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      JOIN `papyrus-data.habi_brokers_listing.property_state` ps
        ON ps.property_card_id = pc.id
      WHERE pc.active = 1
        AND ps.current_state_id = 2
        AND pc.nid IS NOT NULL
    )
    SELECT
      pa.nid,
      pa.fecha_publicacion,
      COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS comercial,
      COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS equipo,
      cd.ciudad
    FROM publicados_activos pa
    JOIN inmo ON inmo.nid = pa.nid
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
      ON CAST(cd.nid AS STRING) = pa.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(pa.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    ORDER BY pa.fecha_publicacion DESC
    """
    try:
        results = client.query(query).result()
        links = get_links_publicacion()
        out = []
        seen = set()
        for row in results:
            nid = str(row.nid) if row.nid else ""
            if not nid or nid in seen:
                continue
            seen.add(nid)
            link = links.get(nid, "")
            if not link:
                continue  # solo mostrar los que tienen link en el Sheet
            out.append({
                "nid": nid,
                "fecha_publicacion": str(row.fecha_publicacion) if row.fecha_publicacion else "",
                "comercial": clean(row.comercial),
                "equipo": clean(row.equipo),
                "ciudad": (row.ciudad or "").title() if row.ciudad else "",
                "link_publicacion": link,
            })
        return jsonify(out)
    except Exception as e:
        print(f"Error consultando links: {e}")
        return jsonify([])


@app.route("/api/por-agendar")
def api_por_agendar():
    query = """
    WITH bubble_unica AS (
      SELECT * FROM (
        SELECT b.nid, b.status, b.fecha_inicio, b.modified_date,
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
      WHERE visit_category = 'Habi Inmobiliaria'
        AND status = 'Finalizado'
    ),
    gravamen_sellers AS (
      SELECT nid, ANY_VALUE(gravamenes_del_apartamento) AS gravamen
      FROM `papyrus-data.habi_wh_inmobiliaria.habiinmobiliaria_sellers_gestion`
      GROUP BY nid
    )
    SELECT
      cd.nid,
      COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS c_comercial_captacion,
      COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS c_equipo_seller,
      cd.ciudad,
      DATE(cd.c_fecha_captacion) AS fecha_captacion,
      cd.tel_fono_del_cliente_1 AS telefono_cliente,
      b.status AS ultimo_status,
      d.date_publication,
      CASE WHEN fc.nid IS NOT NULL THEN 'Publicado sin fotos profesionales' ELSE 'Sin publicar' END AS tipo_fotos
    FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
    LEFT JOIN bubble_unica b ON CAST(cd.nid AS STRING) = b.nid
    LEFT JOIN `papyrus-data.habi_brokers_listing.property_card` pc ON cd.nid = pc.nid
    LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    LEFT JOIN tiene_fotos_cliente fc ON cd.nid = fc.nid
    LEFT JOIN gravamen_sellers gs ON cd.nid = gs.nid
    WHERE cd.c_fecha_captacion IS NOT NULL
      AND cd.fecha_desistio_inmobiliaria IS NULL
      AND h.fecha_desistio_inmobiliaria IS NULL
      AND cd.v_fecha_venta IS NULL
      AND dealstage != '1182117639'
      -- Publicacion (fuente: consolidado_habi_inmobiliaria.date_publication):
      --  * sin publicar => incluir
      --  * publicado >= 2026-04-13 con fotos de cliente (source_image_id = 3) => incluir
      --  * publicado antes del 13-abr => excluir
      --    (antes del 13-abr todo se publicaba con fotos 360 profesionales,
      --     excepto Jamundi que va aparte por el filtro de ciudad)
      AND (
        cd.date_publication IS NULL
        OR (fc.nid IS NOT NULL AND DATE(cd.date_publication) >= DATE '2026-04-13')
      )
      AND CAST(cd.nid AS STRING) NOT IN (SELECT nid FROM nids_con_finalizado)
      AND (b.nid IS NULL OR b.status NOT IN ('Agendado', 'Cerrado'))
      -- El unico bloqueante es patrimonio de familia con hijos menores.
      -- Hipoteca, afectacion familiar sin menores, etc. NO bloquean.
      AND (
        -- Caso A: tabla oficial de estado_patrimonio marca que no hay patrimonio activo
        d.estado_patrimonio IN ('Sin patrimonio', 'Patrimonio levantado')
        -- Caso B: sin registro en esa tabla, usamos gravamenes_del_apartamento como fallback
        OR (
          d.estado_patrimonio IS NULL
          AND (gs.gravamen IS NULL
               OR gs.gravamen NOT IN ('Hipoteca + Patrimonio con hijos', 'Patrimonio hijos'))
        )
      )
      AND LOWER(COALESCE(cd.ciudad, '')) NOT LIKE '%jamundi%'
      AND LOWER(COALESCE(cd.ciudad, '')) NOT LIKE '%jamundí%'
    ORDER BY cd.c_fecha_captacion DESC
    """
    try:
        results = client.query(query).result()
        inmuebles = []
        seen = set()
        for row in results:
            nid = str(row.nid) if row.nid else ""
            if nid in seen:
                continue
            seen.add(nid)
            inmuebles.append({
                "nid": nid,
                "comercial": row.c_comercial_captacion or "",
                "ciudad": (row.ciudad or "").title(),
                "equipo": row.c_equipo_seller or "",
                "fecha_captacion": str(row.fecha_captacion) if row.fecha_captacion else "",
                "telefono_cliente": str(row.telefono_cliente) if row.telefono_cliente else "",
                "ultimo_status": row.ultimo_status or "Sin visita",
                "tipo_fotos": row.tipo_fotos or "",
            })
        return jsonify(inmuebles)
    except Exception as e:
        print(f"Error consultando por agendar: {e}")
        return jsonify([])


@app.route("/api/por-publicar")
def api_por_publicar():
    query = """
    WITH bubble_unica AS (
      SELECT * FROM (
        SELECT b.*,
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
    tiene_fotos_360 AS (
      SELECT DISTINCT pc.nid
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
        ON pc.id = pi.property_card_id
      WHERE pi.source_image_id = 1
    ),
    base AS (
      SELECT cd.nid, COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS c_comercial_captacion, cd.ciudad, COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS c_equipo_seller,
        DATE(SAFE_CAST(NULLIF(b.fecha_inicio, 'nan') AS TIMESTAMP)) AS Fecha_recorrido,
        b.status,
        d.estado_patrimonio,
        d.date_publication,
        CASE WHEN pc.id IS NULL THEN 'Sin CMS' ELSE 'Con CMS' END AS ficha_cms,
        CASE WHEN fc.nid IS NOT NULL THEN 'Publicado sin fotos profesionales'
             ELSE 'Sin publicar' END AS tipo_fotos
      FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
      LEFT JOIN bubble_unica b ON CAST(cd.nid AS STRING) = b.nid
      LEFT JOIN `papyrus-data.habi_brokers_listing.property_card` pc ON cd.nid = pc.nid
      LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
      LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
        ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
      LEFT JOIN tiene_fotos_cliente fc ON cd.nid = fc.nid
      LEFT JOIN tiene_fotos_360 f360 ON cd.nid = f360.nid
      WHERE cd.fecha_desistio_inmobiliaria IS NULL
        AND h.fecha_desistio_inmobiliaria IS NULL
        AND dealstage != '1182117639'
        AND cd.c_fecha_captacion IS NOT NULL
        AND d.estado_patrimonio = 'Sin patrimonio'
        AND b.status = 'Finalizado'
        AND f360.nid IS NULL
        AND (d.date_publication IS NULL OR fc.nid IS NOT NULL)
        AND (cd.date_publication IS NULL OR fc.nid IS NOT NULL)
        AND cd.v_fecha_venta IS NULL
    ),
    base_unica AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY nid ORDER BY Fecha_recorrido DESC NULLS LAST) AS rn FROM base
    )
    SELECT nid, c_comercial_captacion, ciudad, c_equipo_seller, Fecha_recorrido, status,
      estado_patrimonio, ficha_cms, tipo_fotos, date_publication,
      CONCAT(IFNULL(estado_patrimonio, ''), ', ', ficha_cms, ', ', tipo_fotos) AS Estado_actual
    FROM base_unica WHERE rn = 1
    ORDER BY Fecha_recorrido DESC
    """
    try:
        results = client.query(query).result()
        links = get_links_publicacion()
        inmuebles = []
        for row in results:
            nid = str(row.nid) if row.nid else ""
            inmuebles.append({
                "nid": nid,
                "comercial": row.c_comercial_captacion or "",
                "ciudad": (row.ciudad or "").title(),
                "equipo": row.c_equipo_seller or "",
                "fecha_recorrido": str(row.Fecha_recorrido) if row.Fecha_recorrido else "",
                "status_bubble": row.status or "",
                "ficha_cms": row.ficha_cms or "",
                "patrimonio": row.estado_patrimonio or "",
                "visita_360": "Con 360",
                "tipo_fotos": row.tipo_fotos or "",
                "estado_actual": row.Estado_actual or "",
                "link_publicacion": links.get(nid, ""),
            })

        # Agregar NIDs del correo de cleaning (visitados "SI") que no estén en la lista
        nids_existentes = set(i["nid"] for i in inmuebles)
        completados = leer_completados_correo()
        nids_correo = list(set(c["nid"] for c in completados))
        nids_nuevos = [n for n in nids_correo if n not in nids_existentes]

        if nids_nuevos:
            nids_str = ",".join(f"'{n}'" for n in nids_nuevos)
            q_extra = f"""
            SELECT CAST(cd.nid AS STRING) AS nid, cd.c_comercial_captacion, cd.ciudad, cd.c_equipo_seller,
              d.estado_patrimonio
            FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
            LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
            LEFT JOIN (
              SELECT DISTINCT pc2.nid
              FROM `papyrus-data.habi_brokers_listing.property_card` pc2
              INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi2
                ON pc2.id = pi2.property_card_id
              WHERE pi2.source_image_id = 3
            ) fc ON cd.nid = fc.nid
            LEFT JOIN (
              SELECT DISTINCT pc3.nid
              FROM `papyrus-data.habi_brokers_listing.property_card` pc3
              INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi3
                ON pc3.id = pi3.property_card_id
              WHERE pi3.source_image_id = 1
            ) fp ON cd.nid = fp.nid
            WHERE CAST(cd.nid AS STRING) IN ({nids_str})
              AND cd.fecha_desistio_inmobiliaria IS NULL
              AND (d.estado_patrimonio IS NULL OR d.estado_patrimonio = 'Sin patrimonio')
              AND (d.date_publication IS NULL OR fc.nid IS NOT NULL)
              AND (cd.date_publication IS NULL OR fc.nid IS NOT NULL)
              AND cd.v_fecha_venta IS NULL
              AND fp.nid IS NULL
            """
            try:
                for row in client.query(q_extra).result():
                    nid = str(row.nid)
                    completado = next((c for c in completados if c["nid"] == nid), {})
                    inmuebles.append({
                        "nid": nid,
                        "comercial": row.c_comercial_captacion or "",
                        "ciudad": (row.ciudad or completado.get("ciudad_correo", "")).title(),
                        "equipo": row.c_equipo_seller or "",
                        "fecha_recorrido": completado.get("fecha_reporte", "Reciente (correo)"),
                        "status_bubble": "",
                        "ficha_cms": "",
                        "patrimonio": row.estado_patrimonio or "Por confirmar",
                        "visita_360": "Con 360",
                        "tipo_fotos": "Con fotos profesionales",
                        "estado_actual": f"Con fotos profesionales - Visitado {completado.get('fecha_reporte', '')} (correo)",
                        "link_publicacion": links.get(nid, ""),
                    })
            except Exception as e:
                print(f"Error enriqueciendo completados correo: {e}")

        # Poner los del correo de primeros, todo ordenado de más reciente a más viejo
        correo_nids = set(c["nid"] for c in completados) if completados else set()
        del_correo = sorted([i for i in inmuebles if i["nid"] in correo_nids],
                            key=lambda x: x.get("fecha_recorrido", "") or "", reverse=True)
        del_bq = sorted([i for i in inmuebles if i["nid"] not in correo_nids],
                        key=lambda x: x.get("fecha_recorrido", "") or "", reverse=True)
        inmuebles = del_correo + del_bq

        return jsonify(inmuebles)
    except Exception as e:
        print(f"Error consultando por publicar: {e}")
        return jsonify([])


def leer_nids_fotos_cliente():
    """Lee NIDs de correos con asunto 'Publicación Fotos cliente'."""
    nids = []
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")
        status, messages = mail.search(None, 'SUBJECT "Fotos cliente"')
        ids = messages[0].split()
        for msg_id in ids:
            status, data = mail.fetch(msg_id, "(RFC822)")
            msg = email_lib.message_from_bytes(data[0][1])
            subject = msg["subject"] or ""
            # Decodificar subject
            from email.header import decode_header
            decoded_parts = decode_header(subject)
            subj = ""
            for part, enc in decoded_parts:
                if isinstance(part, bytes):
                    subj += part.decode(enc or "utf-8", errors="replace")
                else:
                    subj += part
            # Extraer NID de entre parentesis: (58488002593)
            nid_match = re.search(r"\((\d{8,})\)", subj)
            if not nid_match:
                # Fallback: NID al inicio del asunto
                nid_match = re.search(r"(\d{8,})", subj)
            if nid_match:
                nids.append(nid_match.group(1))
        mail.logout()
    except Exception as e:
        print(f"Error leyendo correos fotos cliente: {e}")
    return list(set(nids))


@app.route("/api/por-publicar-fotos-correo")
def api_por_publicar_fotos_correo():
    nids_correo = leer_nids_fotos_cliente()
    if not nids_correo:
        return jsonify([])

    nids_str = ",".join(f"'{n}'" for n in nids_correo)
    query = f"""
    WITH tiene_fotos_cliente AS (
      SELECT DISTINCT pc.nid
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
        ON pc.id = pi.property_card_id
      WHERE pi.source_image_id = 3
    ),
    tiene_fotos_360 AS (
      SELECT DISTINCT pc.nid
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
        ON pc.id = pi.property_card_id
      WHERE pi.source_image_id = 1
    )
    SELECT
      CAST(cd.nid AS STRING) AS nid,
      COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS comercial,
      cd.ciudad,
      COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS equipo,
      DATE(cd.c_fecha_captacion) AS fecha_captacion,
      cd.tel_fono_del_cliente_1 AS telefono_cliente,
      d.estado_patrimonio
    FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
    LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    LEFT JOIN tiene_fotos_cliente fc ON cd.nid = fc.nid
    LEFT JOIN tiene_fotos_360 f360 ON cd.nid = f360.nid
    WHERE CAST(cd.nid AS STRING) IN ({nids_str})
      AND cd.fecha_desistio_inmobiliaria IS NULL
      AND f360.nid IS NULL
      AND (d.estado_patrimonio IS NULL OR d.estado_patrimonio = 'Sin patrimonio')
      AND (cd.date_publication IS NULL OR fc.nid IS NOT NULL)
      AND (d.date_publication IS NULL OR fc.nid IS NOT NULL)
    """
    try:
        results = client.query(query).result()
        inmuebles = []
        for row in results:
            inmuebles.append({
                "nid": str(row.nid),
                "comercial": row.comercial or "",
                "ciudad": (row.ciudad or "").title(),
                "equipo": row.equipo or "",
                "fecha_captacion": str(row.fecha_captacion) if row.fecha_captacion else "",
                "telefono_cliente": str(row.telefono_cliente) if row.telefono_cliente else "",
                "patrimonio": row.estado_patrimonio or "Sin info",
            })
        return jsonify(inmuebles)
    except Exception as e:
        print(f"Error consultando por publicar fotos correo: {e}")
        return jsonify([])


@app.route("/api/por-publicar-sin-fotos")
def api_por_publicar_sin_fotos():
    query = """
    WITH bubble_unica AS (
      SELECT * FROM (
        SELECT b.nid, b.status,
          ROW_NUMBER() OVER (PARTITION BY b.nid ORDER BY b.modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co` b
        WHERE b.visit_category = 'Habi Inmobiliaria'
      ) WHERE rn = 1
    ),
    tiene_fotos_360 AS (
      SELECT DISTINCT pc.nid
      FROM `papyrus-data.habi_brokers_listing.property_card` pc
      INNER JOIN `papyrus-data.habi_brokers_listing.property_image` pi
        ON pc.id = pi.property_card_id
      WHERE pi.source_image_id = 1
    ),
    gravamen_sellers AS (
      SELECT nid, ANY_VALUE(gravamenes_del_apartamento) AS gravamen
      FROM `papyrus-data.habi_wh_inmobiliaria.habiinmobiliaria_sellers_gestion`
      GROUP BY nid
    ),
    -- NIDs marcados como NPH (Nuevo Proyecto Habitacional) en HubSpot.
    -- Estos los gestiona el equipo NPH, no aparecen en este tab.
    nph_nids AS (
      SELECT DISTINCT CAST(nid AS STRING) AS nid
      FROM `sellers-main-prod.hubspot.deals`
      WHERE flag_inmueble_nph IS NOT NULL
        AND flag_inmueble_nph != ''
        AND nid IS NOT NULL
    )
    SELECT
      cd.nid,
      COALESCE(h.hubspot_owner_id, cd.c_comercial_captacion) AS c_comercial_captacion,
      cd.ciudad,
      COALESCE(h.equipo_sellers, cd.c_equipo_seller) AS c_equipo_seller,
      DATE(cd.c_fecha_captacion) AS fecha_captacion,
      cd.tel_fono_del_cliente_1 AS telefono_cliente
    FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
    LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h
      ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
    LEFT JOIN bubble_unica b ON CAST(cd.nid AS STRING) = b.nid
    LEFT JOIN tiene_fotos_360 f360 ON cd.nid = f360.nid
    LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
    LEFT JOIN gravamen_sellers gs ON cd.nid = gs.nid
    LEFT JOIN nph_nids nph ON nph.nid = CAST(cd.nid AS STRING)
    WHERE cd.c_fecha_captacion IS NOT NULL
      AND cd.fecha_desistio_inmobiliaria IS NULL
      AND h.fecha_desistio_inmobiliaria IS NULL
      AND cd.date_publication IS NULL
      AND cd.v_fecha_venta IS NULL
      AND dealstage != '1182117639'
      AND f360.nid IS NULL
      AND (b.nid IS NULL OR b.status != 'Finalizado')
      AND nph.nid IS NULL  -- excluir NPH
      -- Excluye patrimonio de familia con hijos menores activo.
      -- Si ya se levanto (estado_patrimonio = 'Patrimonio levantado') o nunca hubo, pasa.
      AND (
        d.estado_patrimonio IN ('Sin patrimonio', 'Patrimonio levantado')
        OR (
          d.estado_patrimonio IS NULL
          AND (gs.gravamen IS NULL
               OR gs.gravamen NOT IN ('Hipoteca + Patrimonio con hijos', 'Patrimonio hijos'))
        )
      )
      -- Excluye los que firmaron el contrato de corretaje con patrimonio de familia:
      -- esos los gestiona el equipo de levantamiento de Habi, no requieren accion del comercial.
      AND COALESCE(cd.c_tipo_contrato_firmado, '') != 'Contrato de Corretaje con patrimonio de familia'
    ORDER BY cd.c_fecha_captacion DESC
    """
    try:
        results = client.query(query).result()
        inmuebles = []
        seen = set()
        for row in results:
            nid = str(row.nid) if row.nid else ""
            if nid in seen:
                continue
            seen.add(nid)
            inmuebles.append({
                "nid": nid,
                "comercial": row.c_comercial_captacion or "",
                "ciudad": (row.ciudad or "").title(),
                "equipo": row.c_equipo_seller or "",
                "fecha_captacion": str(row.fecha_captacion) if row.fecha_captacion else "",
                "telefono_cliente": str(row.telefono_cliente) if row.telefono_cliente else "",
            })
        return jsonify(inmuebles)
    except Exception as e:
        print(f"Error consultando por publicar sin fotos: {e}")
        return jsonify([])


@app.route("/api/cancelar", methods=["POST"])
def cancelar_visita():
    data = request.json
    nid = data.get("nid", "")
    ciudad = data.get("ciudad", "")
    fecha_fin = data.get("fecha_fin", "")
    comercial = data.get("comercial", "")

    asunto = "CANCELADA 360"
    cuerpo = (
        f"La visita 360 del NID {nid} agendada para el {fecha_fin} "
        f"fue cancelada por el cliente.\n\n"
        f"Ciudad: {ciudad}\n"
        f"Comercial: {comercial}\n"
    )

    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        msg = MIMEMultipart()
        destinatarios = [EMAIL_USER, "mariaalonso@habi.co"]
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(destinatarios)
        msg["Subject"] = asunto
        msg.attach(MIMEText(cuerpo, "plain"))
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        return jsonify({"status": "ok"})
    except Exception as e:
        print(f"Error enviando correo cancelación: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


ESTADOS_FILE = os.path.join(os.path.dirname(__file__), "estados_visitas.json")


def cargar_estados():
    try:
        with open(ESTADOS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def guardar_estados(estados):
    with open(ESTADOS_FILE, "w") as f:
        json.dump(estados, f)


@app.route("/api/estado", methods=["POST"])
def guardar_estado_visita():
    """Guarda el estado de una visita (confirmada/cancelada)."""
    data = request.json
    nid = data.get("nid", "")
    estado = data.get("estado", "")  # confirmada, cancelada, o vacío para limpiar
    fecha = get_fecha_manana()

    estados = cargar_estados()
    if fecha not in estados:
        estados[fecha] = {}

    if estado:
        estados[fecha][nid] = estado
    elif nid in estados.get(fecha, {}):
        del estados[fecha][nid]

    guardar_estados(estados)
    return jsonify({"status": "ok"})


ESTADOS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbz0mAvpiwFYUMhZWwd6JxbUHVVh6EH-d8eiRqFRXmjIJaFh6xFPmz4-tWk5I8Ww-Wpe/exec"
ESTADOS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1rxvkkdcCnv6eoyRBMvGgjbiP2tiITE_mO7wpZDpoCOw/export?format=csv"


def leer_sheet_estados():
    """Lee todas las filas del Sheet de estados."""
    filas = []
    try:
        r = http_requests.get(ESTADOS_SHEET_URL, timeout=15)
        reader = csv.DictReader(io.StringIO(r.text))
        for row in reader:
            filas.append({
                "nid": (row.get("nid") or "").strip(),
                "fecha": (row.get("fecha") or "").strip(),
                "estado": (row.get("estado") or "").strip(),
            })
    except Exception as e:
        print(f"Error leyendo Sheet: {e}")
    return filas


@app.route("/api/estados")
def obtener_estados():
    """Devuelve todos los estados de mañana, separados por tipo."""
    fecha = get_fecha_manana()
    visita_estados = {}
    whatsapp = {}
    cancel_contacto = {}
    pub_estado = {}
    pub_comentario = {}
    sf_estado = {}
    sf_comentario = {}
    fc_estado = {}
    fc_comentario = {}
    juzgado_estado = {}  # contactado / interesado / no_interesado

    for row in leer_sheet_estados():
        nid = row["nid"]
        f = row["fecha"]
        estado = row["estado"]
        if not nid or not estado:
            continue

        # Estados diarios (visitas, whatsapp, canceladas)
        if f == fecha:
            if estado.startswith("wa:"):
                whatsapp[nid] = True
            elif estado.startswith("cc:"):
                cancel_contacto[nid] = estado[3:]
            elif not estado.startswith(("ps:", "pc:", "sf:", "sc:", "jz:", "fc:", "fd:")):
                visita_estados[nid] = estado

        # Estados permanentes (publicar, sin fotos, juzgado)
        if f == "perm":
            if estado.startswith("ps:") and estado[3:]:
                pub_estado[nid] = estado[3:]
            elif estado.startswith("pc:"):
                pub_comentario[nid] = estado[3:]
            elif estado.startswith("sf:") and estado[3:]:
                sf_estado[nid] = estado[3:]
            elif estado.startswith("sc:"):
                sf_comentario[nid] = estado[3:]
            elif estado.startswith("fc:") and estado[3:]:
                fc_estado[nid] = estado[3:]
            elif estado.startswith("fd:"):
                fc_comentario[nid] = estado[3:]
            elif estado.startswith("jz:") and estado[3:]:
                juzgado_estado[nid] = estado[3:]

    return jsonify({
        "visita": visita_estados,
        "whatsapp": whatsapp,
        "cancelContacto": cancel_contacto,
        "pubEstado": pub_estado,
        "pubComentario": pub_comentario,
        "sfEstado": sf_estado,
        "sfComentario": sf_comentario,
        "fcEstado": fc_estado,
        "fcComentario": fc_comentario,
        "juzgado": juzgado_estado,
    })


@app.route("/api/guardar-estado", methods=["POST"])
def guardar_estado_compartido():
    """Escribe un estado al Google Sheet via Apps Script.

    Pasa también el campo `usuario` para registrar quién hizo el cambio.
    """
    data = request.json
    nid = data.get("nid", "")
    fecha = data.get("fecha", get_fecha_manana())
    estado = data.get("estado", "")
    usuario = (data.get("usuario") or "").strip()
    try:
        http_requests.post(ESTADOS_SCRIPT_URL, json={
            "nid": nid, "fecha": fecha, "estado": estado, "usuario": usuario
        }, timeout=10)
    except Exception as e:
        print(f"Error escribiendo al Sheet: {e}")
    return jsonify({"status": "ok"})
    return jsonify(estados)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)
