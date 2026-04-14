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
import os
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

# Credenciales correo
EMAIL_USER = "sofianoguera@habi.co"
EMAIL_PASS = "ujst tpuv fazx pjtu"


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
    query = f"""
    WITH visitas_manana AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE nid != 'nan'
            AND nid IS NOT NULL
            AND visit_type = 'Habi Inmobiliaria'
            AND status = 'Agendado'
            AND fecha_fin LIKE '{tomorrow}%'
    )
    SELECT
        v.nid, v.fecha_fin, v.fecha_inicio, v.ciudad_muni, v.zona,
        v.direccion, v.torre_apto, v.conjunto, v.visit_type, v.visit_category,
        v.nombre_agendador, v.email_agendador, v.nombre_visitador, v.email_visitador,
        c.c_comercial_captacion,
        c.tel_fono_del_cliente_1 AS telefono_cliente,
        c.c_equipo_seller AS equipo
    FROM visitas_manana v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    WHERE v.rn = 1
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
    """Lee correos de la semana y extrae los NIDs visitados (SI)."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")

        desde = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
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
            todas_canceladas.extend(cancelados)

        mail.logout()

        if not todas_canceladas:
            return [], fechas_reporte

        # Enriquecer con BigQuery
        nids_unicos = list(set(n["nid"] for n in todas_canceladas))
        nids_str = ",".join(f"'{nid}'" for nid in nids_unicos)

        # 1. Solo mantener NIDs cuyo último registro en bubble sea Cancelado o No realizada
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
        try:
            for row in client.query(query_status).result():
                if row.status in ("Cancelado", "No realizada"):
                    nids_realmente_cancelados.add(str(row.nid))
                mod = str(row.modified_date) if row.modified_date else ""
                if mod and "T" in mod:
                    mod = mod.split("T")[0]
                nids_modified[str(row.nid)] = mod
        except Exception as e:
            print(f"Error verificando status: {e}")

        # Filtrar: solo los que realmente están cancelados en bubble
        todas_canceladas = [n for n in todas_canceladas if n["nid"] in nids_realmente_cancelados]
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
        # 3. Traer comercial + teléfono de consolidado
        query_consolidado = f"""
        SELECT
            CAST(c.nid AS STRING) AS nid,
            c.c_comercial_captacion,
            c.tel_fono_del_cliente_1 AS telefono_cliente,
            c.direccion,
            c.ciudad,
            c.c_equipo_seller AS equipo
        FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
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
    dias = request.args.get("dias", 7, type=int)
    canceladas, fechas_reporte = leer_canceladas_correo(dias)
    return jsonify({"canceladas": canceladas, "fechas_reporte": fechas_reporte})


SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/1ZvSbRye1Mq-mv6iIW1IyaFbG97aJsXffBc0Gkd-mylk/export?format=csv"


def get_links_publicacion():
    """Lee el Google Sheet y devuelve un dict nid -> link_publicacion."""
    try:
        r = http_requests.get(SHEETS_CSV_URL, timeout=15)
        reader = csv.DictReader(io.StringIO(r.text))
        links = {}
        for row in reader:
            nid = str(row.get("NID", "")).strip()
            link = (row.get("Link Publicación") or "").strip()
            if nid and link:
                links[nid] = link
        return links
    except Exception as e:
        print(f"Error leyendo Google Sheet: {e}")
        return {}


@app.route("/api/por-publicar")
def api_por_publicar():
    query = """
    WITH bubble_unica AS (
      SELECT * FROM (
        SELECT b.*, ROW_NUMBER() OVER (
          PARTITION BY b.nid ORDER BY
            DATE(SAFE_CAST(NULLIF(b.fecha_inicio, 'nan') AS TIMESTAMP)) DESC NULLS LAST,
            CASE WHEN b.status = 'Agendado' THEN 2 WHEN b.status = 'Finalizado' THEN 1
                 WHEN b.status = 'Cancelado' THEN 3 WHEN b.status = 'No realizada' THEN 4
                 WHEN b.status = 'En obra' THEN 5 ELSE 99 END,
            DATE(SAFE_CAST(NULLIF(b.fecha_agendado, 'nan') AS TIMESTAMP)) DESC NULLS LAST
        ) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co` b
        WHERE b.visit_category = 'Habi Inmobiliaria'
      ) WHERE rn = 1
    ),
    base AS (
      SELECT cd.nid, cd.c_comercial_captacion, cd.ciudad, cd.c_equipo_seller,
        DATE(SAFE_CAST(NULLIF(b.fecha_inicio, 'nan') AS TIMESTAMP)) AS Fecha_recorrido,
        b.status,
        CASE WHEN pc.id IS NULL THEN 'Sin cms' ELSE 'Con cms' END AS Tiene_ficha_CMS,
        CASE WHEN LOWER(cd.tipo_de_grav_men) LIKE '%patrimonio de familia con hijos menores%' THEN 'Patrimonio de familia' ELSE 'Sin patrimonio' END AS Patrimonio_familia,
        d.estado_patrimonio,
        d.estado_cms,
        CASE WHEN DATE(SAFE_CAST(NULLIF(b.fecha_inicio, 'nan') AS TIMESTAMP)) < CURRENT_DATE()
             AND b.status NOT IN ('Cancelado', 'No realizada', 'En obra', 'Agendado')
             THEN 'Con 360' ELSE 'Sin 360' END AS Visita_efectuada
      FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` cd
      LEFT JOIN bubble_unica b ON CAST(cd.nid AS STRING) = b.nid
      LEFT JOIN `papyrus-data.habi_brokers_listing.property_card` pc ON cd.nid = pc.nid
      LEFT JOIN `papyrus-delivery-data.inmobiliaria.detalle_estado_captaciones` d ON cd.nid = d.nid
      LEFT JOIN `papyrus-master.squad_bi_global.hubspot_deal` h ON SAFE_CAST(cd.nid AS INT64) = h.nid AND h.pipeline = '803674753'
      WHERE cd.fecha_desistio_inmobiliaria IS NULL AND h.fecha_desistio_inmobiliaria IS NULL AND d.date_publication IS NULL AND dealstage != '1182117639'
    ),
    base_unica AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY nid ORDER BY Fecha_recorrido DESC NULLS LAST) AS rn FROM base
    )
    SELECT nid, c_comercial_captacion, ciudad, c_equipo_seller, Fecha_recorrido, status,
      Tiene_ficha_CMS, Patrimonio_familia, estado_patrimonio, estado_cms, Visita_efectuada,
      CONCAT(IFNULL(estado_patrimonio, ''), ', ', IFNULL(Tiene_ficha_CMS, estado_cms), ', ', Visita_efectuada) AS Estado_actual
    FROM base_unica WHERE rn = 1
      AND Visita_efectuada = 'Con 360'
      AND estado_patrimonio IN ('Sin patrimonio', 'Patrimonio levantado')
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
                "ficha_cms": row.Tiene_ficha_CMS or "",
                "patrimonio": row.estado_patrimonio or "",
                "visita_360": row.Visita_efectuada or "",
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
            SELECT CAST(nid AS STRING) AS nid, c_comercial_captacion, ciudad, c_equipo_seller
            FROM `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria`
            WHERE CAST(nid AS STRING) IN ({nids_str})
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
                        "patrimonio": "",
                        "visita_360": "Con 360",
                        "estado_actual": f"Visitado {completado.get('fecha_reporte', '')} (correo)",
                        "link_publicacion": links.get(nid, ""),
                    })
            except Exception as e:
                print(f"Error enriqueciendo completados correo: {e}")

        return jsonify(inmuebles)
    except Exception as e:
        print(f"Error consultando por publicar: {e}")
        return jsonify([])


@app.route("/api/cancelar", methods=["POST"])
def cancelar_visita():
    data = request.json
    nid = data.get("nid", "")
    ciudad = data.get("ciudad", "")
    fecha_fin = data.get("fecha_fin", "")

    destinatario = "mariaalonso@habi.co"
    asunto = f"Cancelación visita 360 - NID {nid}"
    cuerpo = (
        f"Hola María José,\n\n"
        f"La visita 360 del NID {nid} agendada para el {fecha_fin} "
        f"fue cancelada por el cliente.\n\n"
        f"Saludos,\n"
        f"Equipo Comercial Habi"
    )

    mailto_url = f"mailto:{destinatario}?subject={quote(asunto)}&body={quote(cuerpo)}"
    gmail_url = f"https://mail.google.com/mail/?view=cm&to={destinatario}&su={quote(asunto)}&body={quote(cuerpo)}"
    return jsonify({"status": "ok", "mailto_url": mailto_url, "gmail_url": gmail_url})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)
