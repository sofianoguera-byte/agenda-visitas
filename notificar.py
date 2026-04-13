import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.cloud import bigquery
from datetime import datetime, timedelta
import os

# Configuración
SMTP_USER = os.environ.get("SMTP_USER", "sofianoguera@habi.co")
SMTP_PASS = os.environ.get("SMTP_PASS", "ujst tpuv fazx pjtu")
PAGE_URL = "http://localhost:5000"

client = bigquery.Client(project="papyrus-data")


def get_fecha_manana():
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def get_fecha_ayer():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_fecha_hoy():
    return datetime.now().strftime("%Y-%m-%d")


def enviar_correo(destinatario, asunto, cuerpo):
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.attach(MIMEText(cuerpo, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def notificar_visitas_manana():
    """Notifica a comerciales sobre sus visitas de mañana."""
    fecha = get_fecha_manana()
    print(f"\n[{datetime.now()}] === VISITAS DE MAÑANA ({fecha}) ===")

    query = f"""
    WITH visitas_dedup AS (
        SELECT nid, MIN(fecha_inicio) AS fecha_inicio,
            MIN(nombre_agendador) AS nombre_agendador,
            MIN(email_agendador) AS email_agendador
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE fecha_fin LIKE '{fecha}%'
            AND nid != 'nan' AND nid IS NOT NULL
            AND status = 'Agendado' AND visit_type = 'Habi Inmobiliaria'
        GROUP BY nid
    )
    SELECT v.*, c.c_comercial_captacion
    FROM visitas_dedup v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    """
    results = client.query(query).result()

    mapa = {}
    for row in results:
        email = row.c_comercial_captacion or row.email_agendador or ""
        if not email or email == "nan":
            continue
        if email not in mapa:
            mapa[email] = {"email": email, "nombre": row.nombre_agendador or email, "count": 0}
        mapa[email]["count"] += 1

    comerciales = list(mapa.values())
    print(f"Encontrados {sum(c['count'] for c in comerciales)} visitas para {len(comerciales)} comerciales")

    for c in comerciales:
        asunto = f"Tienes {c['count']} visita(s) 360 agendada(s) para manana {fecha}"
        cuerpo = (
            f"Hola {c['nombre']},\n\n"
            f"Tienes {c['count']} visita(s) 360 agendada(s) para manana {fecha}.\n\n"
            f"Ingresa en el siguiente link y escribeles para confirmar la visita:\n"
            f"{PAGE_URL}\n\n"
            f"Utiliza los botones de WhatsApp para escribirles y usa los botones "
            f"de Confirmar y Cancelar para informar a Remo.\n\n"
            f"Saludos,\nEquipo Habi"
        )
        try:
            enviar_correo(c["email"], asunto, cuerpo)
            print(f"  Correo enviado a {c['email']} ({c['count']} visitas)")
        except Exception as e:
            print(f"  ERROR enviando a {c['email']}: {e}")


def notificar_canceladas_reagendar():
    """Notifica a comerciales sobre visitas canceladas que deben reagendar."""
    ayer = get_fecha_ayer()
    hoy = get_fecha_hoy()
    print(f"\n[{datetime.now()}] === CANCELADAS POR REAGENDAR ({ayer} - {hoy}) ===")

    query = f"""
    WITH canceladas AS (
        SELECT nid, MIN(fecha_inicio) AS fecha_agendada,
            MIN(nombre_agendador) AS nombre_agendador,
            MIN(email_agendador) AS email_agendador,
            MIN(status) AS status
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE nid != 'nan' AND nid IS NOT NULL
            AND visit_type = 'Habi Inmobiliaria'
            AND status IN ('Cancelado', 'No realizada')
            AND (modified_date LIKE '{ayer}%' OR modified_date LIKE '{hoy}%')
        GROUP BY nid
    )
    SELECT v.*, c.c_comercial_captacion
    FROM canceladas v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    """
    results = client.query(query).result()

    mapa = {}
    for row in results:
        email = row.c_comercial_captacion or row.email_agendador or ""
        if not email or email == "nan":
            continue
        if email not in mapa:
            mapa[email] = {"email": email, "nombre": row.nombre_agendador or email, "nids": []}
        fecha_ag = str(row.fecha_agendada).split("T")[0] if row.fecha_agendada else ""
        mapa[email]["nids"].append({"nid": str(row.nid), "fecha": fecha_ag})

    comerciales = list(mapa.values())
    total_nids = sum(len(c["nids"]) for c in comerciales)
    print(f"Encontradas {total_nids} canceladas para {len(comerciales)} comerciales")

    for c in comerciales:
        nids_list = "\n".join(f"  - NID {n['nid']} (agendado para {n['fecha']})" for n in c["nids"])
        asunto = f"Tienes {len(c['nids'])} visita(s) cancelada(s) por reagendar"
        cuerpo = (
            f"Hola {c['nombre']},\n\n"
            f"Las siguientes visitas fueron canceladas y necesitan ser reagendadas:\n\n"
            f"{nids_list}\n\n"
            f"Ingresa en el siguiente link, ve a la pestana 'Canceladas por Reagendar' "
            f"y contacta a los propietarios para reagendar:\n"
            f"{PAGE_URL}\n\n"
            f"Saludos,\nEquipo Habi"
        )
        try:
            enviar_correo(c["email"], asunto, cuerpo)
            print(f"  Correo enviado a {c['email']} ({len(c['nids'])} canceladas)")
        except Exception as e:
            print(f"  ERROR enviando a {c['email']}: {e}")


def main():
    notificar_visitas_manana()
    notificar_canceladas_reagendar()
    print(f"\n[{datetime.now()}] Proceso completado.")


if __name__ == "__main__":
    main()
