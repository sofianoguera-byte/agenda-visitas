import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.cloud import bigquery
from datetime import datetime, timedelta
import os

# Configuración
SMTP_USER = os.environ.get("SMTP_USER", "sofianoguera@habi.co")
SMTP_PASS = os.environ.get("SMTP_PASS", "ujst tpuv fazx pjtu")
PAGE_URL = "https://agenda-visitas-wcdm.onrender.com"

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
    """Notifica solo si hubo cancelaciones nuevas ayer. Incluye resumen de pendientes de la semana."""
    ayer = get_fecha_ayer()
    primer_dia_mes = datetime.now().strftime("%Y-%m-01")
    print(f"\n[{datetime.now()}] === CANCELADAS POR REAGENDAR (nuevas de {ayer} + pendientes del mes) ===")

    query = f"""
    WITH ultimo_registro AS (
        SELECT nid, status, fecha_inicio, modified_date, nombre_agendador, email_agendador,
            ROW_NUMBER() OVER (PARTITION BY nid ORDER BY modified_date DESC) AS rn
        FROM `papyrus-master.bubble_gold.mart_bubble_schedule_co`
        WHERE nid != 'nan' AND nid IS NOT NULL
            AND visit_type = 'Habi Inmobiliaria'
    ),
    canceladas AS (
        SELECT nid, fecha_inicio AS fecha_agendada, modified_date, nombre_agendador, email_agendador, status
        FROM ultimo_registro
        WHERE rn = 1 AND status IN ('Cancelado', 'No realizada')
    )
    SELECT v.*, c.c_comercial_captacion,
        CASE WHEN v.modified_date LIKE '{ayer}%' THEN 'nueva' ELSE 'pendiente' END AS tipo
    FROM canceladas v
    LEFT JOIN `papyrus-data.habi_wh_inmobiliaria.consolidado_habi_inmobiliaria` c
        ON v.nid = CAST(c.nid AS STRING)
    WHERE v.modified_date >= '{primer_dia_mes}'
    """
    results = client.query(query).result()

    mapa = {}
    hay_nuevas_global = False
    for row in results:
        email = row.c_comercial_captacion or row.email_agendador or ""
        if not email or email == "nan":
            continue
        if email not in mapa:
            mapa[email] = {"email": email, "nombre": row.nombre_agendador or email, "nuevas": [], "pendientes": []}
        fecha_ag = str(row.fecha_agendada).split("T")[0] if row.fecha_agendada else ""
        entry = {"nid": str(row.nid), "fecha": fecha_ag}
        if row.tipo == "nueva":
            mapa[email]["nuevas"].append(entry)
            hay_nuevas_global = True
        else:
            mapa[email]["pendientes"].append(entry)

    if not hay_nuevas_global:
        print("No hubo cancelaciones nuevas ayer. No se envian correos.")
        return

    # Solo enviar a comerciales que tengan al menos 1 nueva
    comerciales = [c for c in mapa.values() if c["nuevas"]]
    print(f"Comerciales con cancelaciones nuevas: {len(comerciales)}")

    for c in comerciales:
        nuevas_list = "\n".join(f"  - NID {n['nid']} (estaba agendado para {n['fecha']})" for n in c["nuevas"])

        cuerpo = (
            f"Hola {c['nombre']},\n\n"
            f"Las siguientes visitas que tenias agendadas para ayer fueron canceladas, reagendalas:\n\n"
            f"{nuevas_list}\n\n"
        )

        if c["pendientes"]:
            pendientes_list = "\n".join(f"  - NID {n['nid']} (agendado para {n['fecha']})" for n in c["pendientes"])
            cuerpo += (
                f"Ademas, tienes estas canceladas de la semana pendientes por reagendar:\n\n"
                f"{pendientes_list}\n\n"
            )

        cuerpo += (
            f"Ingresa aqui para gestionarlas:\n"
            f"{PAGE_URL}\n\n"
            f"Saludos,\nEquipo Habi"
        )

        asunto = f"Visita(s) cancelada(s) ayer - Reagendar ({len(c['nuevas'])} nueva(s))"

        try:
            enviar_correo(c["email"], asunto, cuerpo)
            print(f"  Correo enviado a {c['email']} ({len(c['nuevas'])} nuevas, {len(c['pendientes'])} pendientes)")
        except Exception as e:
            print(f"  ERROR enviando a {c['email']}: {e}")


def main():
    notificar_visitas_manana()
    notificar_canceladas_reagendar()
    print(f"\n[{datetime.now()}] Proceso completado.")


if __name__ == "__main__":
    main()
