# Apps Script — agregar columna "usuario" + timestamp

El Apps Script en `https://script.google.com/macros/s/AKfycbz0mAvpiwFYUMhZWwd6JxbUHVVh6EH-d8eiRqFRXmjIJaFh6xFPmz4-tWk5I8Ww-Wpe/exec`
es el que escribe al Sheet cuando alguien clica "Cliente confirmó".

Para que registre el usuario, tienes que actualizarlo así:

## Paso 1 — Editar las cabeceras del Sheet

Abre el Sheet `1rxvkkdcCnv6eoyRBMvGgjbiP2tiITE_mO7wpZDpoCOw` y agrega 2 columnas nuevas:

| nid | fecha | estado | **usuario** | **timestamp** |

(`usuario` y `timestamp` son nuevas)

## Paso 2 — Editar el Apps Script

1. Abre https://script.google.com/home (con `sofianoguera@habi.co` o la cuenta dueña del script)
2. Busca el script de "Estados Visitas" (o como se llame; la URL termina en `AKfycbz0mAvp...`)
3. Reemplaza el código `doPost` por esto:

```javascript
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const sheet = SpreadsheetApp
      .openById('1rxvkkdcCnv6eoyRBMvGgjbiP2tiITE_mO7wpZDpoCOw')
      .getActiveSheet();

    sheet.appendRow([
      data.nid || '',
      data.fecha || '',
      data.estado || '',
      data.usuario || '',           // ← nueva columna
      new Date()                    // ← timestamp del cambio
    ]);

    return ContentService
      .createTextOutput(JSON.stringify({ ok: true }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: String(err) }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
```

4. Click en **Implementar → Gestionar implementaciones**
5. Click en el ícono de lápiz (editar la implementación existente)
6. Cambia "Versión" → "Nueva versión"
7. Click "Implementar"

⚠️ **No cambies la URL.** La URL del script debe seguir siendo la misma para que la app Flask siga apuntándole.

## Paso 3 — Verificar

1. Abre la página de visitas mañana
2. Te va a pedir tu nombre (solo la primera vez por dispositivo)
3. Clica el botón verde "✅ Cliente confirmó" en cualquier visita
4. Abre el Sheet — la nueva fila debe tener tu nombre en la columna `usuario`

## Backwards-compat

Las filas viejas (sin `usuario`) siguen funcionando — el valor queda vacío para esas. La app Flask al leer las ignora si están vacías.

## Cambiar mi nombre

Si me equivoqué al escribir mi nombre o quiero que aparezca otro:
- En la página, arriba a la derecha junto a la fecha, hay un link "cambiar"
- Click → te pregunta de nuevo
- Tu nuevo nombre se guarda en este dispositivo y se usa para los próximos clicks
