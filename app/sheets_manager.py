import gspread
from google.oauth2.service_account import Credentials
import time

# --- CONFIGURACIÓN ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# --- ¡PON AQUÍ EL ID DE TU HOJA DE CÁLCULO REAL! ---
# (El que obtienes de la URL, por ejemplo: "19qQFrJiA0K5KVS3fF6bleIv8pDNOuvcIX0HjIX6O5bY")
SHEET_ID = "1WxuhyGTskTmYBcMmFd9aN8JN0tfBV7wtWF78K7JL-cw" 

# Asegúrate de que tu archivo de credenciales se llame 'credentials.json'
CREDS_FILE = "credentials.json" 

# --- Carga de credenciales (se hace una sola vez al iniciar la app) ---
try:
    CREDS = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    CLIENT = gspread.authorize(CREDS)
    print("-> Cliente de Google Sheets autenticado correctamente al iniciar.")
except FileNotFoundError:
    print(f"ERROR CRÍTICO: No se encontró el archivo de credenciales '{CREDS_FILE}'.")
    CLIENT = None
except Exception as e:
    print(f"ERROR CRÍTICO al cargar las credenciales: {e}")
    CLIENT = None

# --- Implementación del caché ---
CACHE = {
    'datos': None,
    'timestamp': 0
}
CACHE_DURATION_SECONDS = 300  # 5 minutos

def obtener_datos_certificados():
    """
    Se conecta a la hoja de cálculo por su ID y devuelve todos los registros.
    Este código es robusto y maneja columnas de encabezado vacías.
    """
    if not CLIENT:
        print("No se puede obtener datos porque el cliente de la API no se inicializó correctamente.")
        return []

    current_time = time.time()
    
    # 1. Comprueba si el caché es válido
    if CACHE['datos'] and (current_time - CACHE['timestamp'] < CACHE_DURATION_SECONDS):
        print("Cargando datos de certificados desde el CACHÉ.")
        return CACHE['datos']

    # 2. Si el caché no es válido, carga los datos desde Google Sheets
    print("Cargando datos de certificados desde la API de Google Sheets.")
    try:
        # Abre el archivo por su ID único
        spreadsheet = CLIENT.open_by_key(SHEET_ID)
        
        # Lee la pestaña (worksheet) por su nombre exacto
        worksheet = spreadsheet.worksheet("Form Responses 1")
        
        # Obtiene todos los valores como una lista de listas
        all_values = worksheet.get_all_values()

        if not all_values:
            return [] # La hoja está completamente vacía

        # La primera fila son los encabezados
        headers = all_values[0]
        # El resto son las filas de datos
        data_rows = all_values[1:]

        # Crea la lista de diccionarios manualmente para evitar errores
        datos = []
        for row in data_rows:
            record = {}
            # Itera sobre los encabezados para construir cada registro
            for i, header in enumerate(headers):
                # ¡Clave! Solo añade la columna si el encabezado no está vacío
                if header and header.strip(): 
                    # Asegura que la fila tenga un valor para este índice
                    if i < len(row):
                        record[header] = row[i]
                    else:
                        record[header] = "" # Si la fila es más corta que los headers
            
            # Solo añade el registro si tiene algún dato útil
            if any(str(val).strip() for val in record.values()):
                datos.append(record)

        # Actualiza el caché
        CACHE['datos'] = datos
        CACHE['timestamp'] = current_time
        
        return datos

    except gspread.exceptions.WorksheetNotFound:
        print(f"ERROR: No se encontró la pestaña 'Form Responses 1' en tu archivo. Verifica el nombre.")
        return []
    except Exception as e:
        print(f"Ocurrió un error inesperado al leer la API: {e}")
        return []