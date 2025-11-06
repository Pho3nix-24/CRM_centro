"""
Módulo de Gestión de Base de Datos
---------------------------------
Contiene las funciones para interactuar con la base de datos MySQL.
La conexión, las operaciones CRUD para clientes y pagos, la generación de
reportes y el registro de auditoría.
"""

# --- Importaciones ---
import io
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# --- Configuración ---
DB_CONFIG = {
    "host": "localhost",
    "database": "CRM_CENTRO_db",
    "user": "root",
    "password": "pho3nix241236!",
}

# --- Manejo de Conexión ---
def get_connection():
    """
    Establece y devuelve una nueva conexión a la base de datos.
    
    Returns:
        mysql.connector.connection: Objeto de conexión a la base de datos.
    """
    return mysql.connector.connect(**DB_CONFIG)

# --- Funciones de Estadísticas para el Dashboard ---

def obtener_estadisticas_dashboard():
    """
    Obtiene estadísticas clave para mostrar en el panel principal.
    
    Returns:
        dict: Un diccionario con registros_hoy, ingresos_hoy, e ingresos_mes.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        stats = {
            'registros_hoy': 0,
            'ingresos_hoy': 0.0,
            'ingresos_mes': 0.0
        }

        # 1. Conteo de registros de hoy
        sql_hoy = "SELECT COUNT(*) as total FROM pagos WHERE DATE(fecha) = CURDATE()"
        cursor.execute(sql_hoy)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['registros_hoy'] = resultado['total']

        # 2. Suma de ingresos de hoy
        sql_ingresos_hoy = "SELECT SUM(cuota) as total FROM pagos WHERE DATE(fecha) = CURDATE()"
        cursor.execute(sql_ingresos_hoy)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['ingresos_hoy'] = resultado['total']

        # 3. Suma de ingresos del mes actual
        sql_ingresos_mes = """SELECT SUM(cuota) as total FROM pagos 
                              WHERE YEAR(fecha) = YEAR(CURDATE()) AND MONTH(fecha) = MONTH(CURDATE())"""
        cursor.execute(sql_ingresos_mes)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['ingresos_mes'] = resultado['total']
            
        return stats
    except Error as e:
        print(f"ERROR EN BD (obtener_estadisticas_dashboard): {e}")
        # Devuelve stats en cero si hay un error
        return {'registros_hoy': 0, 'ingresos_hoy': 0, 'ingresos_mes': 0}
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def obtener_ultimos_pagos(limit=5):
    """
    Obtiene los últimos registros de pago para mostrar en el dashboard.
    
    Args:
        limit (int): El número de registros a obtener.
    
    Returns:
        list: Una lista de tuplas con los últimos pagos.
    """
    # Esta función es muy similar a buscar_pagos_completos, pero sin búsqueda
    # y con un límite. Podemos reutilizar esa lógica.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                   p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                   c.dni, c.correo, c.genero, p.asesor
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            ORDER BY p.id DESC
            LIMIT %s
        """
        cursor.execute(sql, (limit,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_ultimos_pagos): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Gestión de Clientes y Pagos (CRUD) ---

#Función para buscar o crear cliente
def buscar_o_crear_cliente(data):
    """
    Busca un cliente por DNI. 
    - Si existe y está inactivo o potencial, lo activa.
    - Si no existe, lo crea como 'activo' (porque está haciendo un pago).
    Devuelve el ID del cliente.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        sql_buscar = "SELECT id, estado FROM clientes WHERE dni = %s"
        cursor.execute(sql_buscar, (data.get('dni'),))
        cliente_existente = cursor.fetchone()
        
        if cliente_existente:
            cliente_id = cliente_existente['id']
            
            # Si el cliente existía pero no estaba activo, lo activamos
            if cliente_existente['estado'] != 'activo':
                print(f"Actualizando estado del cliente a 'activo' con ID: {cliente_id}")
                update_cursor = conn.cursor()
                # ¡AQUÍ ACTUALIZAMOS TODOS SUS DATOS POR SI HAN CAMBIADO!
                sql_actualizar = """
                    UPDATE clientes SET 
                    nombre = %s, correo = %s, celular = %s, genero = %s, estado = 'activo'
                    WHERE id = %s
                """
                datos_actualizar = (
                    data.get('cliente'), data.get('correo'), data.get('celular'),
                    data.get('genero'), cliente_id
                )
                update_cursor.execute(sql_actualizar, datos_actualizar)
                conn.commit()
                update_cursor.close()
            
            return cliente_id
        else:
            # Si el cliente no existe, se crea directamente como 'activo'
            sql_crear = """INSERT INTO clientes (nombre, dni, correo, celular, genero, estado)
                            VALUES (%s, %s, %s, %s, %s, 'activo')"""
            cliente_tuple = (
                data.get('cliente'), data.get('dni'), data.get('correo'),
                data.get('celular'), data.get('genero')
            )
            cursor.execute(sql_crear, cliente_tuple)
            conn.commit()
            return cursor.lastrowid
            
    except Error as e:
        print(f"ERROR EN BD (buscar_o_crear_cliente): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
#Función para crear pago
def crear_pago(cliente_id, data):
    """
    Crea un nuevo registro en la tabla `pagos`.

    Args:
        cliente_id (int): El ID del cliente al que se asocia el pago.
        data (dict): Diccionario con los datos del formulario del pago.

    Returns:
        int: El ID del nuevo pago creado.

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """INSERT INTO pagos (cliente_id, fecha, cuota, tipo_de_cuota, banco, destino, 
                numero_operacion, especialidad, modalidad, asesor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        pago_tuple = (
            cliente_id, data.get('fecha'), data.get('cuota'), data.get('tipo_cuota'),
            data.get('banco'), data.get('destino'), data.get('numero_operacion'),
            data.get('especialidad'), data.get('modalidad'), data.get('asesor')
        )
        cursor.execute(sql, pago_tuple)
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"ERROR EN BD (crear_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para buscar pagos con datos de cliente
def buscar_pagos_completos(query):
    """Busca pagos y une la información del cliente, incluyendo el ID del cliente."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # ¡AÑADIMOS c.id AL SELECT!
        sql = """
            SELECT 
                p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                c.dni, c.correo, c.genero, p.asesor, c.id as cliente_id
            FROM pagos p
            JOIN clientes c ON p.cliente_id = c.id
            WHERE c.dni LIKE %s OR c.nombre LIKE %s
            ORDER BY p.id DESC
        """
        search_term = f"%{query}%"
        cursor.execute(sql, (search_term, search_term))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (buscar_pagos_completos): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para obtener pago por ID
def obtener_pago_por_id(pago_id):
    """
    Obtiene un registro de pago y sus datos de cliente asociados por el ID del pago.

    Args:
        pago_id (int): El ID del pago a buscar.

    Returns:
        dict: Un diccionario con los datos del pago y del cliente, o None si no se encuentra o hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT p.*, c.nombre as cliente, c.dni, c.correo, c.celular, c.genero
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            WHERE p.id = %s
        """
        cursor.execute(sql, (pago_id,))
        return cursor.fetchone()
    except Error as e:
        print(f"ERROR EN BD (obtener_pago_por_id): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para actualizar pago
def actualizar_pago(pago_id, form_data):
    """
    Actualiza los campos de un registro de pago existente.

    Args:
        pago_id (int): El ID del pago a actualizar.
        form_data (dict): Diccionario con los nuevos datos del formulario.

    Returns:
        int: El número de filas afectadas (normalmente 1).

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        campos_pago = [
            "fecha", "cuota", "tipo_de_cuota", "banco", "destino",
            "numero_operacion", "especialidad", "modalidad", "asesor"
        ]
        set_clause = ", ".join([f"{campo} = %s" for campo in campos_pago])
        sql = f"UPDATE pagos SET {set_clause} WHERE id = %s"
        
        form_data_copy = form_data.copy()
        if 'num_operacion' in form_data_copy:
            form_data_copy['numero_operacion'] = form_data_copy.pop('num_operacion')

        valores = [form_data_copy.get(campo) for campo in campos_pago] + [pago_id]
        cursor.execute(sql, tuple(valores))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (actualizar_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para eliminar pago
def eliminar_pago(pago_id):
    """
    Elimina un registro de pago de la base de datos.

    Args:
        pago_id (int): El ID del pago a eliminar.

    Returns:
        int: El número de filas afectadas (normalmente 1).

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM pagos WHERE id = %s"
        cursor.execute(sql, (pago_id,))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (eliminar_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Reportes y Exportación ---

#Función para generar reporte de asesores
def generar_reporte_asesores_db(start_date_str=None, end_date_str=None):
    """
    Genera un reporte de ventas agrupado por asesor con filtro de fecha.

    Args:
        start_date_str (str, optional): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date_str (str, optional): Fecha de fin en formato 'YYYY-MM-DD'.

    Returns:
        list: Lista de diccionarios con los resultados del reporte.
            Devuelve una lista vacía si hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """SELECT asesor, COUNT(*) as registros_asesor, SUM(cuota) as total_asesor 
                FROM pagos"""
        params, where_clauses = [], []
        if start_date_str:
            where_clauses.append("fecha >= %s")
            params.append(start_date_str)
        if end_date_str:
            where_clauses.append("fecha <= %s")
            params.append(end_date_str)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " GROUP BY asesor ORDER BY total_asesor DESC"
        cursor.execute(sql, tuple(params))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (generar_reporte_asesores_db): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para generar Excel dinámico
def generar_excel_dinamico(headers):
    """
    Crea un archivo Excel en memoria con todos los registros,
    manejando fechas inválidas de forma robusta.
    """
    conn = None
    try:
        conn = get_connection()
        sql = """
            SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                   p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                   c.dni, c.correo, c.genero, p.asesor
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            ORDER BY p.id ASC
        """
        df = pd.read_sql(sql, conn)
        df.columns = ["ID"] + headers
        
        # --- LÓGICA DE FECHA MEJORADA ---
        # 1. Convierte las fechas. Si una es inválida, la convierte en 'NaT' (Not a Time)
        df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
        
        # 2. Formatea solo las fechas que son válidas.
        # Las fechas inválidas (NaT) se convertirán en un valor nulo (NaN).
        df['FECHA'] = df['FECHA'].dt.strftime('%Y-%m-%d')

        # 3. Reemplaza cualquier valor nulo restante con un string vacío para un Excel limpio.
        df.fillna('', inplace=True)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Registros')
        output.seek(0)
        return output
    except Error as e:
        print(f"ERROR EN BD (generar_excel_dinamico): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()
            
            
# --- Módulo de Auditoría ---

#Función para registrar auditoría
def registrar_auditoria(usuario, accion, ip, tabla=None, reg_id=None, detalles=None):
    """
    Inserta un nuevo registro en la tabla de auditoría.
    Esta función no propaga errores para no detener la operación principal.

    Args:
        usuario (str): Nombre del usuario que realiza la acción.
        accion (str): Descripción de la acción (ej: 'CREAR_PAGO').
        ip (str): Dirección IP de origen de la petición.
        tabla (str, optional): Nombre de la tabla afectada.
        reg_id (int, optional): ID del registro afectado.
        detalles (str, optional): Información adicional sobre el evento.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """INSERT INTO auditoria_accesos 
                (timestamp, usuario_app, accion, tabla_afectada, registro_id, detalles, ip_origen)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        datos = (datetime.now(), usuario, accion, tabla, reg_id, detalles, ip)
        cursor.execute(sql, datos)
        conn.commit()
    except Error as e:
        print(f"ERROR CRÍTICO AL REGISTRAR AUDITORÍA: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para leer log de auditoría            
def leer_log_auditoria():
    """
    Lee todos los registros de la tabla de auditoría ordenados por fecha.

    Returns:
        list: Lista de diccionarios con los logs de auditoría.
            Devuelve una lista vacía si hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM auditoria_accesos ORDER BY timestamp DESC"
        cursor.execute(sql)
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (leer_log_auditoria): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
            
# Función para cambiar estado de cliente            
def cambiar_estado_cliente(cliente_id, nuevo_estado):
    """
    Cambia el estado de un cliente a 'activo' o 'inactivo'.

    Args:
        cliente_id (int): El ID del cliente a modificar.
        nuevo_estado (str): El nuevo estado ('activo' o 'inactivo').
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "UPDATE clientes SET estado = %s WHERE id = %s"
        cursor.execute(sql, (nuevo_estado, cliente_id))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (cambiar_estado_cliente): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Función para obtener cliente por ID            
def obtener_cliente_por_id(cliente_id):
    """Obtiene los datos de un único cliente por su ID."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM clientes WHERE id = %s"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchone()
    except Error as e:
        print(f"ERROR EN BD (obtener_cliente_por_id): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para obtener pagos por cliente
def obtener_pagos_por_cliente(cliente_id):
    """Obtiene todos los pagos asociados a un ID de cliente."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM pagos WHERE cliente_id = %s ORDER BY fecha DESC"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_pagos_por_cliente): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Integración con Google Sheets ---          
def conectar_a_gsheets():
    """
    Establece una conexión autenticada con la API de Google Sheets.
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Busca el archivo JSON en la carpeta principal del proyecto
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # ¡¡¡IMPORTANTE!!! CAMBIA ESTE NOMBRE POR EL DE TU ARCHIVO JSON
        creds_file = os.path.join(base_dir, 'centro-web-app-474322-d1f6bd19a50f.json') 

        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"ERROR al conectar con Google API: {e}")
        raise e

# Función para obtener datos de una hoja específica
def obtener_datos_sheet(nombre_sheet):
    """
    Obtiene todos los registros de una hoja de cálculo de Google específica.
    """
    try:
        client = conectar_a_gsheets()
        sheet = client.open(nombre_sheet).sheet1
        registros = sheet.get_all_records()
        registros_limpios = [row for row in registros if any(str(val).strip() for val in row.values())]
        return registros_limpios
    except Exception as e:
        print(f"ERROR al leer Google Sheet '{nombre_sheet}': {e}")
        raise e

# Función para registrar potencial (lead)    
def registrar_potencial(data, asesor):
    """
    Crea un nuevo cliente con estado 'potencial' (un lead).
    Evita duplicados por DNI o correo si ya existen.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Verificar si ya existe por DNI o correo
        sql_buscar = "SELECT id FROM clientes WHERE dni = %s OR correo = %s"
        cursor.execute(sql_buscar, (data.get('dni'), data.get('correo')))
        if cursor.fetchone():
            # Si ya existe, no hacemos nada y devolvemos None o un ID existente.
            # Podrías mejorar esto para actualizar el interés del cliente existente.
            return None 

        # 2. Si no existe, lo creamos como potencial
        sql_crear = """
            INSERT INTO clientes 
            (nombre, dni, correo, celular, genero, estado, curso_interes, asesor_asignado, fecha_contacto)
            VALUES (%s, %s, %s, %s, %s, 'potencial', %s, %s, %s)
        """
        cliente_tuple = (
            data.get('cliente'), data.get('dni'), data.get('correo'),
            data.get('celular'), data.get('genero'), data.get('curso_interes'),
            asesor, datetime.now()
        )
        cursor.execute(sql_crear, cliente_tuple)
        conn.commit()
        return cursor.lastrowid
            
    except Error as e:
        print(f"ERROR EN BD (registrar_potencial): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# --- Funciones para el Historial de Interacciones ---

# Función para registrar interacción
def registrar_interaccion(cliente_id, asesor_nombre, tipo, notas):
    """Inserta un nuevo registro en la tabla de interacciones."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO interacciones 
            (cliente_id, asesor_nombre, fecha_interaccion, tipo_interaccion, notas)
            VALUES (%s, %s, %s, %s, %s)
        """
        datos = (cliente_id, asesor_nombre, datetime.now(), tipo, notas)
        cursor.execute(sql, datos)
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"ERROR EN BD (registrar_interaccion): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Función para obtener interacciones por cliente
def obtener_interacciones_por_cliente(cliente_id):
    """Obtiene todas las interacciones de un cliente, ordenadas por fecha."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM interacciones WHERE cliente_id = %s ORDER BY fecha_interaccion DESC"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_interacciones_por_cliente): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# --- Funciones para la Gestión de Oportunidades ---

def crear_oportunidad_si_no_existe(cliente_id, asesor, curso):
    """Crea una oportunidad para un nuevo lead si no tiene una ya."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Usamos INSERT IGNORE para que no falle si la oportunidad ya existe (por la UNIQUE KEY)
        sql = """
            INSERT IGNORE INTO oportunidades 
            (cliente_id, asesor_asignado, curso_interes, estado_oportunidad, fecha_creacion, ultima_actualizacion)
            VALUES (%s, %s, %s, 'Nuevo', %s, %s)
        """
        now = datetime.now()
        cursor.execute(sql, (cliente_id, asesor, curso, now, now))
        conn.commit()
    except Error as e:
        print(f"ERROR EN BD (crear_oportunidad_si_no_existe): {e}")
        # No relanzamos el error para no detener el proceso principal de registro de lead
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Función para obtener oportunidades por asesor
def obtener_oportunidades_por_asesor(asesor_nombre):
    """Obtiene todas las oportunidades de un asesor, uniendo datos del cliente."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT o.*, c.nombre as cliente_nombre, c.celular as cliente_celular
            FROM oportunidades o
            JOIN clientes c ON o.cliente_id = c.id
            WHERE o.asesor_asignado = %s AND o.estado_oportunidad NOT IN ('Ganada', 'Perdida')
            ORDER BY o.ultima_actualizacion DESC
        """
        cursor.execute(sql, (asesor_nombre,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_oportunidades_por_asesor): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para mover oportunidad
def mover_oportunidad(oportunidad_id, nuevo_estado):
    """Actualiza el estado de una oportunidad cuando se mueve en el tablero."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "UPDATE oportunidades SET estado_oportunidad = %s, ultima_actualizacion = %s WHERE id = %s"
        cursor.execute(sql, (nuevo_estado, datetime.now(), oportunidad_id))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (mover_oportunidad): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Funciones para la Gestión de Etiquetas ---
def obtener_o_crear_etiqueta_id(nombre_etiqueta):
    """Busca una etiqueta por nombre. Si no existe, la crea. Devuelve el ID."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Buscar si ya existe (insensible a mayúsculas/minúsculas)
        cursor.execute("SELECT id FROM etiquetas WHERE LOWER(nombre) = LOWER(%s)", (nombre_etiqueta,))
        etiqueta = cursor.fetchone()
        
        if etiqueta:
            return etiqueta['id']
        else:
            # 2. Si no existe, la crea y devuelve el nuevo ID
            cursor.execute("INSERT INTO etiquetas (nombre) VALUES (%s)", (nombre_etiqueta,))
            conn.commit()
            return cursor.lastrowid
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para vincular etiqueta a cliente
def anadir_etiqueta_a_cliente(cliente_id, etiqueta_id):
    """Vincula una etiqueta a un cliente en la tabla intermedia."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # INSERT IGNORE para no fallar si el vínculo ya existe
        cursor.execute("INSERT IGNORE INTO cliente_etiquetas (cliente_id, etiqueta_id) VALUES (%s, %s)", (cliente_id, etiqueta_id))
        conn.commit()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Función para quitar etiqueta a cliente
def quitar_etiqueta_a_cliente(cliente_id, etiqueta_id):
    """Elimina el vínculo entre un cliente y una etiqueta."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cliente_etiquetas WHERE cliente_id = %s AND etiqueta_id = %s", (cliente_id, etiqueta_id))
        conn.commit()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para obtener etiquetas por cliente
def obtener_etiquetas_por_cliente(cliente_id):
    """Obtiene todas las etiquetas de un cliente específico."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT e.id, e.nombre 
            FROM etiquetas e
            JOIN cliente_etiquetas ce ON e.id = ce.etiqueta_id
            WHERE ce.cliente_id = %s
            ORDER BY e.nombre
        """
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            

def buscar_leads(query):
    """Busca clientes que son potenciales o inactivos."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT id, nombre, celular, dni, correo, genero, estado, curso_interes, asesor_asignado, fecha_contacto
            FROM clientes
            WHERE (estado = 'potencial' OR estado = 'inactivo') 
              AND (nombre LIKE %s OR dni LIKE %s OR correo LIKE %s)
            ORDER BY fecha_contacto DESC
        """
        search_term = f"%{query}%"
        cursor.execute(sql, (search_term, search_term, search_term))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (buscar_leads): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def generar_excel_leads():
    """Crea un archivo Excel en memoria con todos los leads."""
    conn = None
    try:
        conn = get_connection()
        sql = """
            SELECT nombre, celular, dni, correo, genero, estado, curso_interes, asesor_asignado, fecha_contacto
            FROM clientes
            WHERE estado = 'potencial' OR estado = 'inactivo'
            ORDER BY fecha_contacto DESC
        """
        df = pd.read_sql(sql, conn)
        # Renombramos las columnas para que se vean bien en el Excel
        df.columns = ["Nombre", "Celular", "DNI", "Correo", "Género", "Estado", 
                    "Curso de Interés", "Asesor Asignado", "Fecha de Contacto"]
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Leads')
        output.seek(0)
        return output
    except Error as e:
        print(f"ERROR EN BD (generar_excel_leads): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()
            
# Función para eliminar lead por ID
def eliminar_lead_por_id(cliente_id):
    """
    Elimina un cliente y todos sus datos asociados de forma permanente.
    Esto es una eliminación física (de raíz), no lógica.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Gracias a ON DELETE CASCADE, todos los registros en tablas relacionadas se eliminarán.
        sql = "DELETE FROM clientes WHERE id = %s"
        cursor.execute(sql, (cliente_id,))
        conn.commit()
        return cursor.rowcount # Devuelve el número de filas eliminadas (debería ser 1)
    except Error as e:
        print(f"ERROR EN BD (eliminar_lead_por_id): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Archivo: app/database_manager.py

# --- Funciones para calcular Indicadores ---

def calcular_tiempo_gestion_promedio():
    """Calcula el tiempo promedio (en días) desde creación hasta cierre (Ganada/Perdida)."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        # Usamos DATEDIFF para obtener la diferencia en días
        sql = """
            SELECT AVG(DATEDIFF(fecha_cierre, fecha_creacion)) as tiempo_promedio_dias
            FROM oportunidades
            WHERE estado_oportunidad IN ('Ganada', 'Perdida') AND fecha_cierre IS NOT NULL
        """
        cursor.execute(sql)
        resultado = cursor.fetchone()
        return resultado['tiempo_promedio_dias'] if resultado and resultado['tiempo_promedio_dias'] is not None else 0
    except Error as e:
        print(f"ERROR EN BD (calcular_tiempo_gestion_promedio): {e}")
        return 0 # Devolver 0 o None si hay error
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def calcular_cumplimiento_primer_contacto(horas_limite=24):
    """Calcula el % de leads contactados dentro de las horas_limite desde su registro."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        # Obtenemos la fecha de registro del cliente y la fecha de la PRIMERA interacción
        sql = """
            SELECT
                c.id,
                c.fecha_contacto as fecha_registro,
                MIN(i.fecha_interaccion) as fecha_primer_contacto
            FROM clientes c
            LEFT JOIN interacciones i ON c.id = i.cliente_id
            WHERE c.estado IN ('potencial', 'activo') -- Consideramos ambos
            GROUP BY c.id, c.fecha_contacto
        """
        cursor.execute(sql)
        leads = cursor.fetchall()

        if not leads:
            return 0.0

        cumplen = 0
        total_evaluados = 0

        for lead in leads:
            # Solo evaluamos si hay fecha de registro
            if lead['fecha_registro']:
                 total_evaluados += 1
                 # Si hay una primera interacción Y ocurrió dentro del límite de tiempo
                 if lead['fecha_primer_contacto'] and \
                    (lead['fecha_primer_contacto'] - lead['fecha_registro']).total_seconds() <= horas_limite * 3600:
                     cumplen += 1

        return (cumplen / total_evaluados) * 100 if total_evaluados > 0 else 0.0

    except Error as e:
        print(f"ERROR EN BD (calcular_cumplimiento_primer_contacto): {e}")
        return 0.0
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def calcular_porcentaje_leads_con_interacciones():
    """Calcula el % de clientes (potenciales o activos) que tienen al menos una interacción registrada."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Contamos cuántos clientes tienen al menos una interacción
        sql_interacciones = """
            SELECT COUNT(DISTINCT cliente_id) as con_interaccion
            FROM interacciones
        """
        cursor.execute(sql_interacciones)
        resultado_interacciones = cursor.fetchone()
        con_interaccion = resultado_interacciones['con_interaccion'] if resultado_interacciones else 0


        # Contamos el total de clientes relevantes (potenciales + activos)
        sql_total = "SELECT COUNT(*) as total FROM clientes WHERE estado IN ('potencial', 'activo')"
        cursor.execute(sql_total)
        resultado_total = cursor.fetchone()
        total_clientes = resultado_total['total'] if resultado_total else 0

        return (con_interaccion / total_clientes) * 100 if total_clientes > 0 else 0.0

    except Error as e:
        print(f"ERROR EN BD (calcular_porcentaje_leads_con_interacciones): {e}")
        return 0.0
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# --- Funciones para la Gestión de Tareas ---

def crear_tarea(asesor, descripcion, cliente_id=None, fecha_vencimiento=None):
    """Crea una nueva tarea."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO tareas
            (asesor_asignado, descripcion, cliente_id, fecha_vencimiento, estado, fecha_creacion)
            VALUES (%s, %s, %s, %s, 'Pendiente', %s)
        """
        # Convertir fecha_vencimiento a objeto date si es string
        fecha_vencimiento_obj = None
        if isinstance(fecha_vencimiento, str) and fecha_vencimiento:
            try:
                fecha_vencimiento_obj = datetime.strptime(fecha_vencimiento, '%Y-%m-%d').date()
            except ValueError:
                fecha_vencimiento_obj = None # Ignorar si el formato es inválido

        datos = (asesor, descripcion, cliente_id, fecha_vencimiento_obj, datetime.now())
        cursor.execute(sql, datos)
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"ERROR EN BD (crear_tarea): {e}")
        raise e
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def obtener_tareas_por_cliente(cliente_id):
    """Obtiene las tareas pendientes de un cliente específico."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT * FROM tareas
            WHERE cliente_id = %s AND estado = 'Pendiente'
            ORDER BY fecha_vencimiento ASC, fecha_creacion ASC
        """
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_tareas_por_cliente): {e}")
        return []
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def obtener_tareas_pendientes_asesor(asesor_nombre):
     """Obtiene todas las tareas pendientes de un asesor."""
     conn = None
     cursor = None # Inicializar cursor
     try:
         conn = get_connection()
         cursor = conn.cursor(dictionary=True)
         sql = """
             SELECT t.*, c.nombre as nombre_cliente
             FROM tareas t
             LEFT JOIN clientes c ON t.cliente_id = c.id
             WHERE t.asesor_asignado = %s AND t.estado = 'Pendiente'
             ORDER BY t.fecha_vencimiento ASC, t.fecha_creacion ASC
         """
         cursor.execute(sql, (asesor_nombre,))
         return cursor.fetchall()
     except Error as e:
         print(f"ERROR EN BD (obtener_tareas_pendientes_asesor): {e}")
         return []
     finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def marcar_tarea_completada(tarea_id):
    """Marca una tarea como completada."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "UPDATE tareas SET estado = 'Completada', fecha_completada = %s WHERE id = %s"
        cursor.execute(sql, (datetime.now(), tarea_id))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (marcar_tarea_completada): {e}")
        raise e
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def calcular_porcentaje_tareas_completadas(asesor_nombre=None):
    """Calcula el % de tareas completadas (opcionalmente filtrado por asesor)."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        base_sql = "SELECT COUNT(*) as count FROM tareas"
        params = []

        # Filtro opcional por asesor
        where_clause = ""
        if asesor_nombre:
            where_clause = " WHERE asesor_asignado = %s"
            params.append(asesor_nombre)

        # Contar completadas
        sql_completadas = base_sql + where_clause + (" AND " if where_clause else " WHERE ") + "estado = 'Completada'"
        cursor.execute(sql_completadas, tuple(params))
        resultado_completadas = cursor.fetchone()
        completadas = resultado_completadas['count'] if resultado_completadas else 0

        # Contar total
        sql_total = base_sql + where_clause
        cursor.execute(sql_total, tuple(params))
        resultado_total = cursor.fetchone()
        total = resultado_total['count'] if resultado_total else 0

        return (completadas / total) * 100 if total > 0 else 0.0

    except Error as e:
        print(f"ERROR EN BD (calcular_porcentaje_tareas_completadas): {e}")
        return 0.0
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# --- Función para eliminar lead (Asegúrate de que también tenga el finally) ---
def eliminar_lead_por_id(cliente_id):
    """Elimina un cliente y todos sus datos asociados de forma permanente."""
    conn = None
    cursor = None # Inicializar cursor
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM clientes WHERE id = %s"
        cursor.execute(sql, (cliente_id,))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (eliminar_lead_por_id): {e}")
        raise e
    finally:
        # ✅ Bloque finally completo
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()