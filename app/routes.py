"""
Módulo de Rutas de Flask
-----------------------
Este archivo define todas las URLs de la aplicación, la lógica de negocio,
y el control de acceso basado en roles y límite de intentos de login.
"""

# --- Importaciones ---
import os
from datetime import datetime
from functools import wraps
from flask import (
    render_template, request, redirect, url_for,
    flash, session, send_file, send_from_directory, jsonify
)
from app import app
from app import database_manager as db
from mysql.connector import IntegrityError, Error as DB_Error
from werkzeug.security import check_password_hash
from app import sheets_manager


# --- Configuración y Constantes ---
RECORDS_PER_PAGE = 5
USERS = {
    'admin':      {'password_hash': 'scrypt:32768:8:1$WFi0YBN2qCDwBgWJ$be6ca3584230b85b3b4fdbba30a11bf25d0b30fd525ac6c1a83708edf7401d9300ab2e3388eee7835ce47534dbfcdb4458b538db90ec960ef3852f793c869b47', 'full_name': 'Administrador',   'role': 'admin'},
    'lud_rojas':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Lud Rojas',       'role': 'equipo'},
    'ruth_lecca': {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Ruth Lecca',      'role': 'equipo'},
    'rafa_diaz':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Rafael Díaz',     'role': 'atencion_cliente'}
}
HEADERS = ["FECHA", "CLIENTE", "CELULAR", "ESPECIALIDAD", "MODALIDAD", "CUOTA", "TIPO DE CUOTA", "BANCO", "DESTINO", "N° OPERACIÓN", "DNI", "CORREO", "GÉNERO", "ASESOR"]
FIELDS = ["fecha", "cliente", "celular", "especialidad", "modalidad", "cuota", "tipo_de_cuota", "banco", "destino", "numero_operacion", "dni", "correo", "genero", "asesor"]
RECORDS_PER_PAGE_SHEETS = 20
failed_logins = {}
LOGIN_ATTEMPT_LIMIT = 5
LOCKOUT_TIME_SECONDS = 300

# --- Decoradores ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('Debes iniciar sesión para ver esta página.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# --- Rutas de Sesión y Menú ---
@app.route("/login", methods=["GET", "POST"])
def login():
    ip_usuario = request.remote_addr
    if ip_usuario in failed_logins:
        user_failures = failed_logins.get(ip_usuario, {'attempts': 0, 'last_attempt_time': datetime.min})
        elapsed_time = (datetime.now() - user_failures['last_attempt_time']).total_seconds()
        if user_failures['attempts'] >= LOGIN_ATTEMPT_LIMIT and elapsed_time < LOCKOUT_TIME_SECONDS:
            tiempo_restante = round((LOCKOUT_TIME_SECONDS - elapsed_time) / 60)
            flash(f"Demasiados intentos fallidos. Por favor, espera {tiempo_restante} minutos.", "error")
            return render_template("login.html")
        if elapsed_time >= LOCKOUT_TIME_SECONDS:
            failed_logins.pop(ip_usuario, None)
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user_data = USERS.get(username)
        if user_data and check_password_hash(user_data['password_hash'], password):
            failed_logins.pop(ip_usuario, None)
            session['logged_in'] = True
            session['full_name'] = user_data['full_name']
            session['username'] = username
            session['role'] = user_data['role']
            db.registrar_auditoria(user_data['full_name'], "INICIO_SESION_EXITOSO", ip_usuario)
            flash("Has iniciado sesión correctamente.", "success")
            return redirect(url_for("menu"))
        else:
            if ip_usuario not in failed_logins:
                failed_logins[ip_usuario] = {'attempts': 0, 'last_attempt_time': datetime.now()}
            failed_logins[ip_usuario]['attempts'] += 1
            failed_logins[ip_usuario]['last_attempt_time'] = datetime.now()
            intentos_restantes = LOGIN_ATTEMPT_LIMIT - failed_logins[ip_usuario]['attempts']
            db.registrar_auditoria(username, "INICIO_SESION_FALLIDO", ip_usuario)
            if intentos_restantes > 0:
                flash(f"Credenciales incorrectas. Te quedan {intentos_restantes} intentos.", "error")
            else:
                flash("Demasiados intentos fallidos. Tu acceso ha sido bloqueado por 5 minutos.", "error")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    db.registrar_auditoria(session.get('full_name', 'desconocido'), "CIERRE_SESION", request.remote_addr)
    session.clear()
    flash("Has cerrado la sesión.", "success")
    return redirect(url_for('login'))

@app.route("/menu")
@login_required
def menu():
    return render_template("menu.html")

# --- Rutas de la Sección VENTAS ---
@app.route("/")
def index():
    return redirect(url_for('dashboard'))

@app.route("/dashboard")
@login_required
def dashboard():
    if session.get('role') == 'atencion_cliente':
        return redirect(url_for('consulta'))
    try:
        estadisticas = db.obtener_estadisticas_dashboard()
        ultimos_pagos = db.obtener_ultimos_pagos(5)
    except DB_Error as e:
        flash(f"Error al cargar los datos del dashboard: {e}", "error")
        estadisticas = {'registros_hoy': 0, 'ingresos_hoy': 0, 'ingresos_mes': 0}
        ultimos_pagos = []
    return render_template("dashboard.html", stats=estadisticas, pagos=ultimos_pagos, current_section='ventas')

@app.route("/registrar", methods=['GET'])
@login_required
def registrar():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    return render_template("index.html", current_section='ventas')

@app.route("/submit", methods=["POST"])
@login_required
def submit():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    form_data = request.form.to_dict()
    try:
        form_data['fecha'] = datetime.strptime(form_data.get('fecha'), '%Y-%m-%d')
        cliente_id = db.buscar_o_crear_cliente(form_data)
        form_data['numero_operacion'] = form_data.pop('num_operacion', None)
        nuevo_pago_id = db.crear_pago(cliente_id, form_data)
        flash("Registro guardado correctamente.", "success")
        detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id}"
        db.registrar_auditoria(session.get('full_name'), "CREAR_PAGO", request.remote_addr, "pagos", nuevo_pago_id, detalles)
    except (ValueError, TypeError):
        flash("La fecha es un campo obligatorio y debe tener el formato correcto.", "error")
    except IntegrityError:
        flash("Error: El N° de Operación ya existe en otro registro.", "error")
    except DB_Error as e:
        flash(f"Error al guardar el registro: {e}", "error")
    return redirect(url_for("registrar"))

@app.route("/consulta", methods=["GET"])
@login_required
def consulta():
    query = request.args.get("query", "").strip()
    page = request.args.get('page', 1, type=int)
    try:
        todos_los_resultados = db.buscar_pagos_completos(query)
        total_records = len(todos_los_resultados)
        total_pages = (total_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE if RECORDS_PER_PAGE > 0 else 1
        start_index = (page - 1) * RECORDS_PER_PAGE
        end_index = start_index + RECORDS_PER_PAGE
        resultados_paginados = todos_los_resultados[start_index:end_index]
    except DB_Error as e:
        flash(f"Error al consultar la base de datos: {e}", "error")
        resultados_paginados, total_pages = [], 1
    headers_db = ["ID"] + HEADERS
    return render_template("consulta.html", resultados=resultados_paginados, headers=headers_db, page=page, total_pages=total_pages, query=query, current_section='ventas')

@app.route("/reportes")
@login_required
def reportes():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    page = request.args.get('page', 1, type=int)
    try:
        reporte_completo_db = db.generar_reporte_asesores_db(start_date, end_date)
        total_records = len(reporte_completo_db)
        total_pages = (total_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE if RECORDS_PER_PAGE > 0 else 1
        start_index = (page - 1) * RECORDS_PER_PAGE
        end_index = start_index + RECORDS_PER_PAGE
        reporte_paginado_db = reporte_completo_db[start_index:end_index]
        total_general_ventas = sum(item.get('total_asesor', 0) or 0 for item in reporte_completo_db)
        total_general_registros = sum(item.get('registros_asesor', 0) for item in reporte_completo_db)
        page_total_ventas = sum(item.get('total_asesor', 0) or 0 for item in reporte_paginado_db)
        page_total_registros = sum(item.get('registros_asesor', 0) for item in reporte_paginado_db)
        reporte_para_plantilla = [(item['asesor'], item) for item in reporte_paginado_db]
        return render_template(
            "reportes.html", reporte=reporte_para_plantilla, total_ventas=total_general_ventas,
            total_registros=total_general_registros, start_date=start_date, end_date=end_date,
            page=page, total_pages=total_pages, page_total_ventas=page_total_ventas,
            page_total_registros=page_total_registros, current_section='ventas'
        )
    except DB_Error as e:
        flash(f"Error al generar el reporte: {e}", "error")
        return render_template("reportes.html", reporte=[], current_section='ventas')

@app.route("/descargar")
@login_required
def descargar():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    try:
        output = db.generar_excel_dinamico(HEADERS)
        if output:
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name='registros_pagos.xlsx')
        flash("No se pudo generar el archivo Excel.", "error")
    except DB_Error as e:
        flash(f"Error al generar el archivo Excel: {e}", "error")
    return redirect(url_for('dashboard'))

@app.route("/auditoria")
@login_required
def auditoria():
    if session.get('role') != 'admin':
        return redirect(url_for('dashboard'))
    try:
        logs = db.leer_log_auditoria()
    except DB_Error as e:
        flash(f"Error al leer la auditoría: {e}", "error")
        logs = []
    return render_template("auditoria.html", logs=logs, current_section='ventas')

# --- Rutas de la Sección CRM ---
@app.route("/crm")
@login_required
def crm_dashboard():
    return render_template("crm_dashboard.html", current_section='crm')

@app.route("/crm/consulta")
@login_required
def consulta_leads():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    query = request.args.get("query", "").strip()
    try:
        leads = db.buscar_leads(query)
    except DB_Error as e:
        flash(f"Error al consultar los leads: {e}", "error")
        leads = []
    return render_template("consulta_leads.html", leads=leads, query=query, current_section='crm')

@app.route("/crm/descargar_leads")
@login_required
def descargar_leads():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    try:
        output = db.generar_excel_leads()
        if output:
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name='registros_leads.xlsx')
        flash("No se pudo generar el archivo Excel de leads.", "error")
    except DB_Error as e:
        flash(f"Error al generar el Excel de leads: {e}", "error")
    return redirect(url_for('crm_dashboard'))

@app.route("/registrar_interesado", methods=['GET', 'POST'])
@login_required
def registrar_interesado():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        form_data = request.form.to_dict()
        asesor = session.get('full_name')
        try:
            nuevo_id = db.registrar_potencial(form_data, asesor)
            if nuevo_id:
                db.crear_oportunidad_si_no_existe(nuevo_id, asesor, form_data.get('curso_interes'))
                flash("Cliente potencial registrado y oportunidad creada.", "success")
                db.registrar_auditoria(asesor, "CREAR_POTENCIAL", request.remote_addr, "clientes", nuevo_id)
            else:
                flash("Ya existe un cliente con ese DNI o correo.", "error")
        except DB_Error as e:
            flash(f"Error al registrar al cliente potencial: {e}", "error")
        return redirect(url_for('registrar_interesado'))
    return render_template("registrar_interesado.html", current_section='crm')

@app.route("/oportunidades")
@login_required
def oportunidades():
    if session.get('role') not in ['admin', 'equipo']:
        return redirect(url_for('dashboard'))
    asesor = session.get('full_name')
    etapas = ['Nuevo', 'Contactado', 'Propuesta', 'Negociación']
    oportunidades_por_etapa = {etapa: [] for etapa in etapas}
    try:
        for op in db.obtener_oportunidades_por_asesor(asesor):
            if op['estado_oportunidad'] in oportunidades_por_etapa:
                oportunidades_por_etapa[op['estado_oportunidad']].append(op)
    except DB_Error as e:
        flash(f"Error al cargar las oportunidades: {e}", "error")
    return render_template("oportunidades.html", oportunidades_por_etapa=oportunidades_por_etapa, etapas=etapas, current_section='crm')

@app.route("/api/oportunidad/mover", methods=['POST'])
@login_required
def mover_oportunidad_api():
    data = request.get_json()
    try:
        db.mover_oportunidad(data.get('id'), data.get('estado'))
        return jsonify({"status": "success"}), 200
    except DB_Error as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- Rutas Comunes y de Edición ---
@app.route("/cliente/<int:cliente_id>", methods=["GET", "POST"])
@login_required
def perfil_cliente(cliente_id):
    try:
        if request.method == "POST":
            form_type = request.form.get('form_type')
            if form_type == 'interaccion':
                db.registrar_interaccion(cliente_id, session.get('full_name'), request.form.get("tipo_interaccion"), request.form.get("notas"))
                flash("Interacción registrada.", "success")
            elif form_type == 'etiqueta':
                nombre_etiqueta = request.form.get('nombre_etiqueta', '').strip()
                if nombre_etiqueta:
                    etiqueta_id = db.obtener_o_crear_etiqueta_id(nombre_etiqueta)
                    db.anadir_etiqueta_a_cliente(cliente_id, etiqueta_id)
                    flash("Etiqueta añadida.", "success")
                else:
                    flash("El nombre de la etiqueta no puede estar vacío.", "error")
            return redirect(url_for('perfil_cliente', cliente_id=cliente_id))
        
        cliente = db.obtener_cliente_por_id(cliente_id)
        if not cliente:
            flash("Cliente no encontrado.", "error")
            return redirect(url_for('consulta_leads'))
        
        return render_template("perfil_cliente.html", 
            cliente=cliente, 
            pagos=db.obtener_pagos_por_cliente(cliente_id),
            interacciones=db.obtener_interacciones_por_cliente(cliente_id),
            etiquetas_cliente=db.obtener_etiquetas_por_cliente(cliente_id),
            current_section='crm')
    except DB_Error as e:
        flash(f"Error al cargar el perfil del cliente: {e}", "error")
        return redirect(url_for('consulta_leads'))

@app.route("/cliente/<int:cliente_id>/etiqueta/quitar/<int:etiqueta_id>")
@login_required
def quitar_etiqueta(cliente_id, etiqueta_id):
    try:
        db.quitar_etiqueta_a_cliente(cliente_id, etiqueta_id)
        flash("Etiqueta eliminada.", "success")
    except DB_Error as e:
        flash(f"Error al quitar la etiqueta: {e}", "error")
    return redirect(url_for('perfil_cliente', cliente_id=cliente_id))

@app.route("/editar/<int:id>", methods=["GET", "POST"])
@login_required
def editar(id):
    if request.method == "POST":
        form_data = request.form.to_dict()
        try:
            db.actualizar_pago(id, form_data)
            flash("Pago actualizado correctamente.", "success")
            db.registrar_auditoria(session.get('full_name'), "EDITAR_PAGO", request.remote_addr, "pagos", id)
        except DB_Error as e:
            flash(f"Error al actualizar el pago: {e}", "error")
        return redirect(url_for("consulta", query=request.form.get("query", "")))
    
    data = db.obtener_pago_por_id(id)
    if not data:
        flash("Registro no encontrado.", "error")
        return redirect(url_for('consulta'))
    return render_template("editar.html", data=data, labels_and_fields=zip(HEADERS, FIELDS), id=id, query=request.args.get('query', ''), current_section='ventas')

@app.route("/actualizar_pago/<int:id>", methods=["GET", "POST"])
@login_required
def actualizar_pago(id):
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    if request.method == "POST":
        query = request.form.get("query", "")
        try:
            pago_original = db.obtener_pago_por_id(id)
            if not pago_original:
                flash("Error: No se encontró el registro original.", "error")
                return redirect(url_for("consulta", query=query))
            cliente_id = pago_original['cliente_id']
            datos_nuevo_pago = request.form.to_dict()
            datos_nuevo_pago['fecha'] = datetime.now()
            datos_nuevo_pago['numero_operacion'] = datos_nuevo_pago.pop('num_operacion', None)
            datos_nuevo_pago['especialidad'] = pago_original['especialidad']
            datos_nuevo_pago['modalidad'] = pago_original['modalidad']
            datos_nuevo_pago['asesor'] = pago_original['asesor']
            nuevo_pago_id = db.crear_pago(cliente_id, datos_nuevo_pago)
            flash("Renovación de pago registrada exitosamente.", "success")
            detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id} (RENOVACIÓN)"
            db.registrar_auditoria(session.get('full_name'), "RENOVAR_PAGO", request.remote_addr, "pagos", nuevo_pago_id, detalles)
            return redirect(url_for("consulta", query=query))
        except IntegrityError:
            flash("Error: El N° de Operación ingresado ya existe. Por favor, verifícalo.", "error")
            return redirect(url_for("actualizar_pago", id=id, query=query))
        except DB_Error as e:
            flash(f"Error al procesar el pago: {e}", "error")
            return redirect(url_for("consulta", query=query))
    
    data = db.obtener_pago_por_id(id)
    if not data:
        flash("Registro no encontrado.", "error")
        return redirect(url_for('consulta'))
    return render_template("actualizar_pago.html", data=data, id=id, query=request.args.get('query', ''), current_section='ventas')

@app.route("/eliminar", methods=["POST"])
@login_required
def eliminar():
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    pago_id = int(request.form.get("id"))
    try:
        pago = db.obtener_pago_por_id(pago_id)
        if pago:
            cliente_id = pago['cliente_id']
            db.cambiar_estado_cliente(cliente_id, 'inactivo')
            flash("Cliente desactivado correctamente.", "success")
            db.registrar_auditoria(session.get('full_name'), "DESACTIVAR_CLIENTE", request.remote_addr, "clientes", cliente_id)
    except DB_Error as e:
        flash(f"Error al desactivar al cliente: {e}", "error")
    return redirect(url_for("consulta", query=request.form.get("query", "")))

# --- Rutas de CERTIFICADOS y Favicon ---
@app.route("/certificados")
@login_required
def certificados():
    query = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    try:
        todos_los_datos = sheets_manager.obtener_datos_certificados()
        if query:
            resultados_filtrados = [r for r in todos_los_datos if any(query in str(v).lower() for v in r.values())]
        else:
            resultados_filtrados = todos_los_datos
        total_records = len(resultados_filtrados)
        total_pages = (total_records + RECORDS_PER_PAGE_SHEETS - 1) // RECORDS_PER_PAGE_SHEETS if RECORDS_PER_PAGE_SHEETS > 0 else 1
        start_index = (page - 1) * RECORDS_PER_PAGE_SHEETS
        resultados_paginados = resultados_filtrados[start_index:start_index + RECORDS_PER_PAGE_SHEETS]
    except Exception as e:
        flash(f"No se pudieron cargar los datos de los certificados: {e}", "error")
        resultados_paginados, total_pages = [], 1
    
    return render_template(
        "certificados.html", 
        certificados=resultados_paginados,
        page=page,
        total_pages=total_pages,
        query=query,
        is_certificate_section=True
    )
    
@app.route("/crm/lead/eliminar/<int:cliente_id>", methods=["POST"])
@login_required
def eliminar_lead(cliente_id):
    """Elimina un lead de forma permanente de la base de datos."""
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta_leads'))
    
    try:
        # Llama a la nueva función de la base de datos
        db.eliminar_lead_por_id(cliente_id)
        flash("Lead eliminado permanentemente con éxito.", "success")
        
        # Registra la acción en la auditoría
        db.registrar_auditoria(session.get('full_name'), "ELIMINAR_LEAD_PERMANENTE", request.remote_addr, "clientes", cliente_id)

    except DB_Error as e:
        flash(f"Error al eliminar el lead: {e}", "error")
    
    return redirect(url_for('consulta_leads'))

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', 'images'), 'icon.png', mimetype='image/png')