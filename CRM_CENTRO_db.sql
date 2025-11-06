CREATE DATABASE IF NOT EXISTS CRM_CENTRO_db;
USE CRM_CENTRO_db;

CREATE TABLE clientes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    dni VARCHAR(15) UNIQUE NOT NULL,
    correo VARCHAR(255) UNIQUE NOT NULL,
    celular VARCHAR(20),
    genero VARCHAR(20)
);

CREATE TABLE pagos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    fecha DATETIME NOT NULL,
    cuota DECIMAL(10, 2) NOT NULL,
    tipo_de_cuota VARCHAR(50),
    banco VARCHAR(100),
    destino VARCHAR(100),
    numero_operacion VARCHAR(50) UNIQUE,
    especialidad VARCHAR(100),
    modalidad VARCHAR(50),
    asesor VARCHAR(255),
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
);

CREATE TABLE auditoria_accesos (
	id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    usuario_app VARCHAR(255) NOT NULL,
    accion VARCHAR(50) NOT NULL,
    tabla_afectada VARCHAR(255),
    registro_id INT,
    detalles TEXT, 
    ip_origen VARCHAR(45)
);

-- Permite que los campos del cliente sean opcionales (excepto DNI)
ALTER TABLE clientes MODIFY COLUMN nombre VARCHAR(255) NULL;
ALTER TABLE clientes MODIFY COLUMN correo VARCHAR(255) NULL;
ALTER TABLE clientes MODIFY COLUMN celular VARCHAR(20) NULL;
ALTER TABLE clientes MODIFY COLUMN genero VARCHAR(20) NULL;

-- Permite que los campos del pago sean opcionales (excepto fecha, cuota y N° de Operación)
-- Mantenemos cuota como NOT NULL porque un pago sin monto no tiene sentido.
ALTER TABLE pagos MODIFY COLUMN tipo_de_cuota VARCHAR(50) NULL;
ALTER TABLE pagos MODIFY COLUMN banco VARCHAR(100) NULL;
ALTER TABLE pagos MODIFY COLUMN destino VARCHAR(100) NULL;
ALTER TABLE pagos MODIFY COLUMN especialidad VARCHAR(100) NULL;
ALTER TABLE pagos MODIFY COLUMN modalidad VARCHAR(50) NULL;
ALTER TABLE pagos MODIFY COLUMN asesor VARCHAR(255) NULL;

-- Nos aseguramos que los campos obligatorios sigan siéndolo
ALTER TABLE pagos MODIFY COLUMN fecha DATETIME NOT NULL;
ALTER TABLE pagos MODIFY COLUMN numero_operacion VARCHAR(50) UNIQUE NOT NULL;

-- Verificar cliente activo --
ALTER TABLE clientes
ADD COLUMN estado VARCHAR(20) NOT NULL DEFAULT 'activo';

-- Estado por defecto sea 'potencial' para nuevos registros
-- y añadimos campos para el seguimiento.
ALTER TABLE clientes
MODIFY COLUMN estado VARCHAR(20) NOT NULL DEFAULT 'potencial',
ADD COLUMN curso_interes VARCHAR(255) NULL,
ADD COLUMN asesor_asignado VARCHAR(255) NULL,
ADD COLUMN fecha_contacto DATETIME NULL;

-- Clientes existentes que ya tienen pagos sean 'activos'.
-- (Este comando es opcional si ya lo tienes controlado)
UPDATE clientes SET estado = 'activo' WHERE id IN (SELECT DISTINCT cliente_id FROM pagos);

-- TABLA PARA EL HISTORIAL DE INTERACCIONES CON EL CLIENTE
CREATE TABLE interacciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    asesor_nombre VARCHAR(255) NOT NULL,
    fecha_interaccion DATETIME NOT NULL,
    tipo_interaccion VARCHAR(50) NOT NULL, -- Ej: 'Llamada', 'WhatsApp', 'Email'
    notas TEXT,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
);

-- TABLA PARA GESTIONAR OPORTUNIDADES DE VENTA (EL EMBUDO)
CREATE TABLE oportunidades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    asesor_asignado VARCHAR(255) NOT NULL,
    curso_interes VARCHAR(255),
    valor_estimado DECIMAL(10, 2) NULL,
    estado_oportunidad VARCHAR(50) NOT NULL, -- 'Nuevo', 'Contactado', 'Propuesta', 'Negociación', 'Ganada', 'Perdida'
    fecha_creacion DATETIME NOT NULL,
    ultima_actualizacion DATETIME NOT NULL,
    UNIQUE KEY (cliente_id, curso_interes), -- Evita duplicar la misma oportunidad para el mismo cliente
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
);

-- Guarda todas las etiquetas únicas que se pueden usar
CREATE TABLE etiquetas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) UNIQUE NOT NULL
);

-- Tabla intermedia para la relación muchos-a-muchos entre clientes y etiquetas
CREATE TABLE cliente_etiquetas (
    cliente_id INT NOT NULL,
    etiqueta_id INT NOT NULL,
    PRIMARY KEY (cliente_id, etiqueta_id),
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
    FOREIGN KEY (etiqueta_id) REFERENCES etiquetas(id) ON DELETE CASCADE
);