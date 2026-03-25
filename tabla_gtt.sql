-- ============================================================
-- GTT para reporte PRR0402G
-- Reemplaza PR_OPERACIONES_TMP (tabla permanente con SESION)
--
-- TIPO_REGISTRO discrimina el origen del registro:
--   'LIN' = Lineas de credito
--   'BL'  = Operaciones bajo linea
--   'FL'  = Operaciones fuera de linea
--   'IND' = Operaciones indirectas
-- ============================================================
CREATE GLOBAL TEMPORARY TABLE PR.PR_ANTEC_CRED_GTT
(
    -- ---------------------------------------------------------
    -- Identificacion del registro
    -- ---------------------------------------------------------
    cod_empresa             VARCHAR2(5)       NOT NULL,  -- cod_empresa
    num_tramite            NUMBER         NOT NULL,
    tipo_registro          VARCHAR2(3)    NOT NULL,  -- 'LIN'/'BL'/'FL'/'IND'
    no_credito             NUMBER(20),             -- no_credito linea (solo tipo 100)
    no_credito_linea       NUMBER(20),             -- no_credito de la linea padre (tipos 200/300/400)

    -- ---------------------------------------------------------
    -- Moneda y fechas principales
    -- ---------------------------------------------------------
    des_moneda             VARCHAR2(30),             -- abreviatura moneda
    fec_inicio             DATE,                     -- f_apertura (lineas) o fec_inicio (ops)
    fec_vencimiento        DATE,                     -- f_vencimiento del tramite/linea

    -- ---------------------------------------------------------
    -- Saldos en dolares (USD)
    -- ---------------------------------------------------------
    saldo_directo_usd      NUMBER(16,2),             -- saldo actual (lineas) / saldo directo (ops)
    saldo_contingente_usd  NUMBER(16,2),             -- saldo disponible (lineas) / saldo contable (ops)
    monto_desembolsado_usd NUMBER(16,2),             -- monto desembolsado convertido a USD

    -- ---------------------------------------------------------
    -- Descripciones
    -- ---------------------------------------------------------
    des_producto           VARCHAR2(60),
    des_estado             VARCHAR2(10),             -- abrev_estado
    des_instrumento        VARCHAR2(60),             -- desc instrumento o nombre cliente (indirectas)

    -- ---------------------------------------------------------
    -- Datos especificos de lineas (tipo_registro = 'LIN')
    -- ---------------------------------------------------------
    fec_prox_revision      DATE,                     -- f_proxima_revision de la linea

    -- ---------------------------------------------------------
    -- Datos especificos de operaciones BL/FL (tipo_registro = 'BL'/'FL')
    -- ---------------------------------------------------------
    num_tramite_padre      NUMBER,                   -- num_tramite de la linea padre
    fec_vto_prox_cuota     DATE,                     -- fecha vencimiento proxima cuota pendiente

    -- Fechas de cancelacion de ultimas cuotas (solo tipo_registro = 'FL')
    fec_cancelacion_cuota1 DATE,                     -- vl_fecha1
    fec_cancelacion_cuota2 DATE,                     -- vl_fecha2  (reutiliza f_paso_castigo)
    fec_cancelacion_cuota3 DATE,                     -- vl_fecha3
    fec_cancelacion_cuota4 DATE,                     -- vl_fecha4

    -- Dias de mora por cuota (tipo_registro = 'BL'/'FL')
    -- Bajo linea: cuotas 1-6 / Fuera de linea: cuotas 1-4
    dias_mora_cuota1       NUMBER(5),                -- vl_cuota1
    dias_mora_cuota2       NUMBER(5),                -- vl_cuota2
    dias_mora_cuota3       NUMBER(5),                -- vl_cuota3
    dias_mora_cuota4       NUMBER(5),                -- vl_cuota4
    dias_mora_cuota5       NUMBER(5),                -- vl_cuota5 (solo tipo_registro = 'BL')
    dias_mora_cuota6       NUMBER(5),                -- vl_cuota6 (solo tipo_registro = 'BL')

    -- Dias de atraso general del tramite
    dias_atraso            NUMBER(10)                -- v_dias_atraso
)
ON COMMIT PRESERVE ROWS;

-- Indice de acceso principal (Oracle Reports filtra por tipo_registro)
-- Valores posibles: 'LIN', 'BL', 'FL', 'IND'
CREATE INDEX ix_antec_cred_gtt_tipo
    ON PR.PR_ANTEC_CRED_GTT (cod_empresa, tipo_registro, num_tramite);

-- Constraint para garantizar valores validos de tipo_registro
ALTER TABLE PR.PR_ANTEC_CRED_GTT
    ADD CONSTRAINT chk_antec_cred_tipo_registro
    CHECK (tipo_registro IN ('LIN', 'BL', 'FL', 'IND'));