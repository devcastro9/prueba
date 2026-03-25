-- ============================================================
-- Cada sesion de BD tiene su propio conjunto de datos en la GTT,
-- por lo que no se necesita columna SESION.
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
    -- PK garantiza unicidad: un tramite solo puede tener un tipo
    -- dentro de la misma carga.
    -- ---------------------------------------------------------
    cod_empresa             VARCHAR2(5)    NOT NULL,   -- PA.EMPRESA.COD_EMPRESA
    num_tramite             NUMBER(15)     NOT NULL,   -- PR_TRAMITE.NUM_TRAMITE / PR_CREDITOS.NUM_TRAMITE
    tipo_registro           VARCHAR2(3)    NOT NULL,   -- 'LIN'/'BL'/'FL'/'IND'
    no_credito              NUMBER(20),                -- PR_CREDITOS.NO_CREDITO (solo LIN)
    -- no_credito_linea eliminado: num_tramite_padre cubre el mismo rol
    -- con mayor precision (es FK directa a PR_CREDITOS.NUM_TRAMITE)

    -- ---------------------------------------------------------
    -- Moneda y fechas principales
    -- ---------------------------------------------------------
    des_moneda              VARCHAR2(10),              -- MONEDA.ABREVIATURA (max 10 en produccion)
    fec_inicio              DATE,                      -- F_APERTURA (LIN) o FEC_INICIO (BL/FL/IND)
    fec_vencimiento         DATE,                      -- F_VENCIMIENTO del tramite o linea

    -- ---------------------------------------------------------
    -- Saldos en dolares (USD)
    -- ---------------------------------------------------------
    saldo_directo_usd       NUMBER(16,2),              -- saldo actual (LIN) / saldo directo (BL/FL/IND)
    saldo_contingente_usd   NUMBER(16,2),              -- saldo disponible (LIN) / saldo contable (BL/FL/IND)
    monto_desembolsado_usd  NUMBER(16,2),              -- monto operacion convertido a USD

    -- ---------------------------------------------------------
    -- Descripciones
    -- ---------------------------------------------------------
    des_producto            VARCHAR2(60),              -- descripcion del tipo de producto
    des_estado              VARCHAR2(20),              -- PR_ESTADOS_CREDITO.ABREV_ESTADO (ampliado a 20)
    des_instrumento         VARCHAR2(60),              -- desc instrumento (BL/FL)
    nom_cliente             VARCHAR2(100),             -- nombre cliente (IND)
    -- ---------------------------------------------------------
    -- Datos especificos de lineas (tipo_registro = 'LIN')
    -- ---------------------------------------------------------
    fec_prox_revision       DATE,                      -- PR_CREDITOS.F_PROXIMA_REVISION

    -- ---------------------------------------------------------
    -- Datos especificos de operaciones BL/FL (tipo_registro = 'BL'/'FL')
    -- ---------------------------------------------------------
    num_tramite_padre       NUMBER(15),                -- NUM_TRAMITE de la linea padre (BL/FL)
    fec_vto_prox_cuota      DATE,                      -- MIN(f_cuota) con estado='A' en PR_PLAN_PAGOS

    -- Fechas de cancelacion de las ultimas cuotas (solo tipo_registro = 'FL')
    -- Origen: PR_PLAN_PAGOS.F_CANCELACION de las 4 ultimas cuotas canceladas
    fec_cancelacion_cuota1  DATE,                      -- cuota mas reciente (vl_fecha1)
    fec_cancelacion_cuota2  DATE,                      -- (vl_fecha2)
    fec_cancelacion_cuota3  DATE,                      -- (vl_fecha3)
    fec_cancelacion_cuota4  DATE,                      -- cuota mas antigua (vl_fecha4)

    -- Dias de mora por cuota
    -- BL: usa cuotas 1-6 (ultimas 6 cuotas de PR_PLAN_PAGOS)
    -- FL: usa cuotas 1-4 (ultimas 4 cuotas de PR_PLAN_PAGOS)
    -- Origen: (F_CANCELACION - F_CUOTA) de PR_PLAN_PAGOS
    dias_mora_cuota1        NUMBER(5),                 -- cuota mas reciente
    dias_mora_cuota2        NUMBER(5),
    dias_mora_cuota3        NUMBER(5),
    dias_mora_cuota4        NUMBER(5),
    dias_mora_cuota5        NUMBER(5),                 -- solo BL
    dias_mora_cuota6        NUMBER(5),                 -- solo BL (requiere fix en cur_plan: MAX-5)

    -- Dias de atraso general del tramite (calculado por Datos_Generales_Tramite)
    dias_atraso             NUMBER(10),

    -- ---------------------------------------------------------
    -- PRIMARY KEY: evita duplicados si el procedimiento se
    -- llama mas de una vez en la misma sesion sin limpiar antes
    -- ---------------------------------------------------------
    CONSTRAINT pk_antec_cred_gtt
        PRIMARY KEY (cod_empresa, tipo_registro, num_tramite)
)
ON COMMIT PRESERVE ROWS;

-- ---------------------------------------------------------
-- Indice 1: acceso principal por tipo (Oracle Reports)
-- Cubre: SELECT ... WHERE cod_empresa=:e AND tipo_registro=:t
-- num_tramite incluido para evitar lookup a tabla (index-only scan)
-- ---------------------------------------------------------
CREATE INDEX ix_antec_cred_tipo
    ON PR.PR_ANTEC_CRED_GTT (cod_empresa, tipo_registro, num_tramite);

-- ---------------------------------------------------------
-- Check constraint: valores validos de tipo_registro
-- ---------------------------------------------------------
ALTER TABLE PR.PR_ANTEC_CRED_GTT
    ADD CONSTRAINT ck_antec_cred_tipo_reg
    CHECK (tipo_registro IN ('LIN', 'BL', 'FL', 'IND'));