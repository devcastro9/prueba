-- ============================================================
-- mocastro - FSN-2026-04-05
-- GTT para centralizar la carga de detalle de previsiones
-- del formulario PR0349 (OBT_PREVISION).
--
-- Cada sesion de BD tiene su propio conjunto de datos.
-- ON COMMIT PRESERVE ROWS: los datos persisten durante
-- toda la sesion, no solo dentro de la transaccion.
--
-- Columnas basadas en los items del bloque :bkdetpre
-- del formulario PR0349.
-- ============================================================
DROP TABLE PR.PR_PREVISION_DET_GTT CASCADE CONSTRAINTS;

CREATE GLOBAL TEMPORARY TABLE PR.PR_PREVISION_DET_GTT
(
    COD_EMPRESA          VARCHAR2(5  BYTE) NOT NULL,
    NUM_TRAMITE          NUMBER(10),
    CODIGO_ESTADO        VARCHAR2(2  BYTE),
    DES_ESTADO           VARCHAR2(60 BYTE),
    COD_MONEDA           NUMBER(3),
    DES_MONEDA           VARCHAR2(10 BYTE),
    COD_TIP_OPERACION    VARCHAR2(10 BYTE),
    TIPO_OPERACION       VARCHAR2(60 BYTE),
    LINEA                VARCHAR2(1  BYTE),   -- bajo_linea_credito
    PLAZA                VARCHAR2(30 BYTE),   -- nombre_corto sucursal
    NO_CREDITO           NUMBER(10),
    F_PRIMER_DESEMBOLSO  DATE,
    F_VCTO_FINAL         DATE,
    F_CUOTA              DATE,
    MONTO_DESEMBOLSADO   NUMBER(16,2),
    SALDO_ACTUAL         NUMBER(16,2),
    PRE_CON              NUMBER(16,2),        -- prevision contingente
    PREVISION            NUMBER(16,2)         -- prevision normal + adicional + diferida
)
ON COMMIT PRESERVE ROWS
NOCACHE;

CREATE INDEX PR.IX_PREVISION_DET
    ON PR.PR_PREVISION_DET_GTT (COD_EMPRESA, NUM_TRAMITE);
