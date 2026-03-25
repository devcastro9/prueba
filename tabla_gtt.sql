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
DROP TABLE PR.PR_ANTEC_CRED_GTT CASCADE CONSTRAINTS;

CREATE GLOBAL TEMPORARY TABLE PR.PR_ANTEC_CRED_GTT
(
    COD_EMPRESA               VARCHAR2 (5 BYTE) NOT NULL,
    TIPO_REGISTRO             VARCHAR2 (3 BYTE) NOT NULL,
    NUM_TRAMITE               NUMBER (10),
    NO_OPERACION              NUMBER (16),
    COD_OPERACION             VARCHAR2 (15 BYTE),
    DES_MONEDA                VARCHAR2 (4 BYTE),
    FEC_INICIO                DATE,
    FEC_VENCIMIENTO           DATE,
    SALDO_DIRECTO             NUMBER (16, 2),
    SALDO_CONTINGENTE         NUMBER (16, 2),
    MONTO_DESEMBOLSADO        NUMBER (16, 2),
    DES_PRODUCTO              VARCHAR2 (60 BYTE),
    DES_ESTADO                VARCHAR2 (20 BYTE),
    DES_INSTRUMENTO           VARCHAR2 (60 BYTE),
    NOM_CLIENTE               VARCHAR2 (100 BYTE),
    FEC_PROX_REVISION         DATE,
    NUM_TRAMITE_PADRE         NUMBER (10),
    NO_CREDITO_ORIGEN         NUMBER (7),
    FEC_VTO_PROX_CUOTA        DATE,
    FEC_CANCELACION_CUOTA1    DATE,
    FEC_CANCELACION_CUOTA2    DATE,
    FEC_CANCELACION_CUOTA3    DATE,
    FEC_CANCELACION_CUOTA4    DATE,
    DIAS_MORA_CUOTA1          NUMBER (5),
    DIAS_MORA_CUOTA2          NUMBER (5),
    DIAS_MORA_CUOTA3          NUMBER (5),
    DIAS_MORA_CUOTA4          NUMBER (5),
    DIAS_MORA_CUOTA5          NUMBER (5),
    DIAS_MORA_CUOTA6          NUMBER (5),
    DIAS_ATRASO               NUMBER (10)
)
ON COMMIT PRESERVE ROWS
NOCACHE;

ALTER TABLE PR.PR_ANTEC_CRED_GTT
    ADD (CONSTRAINT CK_ANTEC_CRED_TIPO_REG CHECK
             (tipo_registro IN ('LIN',
                                'BL',
                                'FL',
                                'IND'))
             ENABLE VALIDATE);

CREATE INDEX PR.IX_ANTEC_CRED
    ON PR.PR_ANTEC_CRED_GTT (COD_EMPRESA, TIPO_REGISTRO);