-- MA: 706096
-- ===============
-- OPTIMIZADO: filtro de cargo empujado al inicio, lectura unica de tablas grandes,
--             paralelismo forzado a nivel de sesion (DOP 16)

SET FEEDBACK OFF
SET SQLFORMAT CSV

-- Forzar paralelismo maximo a nivel de sesion
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16;

SPOOL ma_706096_reporte.csv

WITH
    -- ===========================================================================
    -- 1. Funcionarios con los 3 cargos requeridos (conjunto diminuto)
    --    Empujar este filtro al inicio reduce ordenes de magnitud el volumen
    --    de creditos a procesar en TODOS los CTEs posteriores
    -- ===========================================================================
    oficiales
    AS
        (SELECT /*+ MATERIALIZE */
                fun.ID_FUNCIONARIO,
                ofi.NOMBRE       AS nombre_oficial,
                crg.DESCRIPCION  AS cargo
           FROM RH.RH_FUNCIONARIOS fun
                INNER JOIN RH.RH_CARGOS crg
                    ON     fun.COD_EMPRESA = crg.COD_EMPRESA
                       AND fun.ID_CARGO    = crg.ID_CARGO
                LEFT JOIN PA.PERSONAS ofi
                    ON fun.ID_FUNCIONARIO = ofi.COD_PERSONA
          WHERE     fun.COD_EMPRESA = '1'
                AND crg.ID_CARGO IN ('162570', '162670', '163576')),
    -- ===========================================================================
    -- 2. Creditos validos al cierre + datos de tramite + oficial
    --    Lectura UNICA de PR_CREDITOS_HI, PR_CREDITOS, PR_TRAMITE
    --    Ya filtrado por cargo: reduce drasticamente el universo de trabajo
    -- ===========================================================================
    cred_validos
    AS
        (SELECT /*+ MATERIALIZE PARALLEL(16) USE_HASH(his cre tra) */
                his.CODIGO_EMPRESA,
                his.NO_CREDITO,
                tra.NUM_TRAMITE,
                cre.ESTADO,
                tra.UNIDAD_EJECUTORA,
                tra.COD_TIP_OPERACION,
                tra.COD_TIP_PRODUCTO,
                NVL (cre.CODIGO_ANALISTA, cre.CODIGO_EJECUTIVO)  AS id_oficial,
                ofc.nombre_oficial,
                ofc.cargo
           FROM PR.PR_CREDITOS_HI his
                INNER JOIN PR.PR_CREDITOS cre
                    ON     his.CODIGO_EMPRESA = cre.CODIGO_EMPRESA
                       AND his.NO_CREDITO     = cre.NO_CREDITO
                INNER JOIN PR.PR_TRAMITE tra
                    ON     tra.COD_EMPRESA = cre.CODIGO_EMPRESA
                       AND tra.NUM_TRAMITE = cre.NUM_TRAMITE
                INNER JOIN oficiales ofc
                    ON NVL (cre.CODIGO_ANALISTA, cre.CODIGO_EJECUTIVO) = ofc.ID_FUNCIONARIO
          WHERE     his.FEC_REGISTRO_HI = TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                --AND his.no_credito = 3360766
                AND his.Estado IN ('D',
                                   'E',
                                   'V',
                                   'J',
                                   'G',
                                   'I',
                                   'L',
                                   'T',
                                   'C')),
    -- ===========================================================================
    -- 3. Recibos normales agregados por credito en el periodo
    -- ===========================================================================
    rec
    AS
        (  SELECT /*+ MATERIALIZE PARALLEL(16) */
                  rc.CODIGO_EMPRESA,
                  rc.NO_CREDITO,
                  SUM (rc.MONTO_AMORTIZACION)     AS capital_pagado,
                  SUM (rc.MONTO_INTERESES)         AS interes_pagado,
                  SUM (rc.MONTO_PUNITORIOS)        AS punitorios
             FROM PR.PR_RECIBOS rc
            WHERE     rc.FECHA_CREACION_RECIBO BETWEEN TO_DATE ('1/3/2026',
                                                                'dd/mm/yyyy')
                                                   AND TO_DATE ('31/3/2026',
                                                                'dd/mm/yyyy')
                  AND EXISTS
                          (SELECT 1
                             FROM cred_validos cr
                            WHERE rc.CODIGO_EMPRESA = cr.CODIGO_EMPRESA
                              AND rc.NO_CREDITO     = cr.NO_CREDITO)
         GROUP BY rc.CODIGO_EMPRESA, rc.NO_CREDITO),
    -- ===========================================================================
    -- 4. Recibos diferidos agregados por credito en el periodo
    -- ===========================================================================
    rec_dif
    AS
        (  SELECT /*+ MATERIALIZE PARALLEL(16) */
                  rc.COD_EMPRESA,
                  rc.NO_CREDITO,
                  SUM (rc.MONTO_CAPITAL_PAGADO)       AS capital_pagado_dif,
                  SUM (rc.MONTO_INTERESES_PAGADO)     AS interes_pagado_dif,
                  SUM (rc.MONTO_PUNITORIOS)           AS penal_dif
             FROM PR.PR_RECIBOS_DIF rc
            WHERE     rc.FECHA_RECIBO BETWEEN TO_DATE ('1/3/2026',
                                                       'dd/mm/yyyy')
                                          AND TO_DATE ('31/3/2026',
                                                       'dd/mm/yyyy')
                  AND EXISTS
                          (SELECT 1
                             FROM cred_validos cr
                            WHERE rc.COD_EMPRESA = cr.CODIGO_EMPRESA
                              AND rc.NO_CREDITO     = cr.NO_CREDITO)
         GROUP BY rc.COD_EMPRESA, rc.NO_CREDITO),
    -- ===========================================================================
    -- 5. Historico saldos: inicio (28/2) y cierre (31/3) en un solo scan
    --    PR_CREDITOS_HI y PR_CREDITOS_DIF_HI se leen UNA SOLA VEZ
    -- ===========================================================================
    hi_saldos
    AS
        (  SELECT /*+ MATERIALIZE PARALLEL(16) USE_HASH(hs hsd) */
                  hs.CODIGO_EMPRESA,
                  hs.NO_CREDITO,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                          THEN
                                hs.MONTO_PAGADO_PRINCIPAL
                              - NVL (hsd.MONTO_CAPITAL_DIF, 0)
                      END)    AS saldo_fin,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                          THEN
                                hs.MONTO_PAGADO_PRINCIPAL
                              - NVL (hsd.MONTO_CAPITAL_DIF, 0)
                      END)    AS saldo_ini,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                          THEN
                              NVL (hsd.MONTO_PAGADO_CAPITAL_DIF, 0)
                      END)    AS cap_dif_ini,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                          THEN
                              NVL (hsd.MONTO_PAGADO_CAPITAL_DIF, 0)
                      END)    AS cap_dif_fin
             FROM PR.PR_CREDITOS_HI hs
                  LEFT JOIN PR.PR_CREDITOS_DIF_HI hsd
                      ON     hs.CODIGO_EMPRESA  = hsd.COD_EMPRESA
                         AND hs.NO_CREDITO      = hsd.NO_CREDITO
                         AND hs.FEC_REGISTRO_HI = hsd.FEC_REGISTRO_HI
            WHERE     hs.FEC_REGISTRO_HI IN
                          (TO_DATE ('28/2/2026', 'dd/mm/yyyy'),
                           TO_DATE ('31/3/2026', 'dd/mm/yyyy'))
                  AND EXISTS
                          (SELECT 1
                             FROM cred_validos cr
                            WHERE hs.CODIGO_EMPRESA = cr.CODIGO_EMPRESA
                              AND hs.NO_CREDITO     = cr.NO_CREDITO)
         GROUP BY hs.CODIGO_EMPRESA, hs.NO_CREDITO),
    -- ===========================================================================
    -- 6. Diferidos post historico: inicio y cierre
    -- ===========================================================================
    dif_post_hi
    AS
        (  SELECT /*+ MATERIALIZE PARALLEL(16) */
                  hs.COD_EMPRESA,
                  hs.NO_CREDITO,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                          THEN
                              hs.MONTO_PAGADO_CAPITAL_DIF
                      END)    AS cap_dif_post_ini,
                  SUM (
                      CASE
                          WHEN hs.FEC_REGISTRO_HI =
                               TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                          THEN
                              hs.MONTO_PAGADO_CAPITAL_DIF
                      END)    AS cap_dif_post_fin
             FROM PR.pr_cred_dif_post_hi hs
            WHERE     hs.FEC_REGISTRO_HI IN
                          (TO_DATE ('28/2/2026', 'dd/mm/yyyy'),
                           TO_DATE ('31/3/2026', 'dd/mm/yyyy'))
                  AND EXISTS
                          (SELECT 1
                             FROM cred_validos cr
                            WHERE hs.COD_EMPRESA = cr.CODIGO_EMPRESA
                              AND hs.NO_CREDITO     = cr.NO_CREDITO)
         GROUP BY hs.COD_EMPRESA, hs.NO_CREDITO),
    -- ===========================================================================
    -- 7. Tipo SBEF: solo para tramites del conjunto filtrado
    --    Menos llamadas a pr_utl2.obt_tipo_sbef (PL/SQL row-by-row)
    -- ===========================================================================
    tip_sbef
    AS
        (SELECT /*+ MATERIALIZE NO_MERGE */
                t.CODIGO_EMPRESA   AS COD_EMPRESA,
                t.NUM_TRAMITE,
                pr_utl2.obt_tipo_sbef (t.CODIGO_EMPRESA, t.NUM_TRAMITE)    AS tip_cred
           FROM (SELECT DISTINCT cv.CODIGO_EMPRESA, cv.NUM_TRAMITE
                   FROM cred_validos cv) t)
-- ===========================================================================
-- Query final: cred_validos ya tiene tramite/credito/oficial pre-resueltos
-- Solo resta unir tablas de dimension (pequenas) y CTEs pre-agregados
-- Eliminadas re-lecturas de PR_CREDITOS, PR_TRAMITE, RH_FUNCIONARIOS,
-- RH_CARGOS y PA.PERSONAS(ofi)
-- ===========================================================================
  SELECT /*+ PARALLEL(16) */
         emp.TIT_REPORTES
             AS empresa,
         suc.DESCRIPCION
             AS sucursal,
         uni.DESCRIPCION
             AS agencia,
         cv.NUM_TRAMITE,
         cv.NO_CREDITO,
         prs.COD_PERSONA,
         cli.NOMBRE
             AS NOMBRE_DEUDOR,
         ts.tip_cred,
         tcs.DES_TIP_CRED_S
             AS des_tip_cred,
         est.ABREV_ESTADO
             AS estado,
         prod.DES_PRODUCTO
             AS producto,
         ng.DESCRIPCION
             AS unidad_negocios,
         cv.nombre_oficial,
         cv.cargo,
         NVL (rc.capital_pagado, 0)
             AS capital_pagado,
         NVL (rc.interes_pagado, 0)
             AS interes_pagado,
         NVL (rd.capital_pagado_dif, 0)
             AS capital_pagado_dif,
         NVL (rd.interes_pagado_dif, 0)
             AS interes_pagado_dif,
         NVL (rc.punitorios, 0)
             AS punitorios,
         NVL (rd.penal_dif, 0)
             AS penal_dif,
         NVL (hs.saldo_fin, 0) - NVL (hs.saldo_ini, 0)
             AS capital_pagado_hs,
         NVL (hs.cap_dif_ini, 0) - NVL (hs.cap_dif_fin, 0)
             AS capital_pagado_df_hs,
         NVL (dp.cap_dif_post_ini, 0) - NVL (dp.cap_dif_post_fin, 0)
             AS capital_pagado_df_post_hs
    FROM cred_validos cv
         INNER JOIN PR.PERSONAS_X_PR_TRAMITE prs
             ON     cv.CODIGO_EMPRESA = prs.COD_EMPRESA
                AND cv.NUM_TRAMITE    = prs.NUM_TRAMITE
                AND prs.IND_TITULAR   = 'S'
         INNER JOIN PR.PR_ESTADOS_CREDITO est
             ON cv.ESTADO = est.CODIGO_ESTADO
         INNER JOIN BCG_B2000.CG_UNIDADES_EJECUTORAS uni
             ON     cv.CODIGO_EMPRESA   = uni.CODIGO_EMPRESA
                AND cv.UNIDAD_EJECUTORA = uni.UNIDAD_EJECUTORA
         INNER JOIN BCG_B2000.CG_SUCURSALES suc
             ON     uni.CODIGO_EMPRESA  = suc.CODIGO_EMPRESA
                AND uni.CODIGO_SUCURSAL = suc.CODIGO_SUCURSAL
         INNER JOIN PA.EMPRESA emp
             ON cv.CODIGO_EMPRESA = emp.COD_EMPRESA
         INNER JOIN PR.PR_TIP_PRODUCTO prod
             ON     cv.CODIGO_EMPRESA    = prod.COD_EMPRESA
                AND cv.COD_TIP_OPERACION = prod.COD_TIP_OPERACION
                AND cv.COD_TIP_PRODUCTO  = prod.COD_TIP_PRODUCTO
         INNER JOIN PR.PR_UNI_EJE_X_UNI_NEG ung
             ON     cv.CODIGO_EMPRESA   = ung.COD_EMPRESA
                AND cv.UNIDAD_EJECUTORA = ung.UNIDAD_EJECUTORA
         INNER JOIN PR.PR_UNIDADES_NEGOCIO ng
             ON     ung.COD_EMPRESA     = ng.COD_EMPRESA
                AND ung.UNIDAD_NEGOCIOS = ng.UNIDAD_NEGOCIOS
         INNER JOIN tip_sbef ts
             ON     cv.CODIGO_EMPRESA = ts.COD_EMPRESA
                AND cv.NUM_TRAMITE    = ts.NUM_TRAMITE
         LEFT JOIN PR.PR_TIP_CREDITO_SUPER tcs
             ON     tcs.COD_EMPRESA    = cv.CODIGO_EMPRESA
                AND tcs.COD_TIP_CRED_S = ts.tip_cred
         LEFT JOIN PA.PERSONAS cli
             ON prs.COD_PERSONA = cli.COD_PERSONA
         LEFT JOIN rec rc
             ON     rc.CODIGO_EMPRESA = cv.CODIGO_EMPRESA
                AND rc.NO_CREDITO     = cv.NO_CREDITO
         LEFT JOIN rec_dif rd
             ON     rd.COD_EMPRESA = cv.CODIGO_EMPRESA
                AND rd.NO_CREDITO  = cv.NO_CREDITO
         LEFT JOIN hi_saldos hs
             ON     hs.CODIGO_EMPRESA = cv.CODIGO_EMPRESA
                AND hs.NO_CREDITO     = cv.NO_CREDITO
         LEFT JOIN dif_post_hi dp
             ON     dp.COD_EMPRESA = cv.CODIGO_EMPRESA
                AND dp.NO_CREDITO  = cv.NO_CREDITO
ORDER BY cv.NUM_TRAMITE;

SPOOL OFF

-- Restaurar configuracion de paralelismo
ALTER SESSION DISABLE PARALLEL QUERY;

set sqlformat default
SET FEEDBACK ON

/*
  SELECT emp.TIT_REPORTES
             AS empresa,
         suc.DESCRIPCION
             AS sucursal,
         uni.DESCRIPCION
             AS agencia,
         tra.NUM_TRAMITE,
         cre.NO_CREDITO,
         prs.COD_PERSONA,
         cli.NOMBRE
             AS NOMBRE_DEUDOR,
         pr_utl2.obt_tipo_sbef (tra.COD_EMPRESA, tra.NUM_TRAMITE)
             AS tip_cred,
         (SELECT q.DES_TIP_CRED_S
            FROM PR.PR_TIP_CREDITO_SUPER q
           WHERE     q.COD_EMPRESA = tra.cod_empresa
                 AND q.COD_TIP_CRED_S =
                     pr_utl2.obt_tipo_sbef (tra.COD_EMPRESA, tra.NUM_TRAMITE))
             AS des_tip_cred,
         est.ABREV_ESTADO
             AS estado,
         prod.DES_PRODUCTO
             AS producto,
         ng.DESCRIPCION
             AS unidad_negocios,
         ofi.NOMBRE
             AS nombre_oficial,
         crg.DESCRIPCION
             AS cargo,
         (SELECT NVL (SUM (rc.MONTO_AMORTIZACION), 0)
            FROM PR.PR_RECIBOS rc
           WHERE     rc.CODIGO_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_CREACION_RECIBO BETWEEN TO_DATE ('1/3/2026',
                                                               'dd/mm/yyyy')
                                                  AND TO_DATE ('31/3/2026',
                                                               'dd/mm/yyyy'))
             AS capital_pagado,
         (SELECT NVL (SUM (rc.MONTO_INTERESES), 0)
            FROM PR.PR_RECIBOS rc
           WHERE     rc.CODIGO_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_CREACION_RECIBO BETWEEN TO_DATE ('1/3/2026',
                                                               'dd/mm/yyyy')
                                                  AND TO_DATE ('31/3/2026',
                                                               'dd/mm/yyyy'))
             AS interes_pagado,
         (SELECT NVL (SUM (rc.MONTO_CAPITAL_PAGADO), 0)
            FROM PR.PR_RECIBOS_DIF rc
           WHERE     rc.COD_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_RECIBO BETWEEN TO_DATE ('1/3/2026', 'dd/mm/yyyy')
                                         AND TO_DATE ('31/3/2026',
                                                      'dd/mm/yyyy'))
             AS capital_pagado_dif,
         (SELECT NVL (SUM (rc.MONTO_INTERESES_PAGADO), 0)
            FROM PR.PR_RECIBOS_DIF rc
           WHERE     rc.COD_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_RECIBO BETWEEN TO_DATE ('1/3/2026', 'dd/mm/yyyy')
                                         AND TO_DATE ('31/3/2026',
                                                      'dd/mm/yyyy'))
             AS interes_pagado_dif,
             
             (SELECT NVL (SUM (rc.MONTO_PUNITORIOS), 0)
            FROM PR.PR_RECIBOS rc
           WHERE     rc.CODIGO_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_CREACION_RECIBO BETWEEN TO_DATE ('1/3/2026',
                                                               'dd/mm/yyyy')
                                                  AND TO_DATE ('31/3/2026',
                                                               'dd/mm/yyyy'))
             AS punitorios,
             
            (SELECT NVL (SUM (rc.MONTO_PUNITORIOS), 0)
            FROM PR.PR_RECIBOS_DIF rc
           WHERE     rc.COD_EMPRESA = cre.codigo_empresa
                 AND rc.no_credito = cre.no_credito
                 AND rc.FECHA_RECIBO BETWEEN TO_DATE ('1/3/2026', 'dd/mm/yyyy')
                                         AND TO_DATE ('31/3/2026',
                                                      'dd/mm/yyyy'))
             AS penal_dif, 
             
         
         (  (SELECT hs.MONTO_PAGADO_PRINCIPAL - NVL(hsd.MONTO_CAPITAL_DIF, 0)
               FROM PR.PR_CREDITOS_HI hs
               left join PR.PR_CREDITOS_DIF_HI hsd
               on hs.CODIGO_EMPRESA = hsd.COD_EMPRESA
               and hs.NO_CREDITO = hsd.NO_CREDITO
               and hs.FEC_REGISTRO_HI = hsd.FEC_REGISTRO_HI
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE  ('31/3/2026', 'dd/mm/yyyy') 
                    AND hs.CODIGO_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito)
          - (SELECT hs.MONTO_PAGADO_PRINCIPAL - NVL(hsd.MONTO_CAPITAL_DIF, 0)
               FROM PR.PR_CREDITOS_HI hs
               left join PR.PR_CREDITOS_DIF_HI hsd
               on hs.CODIGO_EMPRESA = hsd.COD_EMPRESA
               and hs.NO_CREDITO = hsd.NO_CREDITO
               and hs.FEC_REGISTRO_HI = hsd.FEC_REGISTRO_HI
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                    AND hs.CODIGO_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito))
             AS capital_pagado_hs,
         (  (SELECT hs.MONTO_PAGADO_CAPITAL_DIF
               FROM PR.PR_CREDITOS_DIF_HI hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito)
          - (SELECT hs.MONTO_PAGADO_CAPITAL_DIF
               FROM PR.PR_CREDITOS_DIF_HI hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito))
             AS capital_pagado_df_hs,
             
           (  (SELECT hs.MONTO_PAGADO_CAPITAL_DIF
               FROM PR.pr_cred_dif_post_hi hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito)
          - (SELECT hs.MONTO_PAGADO_CAPITAL_DIF
               FROM PR.pr_cred_dif_post_hi hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito))
             AS capital_pagado_df_post_hs,
             
             (  (SELECT hs.MONTO_PAGADO_INT_DEV_DIF + hs.MONTO_PAGADO_INTERES_DIF
               FROM PR.pr_cred_dif_post_hi hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('28/2/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito)
          - (SELECT hs.MONTO_PAGADO_INT_DEV_DIF + hs.MONTO_PAGADO_INTERES_DIF
               FROM PR.pr_cred_dif_post_hi hs
              WHERE     hs.FEC_REGISTRO_HI =
                        TO_DATE ('31/3/2026', 'dd/mm/yyyy')
                    AND hs.COD_EMPRESA = cre.codigo_empresa
                    AND hs.NO_CREDITO = cre.no_credito))
             AS int_pagado_df_post_hs
    FROM PR.PR_TRAMITE tra
         INNER JOIN PR.PR_CREDITOS cre
             ON     tra.COD_EMPRESA = cre.CODIGO_EMPRESA
                AND tra.NUM_TRAMITE = cre.NUM_TRAMITE
         inner join PR.PR_CREDITOS_HI his
         on cre.CODIGO_EMPRESA = his.CODIGO_EMPRESA
         and cre.NO_CREDITO = his.NO_CREDITO
         and his.FEC_REGISTRO_HI = TO_DATE ('31/3/2026', 'dd/mm/yyyy')
         INNER JOIN PR.PERSONAS_X_PR_TRAMITE prs
             ON     tra.COD_EMPRESA = prs.COD_EMPRESA
                AND tra.NUM_TRAMITE = prs.NUM_TRAMITE
                AND prs.IND_TITULAR = 'S'
         INNER JOIN PR.PR_ESTADOS_CREDITO est ON cre.ESTADO = est.CODIGO_ESTADO
         INNER JOIN BCG_B2000.CG_UNIDADES_EJECUTORAS uni
             ON     tra.COD_EMPRESA = uni.CODIGO_EMPRESA
                AND tra.UNIDAD_EJECUTORA = uni.UNIDAD_EJECUTORA
         INNER JOIN BCG_B2000.CG_SUCURSALES suc
             ON     uni.CODIGO_EMPRESA = suc.CODIGO_EMPRESA
                AND uni.CODIGO_SUCURSAL = suc.CODIGO_SUCURSAL
         INNER JOIN PA.EMPRESA emp ON tra.COD_EMPRESA = emp.COD_EMPRESA
         INNER JOIN PR.PR_TIP_PRODUCTO prod
             ON     tra.COD_EMPRESA = prod.COD_EMPRESA
                AND tra.COD_TIP_OPERACION = prod.COD_TIP_OPERACION
                AND tra.COD_TIP_PRODUCTO = prod.COD_TIP_PRODUCTO
         INNER JOIN PR.PR_UNI_EJE_X_UNI_NEG ung
             ON     tra.COD_EMPRESA = ung.COD_EMPRESA
                AND tra.UNIDAD_EJECUTORA = ung.UNIDAD_EJECUTORA
         INNER JOIN PR.PR_UNIDADES_NEGOCIO ng
             ON     ung.COD_EMPRESA = ng.COD_EMPRESA
                AND ung.UNIDAD_NEGOCIOS = ng.UNIDAD_NEGOCIOS
         LEFT JOIN PA.PERSONAS cli ON prs.COD_PERSONA = cli.COD_PERSONA
         LEFT JOIN PA.PERSONAS ofi
             ON NVL (cre.CODIGO_ANALISTA, cre.CODIGO_EJECUTIVO) =
                ofi.COD_PERSONA
         LEFT JOIN RH.RH_FUNCIONARIOS fun
             ON     fun.COD_EMPRESA = '1'
                AND NVL (cre.CODIGO_ANALISTA, cre.CODIGO_EJECUTIVO) =
                    fun.ID_FUNCIONARIO
               and fun.IND_ACTIVO = 'S'
         LEFT JOIN RH.RH_CARGOS crg
             ON     fun.COD_EMPRESA = crg.COD_EMPRESA
                AND fun.ID_CARGO = crg.ID_CARGO
   WHERE     his.Estado IN ('D',
                            'E',
                            'V',
                            'J',
                            'G',
                            'I',
                            'L',
                            'T', 'C')
         AND crg.ID_CARGO IN ('162570', '162670', '163576')
         and not exists(select 1 from PR.PR_CREDITOS_DIF_POST d
         where d.COD_EMPRESA = cre.CODIGO_EMPRESA and d.NO_CREDITO = cre.NO_CREDITO)
ORDER BY tra.NUM_TRAMITE;
*/