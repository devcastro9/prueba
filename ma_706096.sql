-- MA: 706096
-- ===============
-- Optimizado v2:
-- - 3 queries unificadas en 1 (ID_CARGO IN (...))
-- - pr_utl2.obt_tipo_sbef calculado 1 sola vez por fila via CTE
-- - FULL hints eliminados; LEADING desde crg→fun (filtro selectivo de cargo)
-- - Subqueries agregados filtrados con cred_keys (evita agregar filas innecesarias)
-- - CTAS: resultado materializado en tabla temporal, 3 SPOOLs triviales
-- - PARALLEL(16), USE_HASH, NO_MERGE mantenidos donde aplica
SET FEEDBACK OFF
SET SQLFORMAT CSV

-- =====================================================
-- Paso 1: Ejecutar query pesada UNA sola vez → tabla temporal
-- =====================================================
CREATE TABLE tmp_ma_706096 NOLOGGING PARALLEL 16 AS
WITH creditos_base AS (
    SELECT /*+ PARALLEL(16)
               LEADING(crg fun cre tra his prs)
               USE_HASH(fun cre tra his prs)
               PQ_DISTRIBUTE(cre HASH HASH) PQ_DISTRIBUTE(his HASH HASH) PQ_DISTRIBUTE(prs HASH HASH) */
           tra.COD_EMPRESA,
           tra.NUM_TRAMITE,
           tra.UNIDAD_EJECUTORA,
           tra.COD_TIP_OPERACION,
           tra.COD_TIP_PRODUCTO,
           cre.CODIGO_EMPRESA,
           cre.NO_CREDITO,
           cre.ESTADO,
           cre.CODIGO_ANALISTA,
           cre.CODIGO_EJECUTIVO,
           his.MONTO_PAGADO_PRINCIPAL,
           prs.COD_PERSONA,
           crg.ID_CARGO,
           crg.DESCRIPCION AS cargo_desc,
           (SELECT pr_utl2.obt_tipo_sbef(tra.COD_EMPRESA, tra.NUM_TRAMITE) FROM DUAL) AS tip_cred
      FROM RH.RH_CARGOS crg
           INNER JOIN RH.RH_FUNCIONARIOS fun
               ON fun.COD_EMPRESA = crg.COD_EMPRESA AND fun.ID_CARGO = crg.ID_CARGO
              AND fun.COD_EMPRESA = '1' AND fun.IND_ACTIVO = 'S'
           INNER JOIN PR.PR_CREDITOS cre
               ON NVL(cre.CODIGO_ANALISTA, cre.CODIGO_EJECUTIVO) = fun.ID_FUNCIONARIO
           INNER JOIN PR.PR_TRAMITE tra
               ON tra.COD_EMPRESA = cre.CODIGO_EMPRESA AND tra.NUM_TRAMITE = cre.NUM_TRAMITE
           INNER JOIN PR.PR_CREDITOS_HI his
               ON cre.CODIGO_EMPRESA = his.CODIGO_EMPRESA AND cre.NO_CREDITO = his.NO_CREDITO
              AND his.FEC_REGISTRO_HI = TO_DATE('31/3/2026','dd/mm/yyyy')
           INNER JOIN PR.PERSONAS_X_PR_TRAMITE prs
               ON tra.COD_EMPRESA = prs.COD_EMPRESA AND tra.NUM_TRAMITE = prs.NUM_TRAMITE
              AND prs.IND_TITULAR = 'S'
     WHERE crg.ID_CARGO IN ('162570','162670','163576')
       AND his.Estado IN ('D','E','V','J','G','I','L','T','C')
),
cred_keys AS (
    SELECT /*+ NO_MERGE MATERIALIZE */
           DISTINCT CODIGO_EMPRESA, NO_CREDITO
      FROM creditos_base
),
rc_agg AS (
    SELECT /*+ NO_MERGE PARALLEL(rc,16) */
           rc.CODIGO_EMPRESA, rc.NO_CREDITO,
           SUM(rc.MONTO_AMORTIZACION) AS capital_pagado,
           SUM(rc.MONTO_INTERESES)    AS interes_pagado,
           SUM(rc.MONTO_PUNITORIOS)   AS punitorios
      FROM PR.PR_RECIBOS rc
           INNER JOIN cred_keys ck
               ON rc.CODIGO_EMPRESA = ck.CODIGO_EMPRESA AND rc.NO_CREDITO = ck.NO_CREDITO
     WHERE rc.FECHA_CREACION_RECIBO BETWEEN TO_DATE('1/3/2026','dd/mm/yyyy')
                                        AND TO_DATE('31/3/2026','dd/mm/yyyy')
     GROUP BY rc.CODIGO_EMPRESA, rc.NO_CREDITO
),
rc_dif_agg AS (
    SELECT /*+ NO_MERGE PARALLEL(rc,16) */
           rc.COD_EMPRESA, rc.NO_CREDITO,
           SUM(rc.MONTO_CAPITAL_PAGADO)   AS capital_pagado_dif,
           SUM(rc.MONTO_INTERESES_PAGADO) AS interes_pagado_dif,
           SUM(rc.MONTO_PUNITORIOS)       AS penal_dif
      FROM PR.PR_RECIBOS_DIF rc
           INNER JOIN cred_keys ck
               ON rc.COD_EMPRESA = ck.CODIGO_EMPRESA AND rc.NO_CREDITO = ck.NO_CREDITO
     WHERE rc.FECHA_RECIBO BETWEEN TO_DATE('1/3/2026','dd/mm/yyyy')
                                AND TO_DATE('31/3/2026','dd/mm/yyyy')
     GROUP BY rc.COD_EMPRESA, rc.NO_CREDITO
),
dif_agg AS (
    SELECT /*+ NO_MERGE PARALLEL(d,16) */
           d.COD_EMPRESA, d.NO_CREDITO,
           MAX(CASE WHEN d.FEC_REGISTRO_HI = TO_DATE('28/2/2026','dd/mm/yyyy') THEN d.MONTO_CAPITAL_DIF END)        AS cap_dif_ini,
           MAX(CASE WHEN d.FEC_REGISTRO_HI = TO_DATE('31/3/2026','dd/mm/yyyy') THEN d.MONTO_CAPITAL_DIF END)        AS cap_dif_fin,
           MAX(CASE WHEN d.FEC_REGISTRO_HI = TO_DATE('28/2/2026','dd/mm/yyyy') THEN d.MONTO_PAGADO_CAPITAL_DIF END) AS pag_cap_ini,
           MAX(CASE WHEN d.FEC_REGISTRO_HI = TO_DATE('31/3/2026','dd/mm/yyyy') THEN d.MONTO_PAGADO_CAPITAL_DIF END) AS pag_cap_fin
      FROM PR.PR_CREDITOS_DIF_HI d
           INNER JOIN cred_keys ck
               ON d.COD_EMPRESA = ck.CODIGO_EMPRESA AND d.NO_CREDITO = ck.NO_CREDITO
     WHERE d.FEC_REGISTRO_HI IN (TO_DATE('28/2/2026','dd/mm/yyyy'), TO_DATE('31/3/2026','dd/mm/yyyy'))
     GROUP BY d.COD_EMPRESA, d.NO_CREDITO
),
post_agg AS (
    SELECT /*+ NO_MERGE PARALLEL(p,16) */
           p.COD_EMPRESA, p.NO_CREDITO,
           MAX(CASE WHEN p.FEC_REGISTRO_HI = TO_DATE('28/2/2026','dd/mm/yyyy') THEN p.MONTO_PAGADO_CAPITAL_DIF END) AS cap_ini,
           MAX(CASE WHEN p.FEC_REGISTRO_HI = TO_DATE('31/3/2026','dd/mm/yyyy') THEN p.MONTO_PAGADO_CAPITAL_DIF END) AS cap_fin,
           MAX(CASE WHEN p.FEC_REGISTRO_HI = TO_DATE('28/2/2026','dd/mm/yyyy') THEN p.MONTO_PAGADO_INT_DEV_DIF + p.MONTO_PAGADO_INTERES_DIF END) AS int_ini,
           MAX(CASE WHEN p.FEC_REGISTRO_HI = TO_DATE('31/3/2026','dd/mm/yyyy') THEN p.MONTO_PAGADO_INT_DEV_DIF + p.MONTO_PAGADO_INTERES_DIF END) AS int_fin
      FROM PR.pr_cred_dif_post_hi p
           INNER JOIN cred_keys ck
               ON p.COD_EMPRESA = ck.CODIGO_EMPRESA AND p.NO_CREDITO = ck.NO_CREDITO
     WHERE p.FEC_REGISTRO_HI IN (TO_DATE('28/2/2026','dd/mm/yyyy'), TO_DATE('31/3/2026','dd/mm/yyyy'))
     GROUP BY p.COD_EMPRESA, p.NO_CREDITO
)
SELECT cb.ID_CARGO AS cargo_id,
       emp.TIT_REPORTES AS empresa,
       suc.DESCRIPCION AS sucursal,
       uni.DESCRIPCION AS agencia,
       cb.NUM_TRAMITE,
       cb.NO_CREDITO,
       cb.COD_PERSONA,
       cli.NOMBRE AS NOMBRE_DEUDOR,
       cb.tip_cred,
       sbef_q.DES_TIP_CRED_S AS des_tip_cred,
       est.ABREV_ESTADO AS estado,
       prod.DES_PRODUCTO AS producto,
       ng.DESCRIPCION AS unidad_negocios,
       ofi.NOMBRE AS nombre_oficial,
       cb.cargo_desc AS cargo,
       NVL(rc_agg.capital_pagado, 0) AS capital_pagado,
       NVL(rc_agg.interes_pagado, 0) AS interes_pagado,
       NVL(rc_dif_agg.capital_pagado_dif, 0) AS capital_pagado_dif,
       NVL(rc_dif_agg.interes_pagado_dif, 0) AS interes_pagado_dif,
       NVL(rc_agg.punitorios, 0) AS punitorios,
       NVL(rc_dif_agg.penal_dif, 0) AS penal_dif,
       (NVL(cb.MONTO_PAGADO_PRINCIPAL, 0) - NVL(dif_agg.cap_dif_fin, 0))
     - (NVL(hs_ini.MONTO_PAGADO_PRINCIPAL, 0) - NVL(dif_agg.cap_dif_ini, 0)) AS capital_pagado_hs,
       NVL(dif_agg.pag_cap_ini, 0) - NVL(dif_agg.pag_cap_fin, 0) AS capital_pagado_df_hs,
       NVL(post_agg.cap_ini, 0) - NVL(post_agg.cap_fin, 0) AS capital_pagado_df_post_hs,
       (NVL(post_agg.int_ini, 0)) - (NVL(post_agg.int_fin, 0)) AS int_pagado_df_post_hs
  FROM creditos_base cb
       INNER JOIN PR.PR_ESTADOS_CREDITO est ON cb.ESTADO = est.CODIGO_ESTADO
       INNER JOIN BCG_B2000.CG_UNIDADES_EJECUTORAS uni
           ON cb.COD_EMPRESA = uni.CODIGO_EMPRESA AND cb.UNIDAD_EJECUTORA = uni.UNIDAD_EJECUTORA
       INNER JOIN BCG_B2000.CG_SUCURSALES suc
           ON uni.CODIGO_EMPRESA = suc.CODIGO_EMPRESA AND uni.CODIGO_SUCURSAL = suc.CODIGO_SUCURSAL
       INNER JOIN PA.EMPRESA emp ON cb.COD_EMPRESA = emp.COD_EMPRESA
       INNER JOIN PR.PR_TIP_PRODUCTO prod
           ON cb.COD_EMPRESA = prod.COD_EMPRESA AND cb.COD_TIP_OPERACION = prod.COD_TIP_OPERACION
          AND cb.COD_TIP_PRODUCTO = prod.COD_TIP_PRODUCTO
       INNER JOIN PR.PR_UNI_EJE_X_UNI_NEG ung
           ON cb.COD_EMPRESA = ung.COD_EMPRESA AND cb.UNIDAD_EJECUTORA = ung.UNIDAD_EJECUTORA
       INNER JOIN PR.PR_UNIDADES_NEGOCIO ng
           ON ung.COD_EMPRESA = ng.COD_EMPRESA AND ung.UNIDAD_NEGOCIOS = ng.UNIDAD_NEGOCIOS
       LEFT JOIN PA.PERSONAS cli ON cb.COD_PERSONA = cli.COD_PERSONA
       LEFT JOIN PA.PERSONAS ofi
           ON NVL(cb.CODIGO_ANALISTA, cb.CODIGO_EJECUTIVO) = ofi.COD_PERSONA
       LEFT JOIN PR.PR_TIP_CREDITO_SUPER sbef_q
           ON sbef_q.COD_EMPRESA = cb.COD_EMPRESA
          AND sbef_q.COD_TIP_CRED_S = cb.tip_cred
       LEFT JOIN rc_agg
           ON cb.CODIGO_EMPRESA = rc_agg.CODIGO_EMPRESA AND cb.NO_CREDITO = rc_agg.NO_CREDITO
       LEFT JOIN rc_dif_agg
           ON cb.CODIGO_EMPRESA = rc_dif_agg.COD_EMPRESA AND cb.NO_CREDITO = rc_dif_agg.NO_CREDITO
       LEFT JOIN PR.PR_CREDITOS_HI hs_ini
           ON cb.CODIGO_EMPRESA = hs_ini.CODIGO_EMPRESA AND cb.NO_CREDITO = hs_ini.NO_CREDITO
          AND hs_ini.FEC_REGISTRO_HI = TO_DATE('28/2/2026','dd/mm/yyyy')
       LEFT JOIN dif_agg
           ON cb.CODIGO_EMPRESA = dif_agg.COD_EMPRESA AND cb.NO_CREDITO = dif_agg.NO_CREDITO
       LEFT JOIN post_agg
           ON cb.CODIGO_EMPRESA = post_agg.COD_EMPRESA AND cb.NO_CREDITO = post_agg.NO_CREDITO;

-- =====================================================
-- Paso 2: 3 SPOOLs triviales desde la tabla temporal
-- =====================================================
SPOOL ma_706096_162570.csv
SELECT * FROM tmp_ma_706096 WHERE cargo_id = '162570' ORDER BY num_tramite;
SPOOL OFF

SPOOL ma_706096_162670.csv
SELECT * FROM tmp_ma_706096 WHERE cargo_id = '162670' ORDER BY num_tramite;
SPOOL OFF

SPOOL ma_706096_163576.csv
SELECT * FROM tmp_ma_706096 WHERE cargo_id = '163576' ORDER BY num_tramite;
SPOOL OFF

-- =====================================================
-- Paso 3: Limpieza
-- =====================================================
DROP TABLE tmp_ma_706096 PURGE;

SET SQLFORMAT DEFAULT
SET FEEDBACK ON
