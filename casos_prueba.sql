-- Personas (titulares/codeudores) con operaciones activas en más de una empresa y más de un tipo de operación.
-- DOP paralelo: 4 (ajustar según CPUs del servidor).
WITH
    -- base: join único entre personas y trámites activos.
    -- Para incluir/excluir estados agregar o quitar códigos del NOT IN ('C','B','X','N','M','O').
    base
    AS
        (SELECT /*+ materialize parallel(p, 4) parallel(o, 4) */
                p.cod_persona,
                o.cod_empresa,
                o.cod_tip_operacion,
                o.num_tramite
           FROM pr.personas_x_pr_tramite  p
                INNER JOIN pr.pr_tramite o
                    ON     p.cod_empresa = o.cod_empresa
                       AND p.num_tramite = o.num_tramite
          WHERE     o.codigo_estado NOT IN ('C', 'B', 'X', 'N', 'M', 'O')),
    -- persona_filtrada: filtra antes del LISTAGG para no procesar personas descartadas.
    -- Umbrales: > 1 empresa  Y  > 1 tipo de operación  (cambiar el valor numérico según requerimiento).
    -- MATERIALIZE: evita que Oracle re-ejecute esta CTE en cada una de las 3 referencias.
    persona_filtrada
    AS
        (  SELECT /*+ materialize */
                  cod_persona
             FROM base
         GROUP BY cod_persona
           HAVING     COUNT (DISTINCT cod_empresa)       > 1
                  AND COUNT (DISTINCT cod_tip_operacion) > 1),
    -- tipos_distinct: una fila por (persona, tipo de operación) con su descripción.
    tipos_distinct
    AS
        (SELECT /*+ parallel(b,4) parallel(pf,4) parallel(tp,4) */
           b.cod_persona,
           b.cod_tip_operacion,
           tp.descripcion
    FROM base b
         INNER JOIN persona_filtrada pf ON b.cod_persona = pf.cod_persona
         INNER JOIN pr.pr_tip_operacion tp ON b.cod_tip_operacion = tp.cod_tip_operacion
    GROUP BY b.cod_persona, b.cod_tip_operacion, tp.descripcion),
    -- empresas_distinct: una fila por (persona, empresa) con su descripción.
    empresas_distinct
    AS
        (SELECT /*+ parallel(b, 4) parallel(pf, 4) parallel(pe, 4) */
                b.cod_persona,
                b.cod_empresa,
                pe.TIT_REPORTES
           FROM base  b
                INNER JOIN persona_filtrada pf
                    ON b.cod_persona = pf.cod_persona
                INNER JOIN pa.EMPRESA pe
                    ON b.cod_empresa = pe.cod_empresa
        GROUP BY b.cod_persona, b.cod_empresa, pe.TIT_REPORTES),
    -- listagg_precalc: concatena tipos de operación por persona (formato: 'COD - Descripción, ...').
    listagg_precalc
    AS
        (  SELECT /*+ materialize */
                  cod_persona,
                  LISTAGG (cod_tip_operacion || ' - ' || descripcion, ', ')
                      WITHIN GROUP (ORDER BY cod_tip_operacion)    AS tipos_operacion
             FROM tipos_distinct
         GROUP BY cod_persona),
    -- listagg_empresas: concatena empresas por persona (formato: 'COD - Descripción, ...').
    listagg_empresas
    AS
        (  SELECT /*+ materialize */
                  cod_persona,
                  LISTAGG (cod_empresa || ' - ' || TIT_REPORTES, ', ')
                      WITHIN GROUP (ORDER BY cod_empresa)    AS empresas
             FROM empresas_distinct
         GROUP BY cod_persona)

-- Proyección final: totales por persona + listas concatenadas de empresas y tipos de operación.
-- total_tramite = cantidad de trámites activos (filas en base), no cantidad de tipos.
-- MAX() sobre empresas/tipos_operacion: son 1:1 con el codigo de persona; evita agrupar por strings largos.
  SELECT /*+ parallel(b, 4) parallel(pf, 4) */
         b.cod_persona,
         COUNT (DISTINCT b.cod_empresa)            AS cantidad_empresas,
         COUNT (DISTINCT b.cod_tip_operacion)      AS cantidad_tipos_operacion,
         COUNT (*)                                 AS total_tramite,
         MAX (le.empresas)                         AS empresas,
         MAX (lp.tipos_operacion)                  AS tipos_operacion
    FROM base b
         INNER JOIN persona_filtrada pf  ON b.cod_persona = pf.cod_persona
         INNER JOIN listagg_precalc  lp  ON b.cod_persona = lp.cod_persona
         INNER JOIN listagg_empresas le  ON b.cod_persona = le.cod_persona
GROUP BY b.cod_persona
ORDER BY cantidad_empresas DESC,
         cantidad_tipos_operacion DESC,
         total_tramite DESC;