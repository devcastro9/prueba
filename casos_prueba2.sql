-- Personas (titulares/codeudores) con más de dos trámites activos del tipo de operación 122.
-- DOP paralelo: 4 (ajustar según CPUs del servidor).
WITH
    -- base: join único entre personas y trámites activos.
    -- Para incluir/excluir estados agregar o quitar códigos del NOT IN ('C','B','X','N','M','O','R').
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
          WHERE     o.codigo_estado NOT IN ('C', 'B', 'X', 'N', 'M', 'O', 'R')
            AND     o.cod_tip_operacion = 122),
    -- persona_filtrada: filtra antes del LISTAGG para no procesar personas descartadas.
    -- Umbral: > 2 trámites activos con cod_tip_operacion = 122.
    -- MATERIALIZE: evita que Oracle re-ejecute esta CTE en cada una de las referencias.
    persona_filtrada
    AS
        (  SELECT /*+ materialize */
                  cod_persona
             FROM base
         GROUP BY cod_persona
           HAVING COUNT (*) > 2),
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
    -- listagg_empresas: concatena empresas por persona (formato: 'COD - Descripción, ...').
    listagg_empresas
    AS
        (  SELECT /*+ materialize */
                  cod_persona,
                  LISTAGG (cod_empresa || ' - ' || TIT_REPORTES, ', ')
                      WITHIN GROUP (ORDER BY cod_empresa)    AS empresas
             FROM empresas_distinct
         GROUP BY cod_persona)

-- Proyección final: cantidad de trámites tipo 122 por persona + empresas donde operan.
-- total_tramite = cantidad de trámites activos de tipo 122.
-- MAX() sobre empresas: es 1:1 con cod_persona; evita agrupar por strings largos.
  SELECT /*+ parallel(b, 4) parallel(pf, 4) */
         b.cod_persona,
         COUNT (DISTINCT b.cod_empresa)    AS cantidad_empresas,
         COUNT (*)                         AS total_tramite_122,
         MAX (le.empresas)                 AS empresas
    FROM base b
         INNER JOIN persona_filtrada pf  ON b.cod_persona = pf.cod_persona
         INNER JOIN listagg_empresas le  ON b.cod_persona = le.cod_persona
GROUP BY b.cod_persona
ORDER BY total_tramite_122 DESC,
         cantidad_empresas DESC;