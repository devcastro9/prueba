DECLARE
    v_fecha_hoy  DATE := TRUNC(SYSDATE);
    v_error      VARCHAR2(9000);
    v_pass       PLS_INTEGER := 0;
    v_fail       PLS_INTEGER := 0;
    v_total      PLS_INTEGER := 0;

    -- Cursor basado en casos_prueba.sql:
    -- personas con operaciones activas en mas de una empresa Y mas de un tipo de operacion.
    CURSOR c_casos IS
        SELECT *
          FROM (
                WITH
                    base AS (
                        SELECT /*+ materialize parallel(p, 4) parallel(o, 4) */
                               p.cod_persona,
                               o.cod_empresa,
                               o.cod_tip_operacion
                          FROM pr.personas_x_pr_tramite p
                               INNER JOIN pr.pr_tramite o
                                   ON     p.cod_empresa = o.cod_empresa
                                      AND p.num_tramite  = o.num_tramite
                         WHERE o.codigo_estado NOT IN ('C', 'B', 'X', 'N', 'M', 'O', 'R')
                    ),
                    persona_filtrada AS (
                        SELECT /*+ materialize */
                               cod_persona
                          FROM base
                      GROUP BY cod_persona
                        HAVING     COUNT(DISTINCT cod_empresa)       > 1
                               AND COUNT(DISTINCT cod_tip_operacion) > 1
                    )
                SELECT /*+ parallel(b, 4) parallel(pf, 4) */
                       b.cod_persona,
                       COUNT(DISTINCT b.cod_empresa)       AS cantidad_empresas,
                       COUNT(DISTINCT b.cod_tip_operacion) AS cantidad_tipos_operacion,
                       COUNT(*)                            AS total_tramite
                  FROM base b
                       INNER JOIN persona_filtrada pf ON b.cod_persona = pf.cod_persona
              GROUP BY b.cod_persona
              ORDER BY cantidad_empresas DESC, cantidad_tipos_operacion DESC, total_tramite DESC
               )
         WHERE ROWNUM <= 500; -- limitar a los 500 casos mas representativos

BEGIN
    DBMS_OUTPUT.PUT_LINE('=== PRUEBAS AUTOMATICAS ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || ' ===');

    FOR rec IN c_casos LOOP
        v_total := v_total + 1;
        v_error := NULL;

        pr.carga_prr0402m(rec.cod_persona, v_fecha_hoy, v_error);

        IF v_error IS NULL THEN
            v_pass := v_pass + 1;
            DBMS_OUTPUT.PUT_LINE('PASS | cod_persona=' || rec.cod_persona
                || ' | empresas='       || rec.cantidad_empresas
                || ' | tipos_op='       || rec.cantidad_tipos_operacion
                || ' | tramites='       || rec.total_tramite);
        ELSE
            v_fail := v_fail + 1;
            DBMS_OUTPUT.PUT_LINE('FAIL | cod_persona=' || rec.cod_persona
                || ' | error=' || v_error);
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== RESUMEN: total=' || v_total
        || '  pass=' || v_pass
        || '  fail=' || v_fail || ' ===');

    IF v_fail > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, v_fail || ' prueba(s) fallaron.');
    END IF;
END;
/
