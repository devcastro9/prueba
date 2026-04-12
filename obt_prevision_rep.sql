-- ============================================================
-- mocastro - FSN-2026-04-05
-- Package de prevision para Oracle Reports PRR0402G.
--
-- Patron cache de sesion: cargar() ejecuta la logica solo cuando
-- los parametros cambian respecto al ultimo llamado. Seguro ante
-- reutilizacion de conexiones en el pool de Oracle Reports.
--
-- Uso en Formula Columns del Data Model:
--   CF_TOT_PREVISION:
--     pr.pkg_obt_prevision.cargar(:p_cod_empresa, :p_cod_cliente, :p_fecha);
--     RETURN pr.pkg_obt_prevision.tot_prevision;
--
--   CF_TOT_PRE_CON:
--     pr.pkg_obt_prevision.cargar(:p_cod_empresa, :p_cod_cliente, :p_fecha);
--     RETURN pr.pkg_obt_prevision.tot_pre_con;
-- ============================================================

CREATE OR REPLACE PACKAGE pr.pkg_obt_prevision AS

    PROCEDURE cargar (
        p_cod_empresa  IN VARCHAR2,
        p_cod_cliente  IN VARCHAR2,
        p_fecha        IN DATE
    );

    FUNCTION tot_prevision RETURN NUMBER;
    FUNCTION tot_pre_con   RETURN NUMBER;
    FUNCTION error_msg     RETURN VARCHAR2;

END pkg_obt_prevision;
/

CREATE OR REPLACE PACKAGE BODY pr.pkg_obt_prevision AS

    -- Estado privado de sesion
    g_tot_prevision NUMBER  := 0;
    g_tot_pre_con   NUMBER  := 0;
    g_error         VARCHAR2(200);
    -- Parametros de la ultima carga: detectan cambio de cliente/empresa/fecha
    -- incluso cuando la conexion es reutilizada desde el pool de Oracle Reports
    g_cod_empresa   VARCHAR2(5);
    g_cod_cliente   VARCHAR2(20);
    g_fecha         DATE;

    -- --------------------------------------------------------
    PROCEDURE cargar (
        p_cod_empresa  IN VARCHAR2,
        p_cod_cliente  IN VARCHAR2,
        p_fecha        IN DATE
    ) IS
        -- Codigos de operacion cacheados (evita releer PA.PARAM_GENERALES por cada fila)
        v_op_cartera     PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
        v_op_comex       PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
        v_op_sobregiro   PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
        v_op_tarjeta     PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
        v_op_factoraje   PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
        v_cod_moneda_usd PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;

        TYPE t_cur_tramite IS REF CURSOR;
        cur_tramite t_cur_tramite;

        TYPE t_rec_tramite IS RECORD (
            num_tramite        PR_TRAMITE.num_tramite%TYPE,
            codigo_estado      PR_TRAMITE.codigo_estado%TYPE,
            cod_moneda         PR_TRAMITE.cod_moneda%TYPE,
            bajo_linea_credito PR_TRAMITE.bajo_linea_credito%TYPE,
            descripcion        PR_TIP_CREDITO.descripcion%TYPE,
            cod_tip_operacion  PR_TRAMITE.cod_tip_operacion%TYPE,
            unidad_ejecutora   PR_TRAMITE.unidad_ejecutora%TYPE
        );
        reg_tramite t_rec_tramite;

        v_num_proceso  PR_PROVISIONES.num_proceso%TYPE;
        v_pre_contin   NUMBER(16,2);
        v_pre_directa  NUMBER(16,2);
    BEGIN
        -- Si los parametros no cambiaron, los datos del cache siguen vigentes
        IF     g_cod_empresa = p_cod_empresa
           AND g_cod_cliente = p_cod_cliente
           AND g_fecha       = p_fecha
        THEN
            RETURN;
        END IF;

        -- Resetear cache para la nueva combinacion de parametros
        g_tot_prevision := 0;
        g_tot_pre_con   := 0;
        g_error         := NULL;

        -- Cachear codigos de operacion una sola vez
        v_cod_moneda_usd := pa.PARAMETRO_GENERAL('PR', 'COD_MONEDA_DOLAR');  -- [25/03/2026]
        v_op_cartera     := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_CARTERA');
        v_op_comex       := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_COMEX');
        v_op_sobregiro   := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_SOBREGIRO');
        v_op_tarjeta     := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_TARJETA');
        v_op_factoraje   := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_FACTORAJE');

        -- Obtiene nro. de proceso de prevision vigente
        SELECT MAX(num_proceso)
          INTO v_num_proceso
          FROM PR_PROVISIONES
         WHERE cod_empresa                  = p_cod_empresa
           AND TRUNC(fec_ult_calificacion) <= p_fecha
           AND provisionado                 = 'S';

        -- Tramites activos del cliente (ANSI JOIN)
        OPEN cur_tramite FOR
            SELECT PT.num_tramite,
                   PT.codigo_estado,
                   PT.cod_moneda,
                   PT.bajo_linea_credito,
                   PTC.descripcion,
                   PT.cod_tip_operacion,
                   PT.unidad_ejecutora
              FROM PR_TRAMITE PT
                   INNER JOIN personas_x_pr_tramite A
                       ON  A.cod_empresa = PT.cod_empresa
                       AND A.num_tramite = PT.num_tramite
                   INNER JOIN PR_TIP_CREDITO PTC
                       ON  PTC.cod_tip_credito = PT.cod_tip_credito
                       AND PTC.cod_empresa     = PT.cod_empresa
                   INNER JOIN PR_TIP_OPERACION PTO
                       ON  PTO.cod_tip_operacion = PT.cod_tip_operacion
             WHERE PT.cod_tip_operacion IN (
                       v_op_cartera, v_op_comex, v_op_sobregiro,
                       v_op_tarjeta, v_op_factoraje
                   )
               AND PT.codigo_estado = Pr_Utl_Estados.Verif_Estado_Act_Cast(PT.Codigo_Estado)
               AND PT.cod_empresa   = p_cod_empresa
               AND A.cod_persona    = p_cod_cliente;

        LOOP
            FETCH cur_tramite INTO reg_tramite;
            EXIT WHEN cur_tramite%NOTFOUND;

            -- Resetear por iteracion: evita acumular valores de iteraciones anteriores
            -- cuando este tramite no tiene fila en PR_HIS_CALIF_X_PR_TRAMITE
            v_pre_contin  := 0;
            v_pre_directa := 0;

            BEGIN
                SELECT mon_pre_contin,
                       mon_prevision + NVL(mon_prev_diferido, 0)  -- OBCHOQUE 13/06/2022 MA 373820, adiciona prevision diferida
                  INTO v_pre_contin,
                       v_pre_directa
                  FROM PR_HIS_CALIF_X_PR_TRAMITE
                 WHERE cod_empresa = p_cod_empresa
                   AND num_tramite = reg_tramite.num_tramite
                   AND num_proceso = v_num_proceso;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN NULL;
            END;

            IF reg_tramite.cod_moneda <> v_cod_moneda_usd THEN
                -- Convierte prevision contingente a USD
                Pr_Utl.Convierte_Moneda_a_Moneda(
                    p_cod_empresa,
                    v_pre_contin,
                    p_fecha,
                    TO_NUMBER(reg_tramite.cod_moneda),
                    TO_NUMBER(v_cod_moneda_usd),
                    g_error,
                    v_pre_contin
                );
                -- Convierte prevision directa a USD
                Pr_Utl.Convierte_Moneda_a_Moneda(
                    p_cod_empresa,
                    v_pre_directa,
                    p_fecha,
                    TO_NUMBER(reg_tramite.cod_moneda),
                    TO_NUMBER(v_cod_moneda_usd),
                    g_error,
                    v_pre_directa
                );
            END IF;

            g_tot_pre_con   := g_tot_pre_con   + NVL(v_pre_contin,  0);
            g_tot_prevision := g_tot_prevision + NVL(v_pre_directa, 0);

        END LOOP;
        CLOSE cur_tramite;

        -- Registrar los parametros de esta carga
        g_cod_empresa := p_cod_empresa;
        g_cod_cliente := p_cod_cliente;
        g_fecha       := p_fecha;

    END cargar;

    -- --------------------------------------------------------
    FUNCTION tot_prevision RETURN NUMBER IS
    BEGIN
        RETURN g_tot_prevision;
    END;

    FUNCTION tot_pre_con RETURN NUMBER IS
    BEGIN
        RETURN g_tot_pre_con;
    END;

    FUNCTION error_msg RETURN VARCHAR2 IS
    BEGIN
        RETURN g_error;
    END;

END pkg_obt_prevision;
/
