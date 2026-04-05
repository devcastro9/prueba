CREATE OR REPLACE PROCEDURE pr.carga_pr0349_prevdet (
    p_cod_empresa  IN  VARCHAR2,
    p_cod_cliente  IN  VARCHAR2,
    p_fecha_hoy    IN  DATE,
    p_error        OUT VARCHAR2,
    p_commit       IN  BOOLEAN DEFAULT TRUE
) IS
-- mocastro - FSN-2026-04-05
-- Objetivo : Centraliza en BD la logica de OBT_PREVISION (PR0349).
--            Carga PR_PREVISION_DET_GTT con el detalle de previsiones
--            de un cliente para una empresa, incluyendo saldos por tipo
--            de operacion (cartera, sobregiros, tarjetas, boletas, comex)
--            y montos convertidos a dolares.
--            El formulario PR0349 queda reducido a leer la GTT y popular
--            el bloque :bkdetpre.
--
-- Dependencias de BD ya disponibles:
--   CC_INTERFACE_PR.Sobre_Riesgo
--   I2000_Interface_Pr.Comex_Super_Op
--   Tc_Utl.tc_antecedentes_directos
--   Gt_Interface_Pr.BolGar_Riesgo
--   Pr_Utl_Estados.Verif_Estado_Act_Cast
--   Convierte_Moneda_a_Moneda
--   Calcular_Saldo_Movimientos   -- VERIFICAR: confirmar si existe en BD
--                                -- o solo en libreria Forms

    -- Codigos de tipo de operacion leidos de parametros
    v_cod_cartera     PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_sobregiros  PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_tarjeta     PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_comex       PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_boletas     PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_factoraje   PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_cancelado   PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    v_cod_moneda_usd  PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;

    v_num_proceso     pr_provisiones.num_proceso%TYPE;

    -- Variables de trabajo por registro
    v_plaza           cg_sucursales.nombre_corto%TYPE;
    v_no_credito      NUMBER(10);
    v_f_prim_desemb   DATE;
    v_f_vcto_final    DATE;
    v_f_cuota         DATE;
    v_monto_desemb    NUMBER(16,2);
    v_saldo_actual    NUMBER(16,2);
    v_pre_con         NUMBER(16,2);
    v_prevision       NUMBER(16,2);
    v_des_estado      pr_estados_credito.descripcion_estado%TYPE;
    v_des_moneda      moneda.abreviatura%TYPE;
    v_monto_orig      NUMBER(16,2);

    -- Variables Sobregiros
    v_pcta_efectivo       NUMBER;
    v_pFec_Inicio         DATE;
    v_pCan_Dias_Gracia    NUMBER;
    v_pTasa_Interes       NUMBER;
    v_pFec_Vencimiento    DATE;
    v_pMon_Contratado     NUMBER;
    v_pPlazo_Dias         NUMBER;
    v_pFec_Cancelacion    DATE;
    v_pDias_Atraso        NUMBER;
    v_pFec_Ult_Mov        DATE;
    v_pSaldo_Vig          NUMBER;
    v_pCta_Vig            VARCHAR2(20);
    v_pInteres_Vig        NUMBER;
    v_pCta_Int_Vig        VARCHAR2(20);
    v_pSaldo_Ven          NUMBER;
    v_pCta_Ven            VARCHAR2(20);
    v_pInt_Susp_Ven       NUMBER;
    v_pCta_Int_Susp_Ven   VARCHAR2(20);
    v_pCod_Error          VARCHAR2(20);
    v_pCta_Contingente    VARCHAR2(20);
    v_mon_contingente     NUMBER(16,2);
    v_fec_estado          DATE;

    -- Variables Tarjetas
    v_nro_tarjeta         NUMBER;
    v_fec_habilit         DATE;
    v_fec_vencimi         DATE;
    v_monto_aprob         NUMBER;
    v_saldo_tarjeta       NUMBER;
    v_msg_tarjeta         VARCHAR2(20);

    -- Variables Boletas
    v_p_monto             NUMBER;
    v_p_fec_ini           DATE;
    v_p_fec_venci         DATE;
    v_p_fec_cancela       DATE;
    v_p_cta_con           VARCHAR2(15);
    v_p_act_econo         VARCHAR2(20);
    v_p_no_operacion      NUMBER;
    v_p_tasa_interes      NUMBER;
    v_p_plazo_dias        NUMBER;

    -- Variables Comex
    v_FechaInicio         DATE;
    v_FechaVenc           DATE;
    v_FechaCanc           DATE;
    v_FechaUltMov         DATE;
    v_Plazo               NUMBER;
    v_TasaInt             NUMBER;
    v_NumOperac           NUMBER;
    v_MtoContratado       NUMBER;
    v_MtoCont             NUMBER;
    v_CtaCont             VARCHAR2(25);
    v_MtoNegFinanc        NUMBER;
    v_CtaNegFinanc        VARCHAR2(25);
    v_MtoIntxCobrar       NUMBER;
    v_CtaIntxCobrar       VARCHAR2(25);
    v_EsNeg_o_CDI         VARCHAR2(1);
    v_MensajeError        VARCHAR2(200);
    v_Saldo_Cont          NUMBER;
    v_Saldo_Ope           NUMBER;
    v_Intereses_Ope       NUMBER;

    CURSOR cur_tramite IS
        SELECT pr_tramite.num_tramite,
               pr_tramite.codigo_estado,
               pr_tramite.cod_moneda,
               pr_tramite.bajo_linea_credito,
               pr_tip_credito.descripcion,
               pr_tramite.cod_tip_operacion,
               pr_tramite.unidad_ejecutora
          FROM pr_tramite,
               personas_x_pr_tramite a,
               pr_tip_credito,
               pr_tip_operacion
         WHERE pr_tramite.cod_tip_operacion = pr_tip_operacion.cod_tip_operacion
           AND pr_tramite.cod_tip_operacion IN (v_cod_cartera,
                                                v_cod_sobregiros,
                                                v_cod_tarjeta,
                                                v_cod_comex,
                                                v_cod_boletas,
                                                v_cod_factoraje)
           AND pr_tramite.codigo_estado     = Pr_Utl_Estados.Verif_Estado_Act_Cast(pr_tramite.codigo_estado)
           AND pr_tip_credito.cod_tip_credito = pr_tramite.cod_tip_credito
           AND pr_tip_credito.cod_empresa   = pr_tramite.cod_empresa
           AND pr_tramite.cod_empresa       = p_cod_empresa
           AND a.cod_empresa               = pr_tramite.cod_empresa
           AND a.num_tramite               = pr_tramite.num_tramite
           AND a.cod_persona               = p_cod_cliente;

BEGIN
    p_error := NULL;

    -- Leer codigos de operacion desde parametros (evita bind variables de Forms)
    v_cod_cartera    := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_CARTERA');
    v_cod_sobregiros := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_SOBREGIRO');
    v_cod_tarjeta    := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_TARJETA');
    v_cod_comex      := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_COMEX');
    v_cod_boletas    := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_BOLETAS');
    v_cod_factoraje  := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_FACTORAJE');
    v_cod_cancelado  := pa.PARAMETRO_GENERAL('PR', 'COD_ESTADO_CANCELADO');
    v_cod_moneda_usd := pa.PARAMETRO_GENERAL('PR', 'COD_MONEDA_DOLAR');

    -- Limpiar datos previos de la sesion
    DELETE FROM pr.PR_PREVISION_DET_GTT
     WHERE cod_empresa = p_cod_empresa;

    -- Obtener numero de proceso de prevision vigente
    BEGIN
        SELECT MAX(num_proceso)
          INTO v_num_proceso
          FROM pr_provisiones
         WHERE cod_empresa                  = p_cod_empresa
           AND TRUNC(fec_ult_calificacion) <= p_fecha_hoy
           AND provisionado                 = 'S';
    END;

    FOR reg IN cur_tramite LOOP

        -- Inicializar variables de trabajo
        v_no_credito    := NULL;
        v_f_prim_desemb := NULL;
        v_f_vcto_final  := NULL;
        v_f_cuota       := NULL;
        v_monto_desemb  := NULL;
        v_saldo_actual  := NULL;
        v_pre_con       := NULL;
        v_prevision     := NULL;

        -- Plaza / Sucursal
        BEGIN
            SELECT suc.nombre_corto
              INTO v_plaza
              FROM cg_unidades_ejecutoras uni,
                   cg_sucursales         suc
             WHERE uni.codigo_empresa   = p_cod_empresa
               AND suc.codigo_empresa   = p_cod_empresa
               AND suc.codigo_sucursal  = uni.codigo_sucursal
               AND uni.unidad_ejecutora = reg.unidad_ejecutora;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN v_plaza := NULL;
        END;

        -- *** SOBREGIROS ***
        IF reg.cod_tip_operacion = v_cod_sobregiros THEN
            BEGIN
                SELECT num_cuenta
                  INTO v_pcta_efectivo
                  FROM pr_sol_adic_sobre
                 WHERE cod_empresa    = p_cod_empresa
                   AND num_solicitud  = reg.num_tramite;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN NULL;
            END;

            CC_INTERFACE_PR.Sobre_Riesgo(
                p_cod_empresa, v_pcta_efectivo, reg.num_tramite, p_fecha_hoy,
                v_pFec_Inicio, v_pCan_Dias_Gracia, v_pTasa_Interes,
                v_pFec_Vencimiento, v_pMon_Contratado, v_pPlazo_Dias,
                v_pFec_Cancelacion, v_pDias_Atraso, v_pFec_Ult_Mov,
                v_fec_estado, v_pSaldo_Vig, v_pCta_Vig,
                v_mon_contingente, v_pCta_Contingente, v_pInteres_Vig,
                v_pCta_Int_Vig, v_pInt_Susp_Ven, v_pCta_Int_Susp_Ven,
                v_pCod_Error);

            v_no_credito    := v_pcta_efectivo;
            v_f_prim_desemb := v_pFec_Inicio;
            v_f_vcto_final  := v_pFec_Vencimiento;
            v_monto_desemb  := v_pMon_Contratado;
            v_saldo_actual  := NVL(v_pSaldo_Vig, 0) + NVL(v_pSaldo_Ven, 0);
        END IF;

        -- *** COMEX ***
        IF reg.cod_tip_operacion = v_cod_comex THEN
            BEGIN
                I2000_Interface_Pr.Comex_Super_Op(
                    p_cod_empresa, reg.num_tramite,
                    v_FechaInicio, v_FechaVenc, v_FechaCanc, v_FechaUltMov,
                    v_Plazo, v_TasaInt, v_NumOperac, v_MtoContratado,
                    v_MtoCont, v_CtaCont, v_MtoNegFinanc, v_CtaNegFinanc,
                    v_MtoIntxCobrar, v_CtaIntxCobrar, v_EsNeg_o_CDI,
                    v_MensajeError);

                IF v_MensajeError IS NOT NULL THEN
                    p_error := '002725';
                    RETURN;
                END IF;

                IF v_EsNeg_o_CDI = 'N' THEN
                    v_Saldo_Cont    := 0;
                    v_Saldo_Ope     := v_MtoNegFinanc;
                    v_Intereses_Ope := v_MtoIntxCobrar;
                ELSIF v_EsNeg_o_CDI = 'C' THEN
                    v_Saldo_Cont    := v_MtoCont;
                    v_Saldo_Ope     := 0;
                    v_Intereses_Ope := 0;
                ELSE
                    p_error := '002725';
                    RETURN;
                END IF;

                v_f_prim_desemb := v_FechaInicio;
                v_f_vcto_final  := v_FechaVenc;
                v_monto_desemb  := v_MtoContratado;
                v_no_credito    := v_NumOperac;
                v_saldo_actual  := NVL(v_MtoCont, 0);
            EXCEPTION
                WHEN OTHERS THEN
                    p_error := '002725';
                    RETURN;
            END;
        END IF;

        -- *** TARJETAS DE CREDITO ***
        IF reg.cod_tip_operacion = v_cod_tarjeta THEN
            Tc_Utl.tc_antecedentes_directos(
                p_cod_empresa, reg.num_tramite,
                v_nro_tarjeta, v_fec_habilit, v_fec_vencimi,
                v_monto_aprob, v_saldo_tarjeta, v_msg_tarjeta);

            v_no_credito    := v_nro_tarjeta;
            v_f_prim_desemb := v_fec_habilit;
            v_f_vcto_final  := v_fec_vencimi;
            v_monto_desemb  := v_monto_aprob;
            v_saldo_actual  := v_saldo_tarjeta;
            -- NOTA: v_msg_tarjeta se ignora aqui; el Forms lo mostraba via UTILITARIOS.mensaje
        END IF;

        -- *** BOLETAS DE GARANTIA ***
        IF reg.cod_tip_operacion = v_cod_boletas THEN
            Gt_Interface_Pr.BolGar_Riesgo(
                p_cod_empresa, reg.num_tramite,
                v_p_monto, v_p_fec_ini, v_p_fec_venci, v_p_fec_cancela,
                v_p_cta_con, v_p_act_econo, v_p_no_operacion,
                v_p_tasa_interes, v_p_plazo_dias);

            v_f_prim_desemb := v_p_fec_ini;
            v_f_vcto_final  := v_p_fec_venci;
            v_saldo_actual  := v_p_monto;
            v_no_credito    := v_p_no_operacion;
        END IF;

        -- *** CARTERA ***
        IF reg.cod_tip_operacion = v_cod_cartera THEN
            BEGIN
                SELECT no_credito, f_primer_desembolso, f_vencimiento, monto_desembolsado
                  INTO v_no_credito, v_f_prim_desemb, v_f_vcto_final, v_monto_desemb
                  FROM pr_creditos
                 WHERE codigo_empresa = p_cod_empresa
                   AND num_tramite    = reg.num_tramite;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN v_no_credito := NULL;
            END;

            -- Fecha proxima cuota (estado activo)
            IF reg.codigo_estado = Pr_Utl_Estados.Verif_Estado_Act_Cast(reg.codigo_estado) THEN
                BEGIN
                    SELECT /*+ INDEX(PR_PLAN_PAGOS PK_PR_PLAN_PAGOS) */
                           MIN(f_cuota)
                      INTO v_f_cuota
                      FROM pr_plan_pagos
                     WHERE codigo_empresa = p_cod_empresa
                       AND no_credito     = v_no_credito
                       AND no_cuota      != 0
                       AND estado         = 'A';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN v_f_cuota := NULL;
                END;
            END IF;

            -- Fecha ultima cuota (estado cancelado)
            IF reg.codigo_estado = v_cod_cancelado THEN
                BEGIN
                    SELECT /*+ INDEX(PR_PLAN_PAGOS PK_PR_PLAN_PAGOS) */
                           MAX(f_cuota)
                      INTO v_f_cuota
                      FROM pr_plan_pagos
                     WHERE codigo_empresa = p_cod_empresa
                       AND no_credito     = v_no_credito
                       AND no_cuota      != 0
                       AND estado         = 'C';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN v_f_cuota := NULL;
                END;
            END IF;

            -- Saldo principal
            -- TODO: confirmar si Calcular_Saldo_Movimientos existe en BD
            --       o solo en libreria Forms. Si es solo Forms, mantener
            --       este calculo en el formulario y recibir v_saldo_actual
            --       como parametro adicional o calcularlo aqui con logica equivalente.
            Calcular_Saldo_Movimientos(
                p_cod_empresa, v_no_credito, v_f_prim_desemb,
                p_fecha_hoy, v_saldo_actual, p_error);
        END IF;

        -- Descripcion de estado
        BEGIN
            SELECT descripcion_estado
              INTO v_des_estado
              FROM pr_estados_credito
             WHERE codigo_estado = reg.codigo_estado;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN v_des_estado := NULL;
        END;

        -- Descripcion de moneda
        BEGIN
            SELECT abreviatura
              INTO v_des_moneda
              FROM moneda
             WHERE cod_moneda = reg.cod_moneda;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        -- Prevision
        BEGIN
            SELECT mon_pre_contin,
                   mon_prevision + NVL(mon_pre_adicional, 0) + NVL(mon_prev_diferido, 0)
              INTO v_pre_con, v_prevision
              FROM pr_his_calif_x_pr_tramite
             WHERE cod_empresa = p_cod_empresa
               AND num_tramite = reg.num_tramite
               AND num_proceso = v_num_proceso;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        -- Conversion de moneda a dolares (solo si no es ya dolares)
        IF TO_NUMBER(reg.cod_moneda) <> TO_NUMBER(v_cod_moneda_usd) THEN
            Convierte_Moneda_a_Moneda(p_cod_empresa, v_monto_desemb,  p_fecha_hoy,
                TO_NUMBER(reg.cod_moneda), TO_NUMBER(v_cod_moneda_usd), p_error, v_monto_orig);
            v_monto_desemb := v_monto_orig;

            Convierte_Moneda_a_Moneda(p_cod_empresa, v_saldo_actual,  p_fecha_hoy,
                TO_NUMBER(reg.cod_moneda), TO_NUMBER(v_cod_moneda_usd), p_error, v_monto_orig);
            v_saldo_actual := v_monto_orig;

            Convierte_Moneda_a_Moneda(p_cod_empresa, v_pre_con,       p_fecha_hoy,
                TO_NUMBER(reg.cod_moneda), TO_NUMBER(v_cod_moneda_usd), p_error, v_monto_orig);
            v_pre_con := v_monto_orig;

            Convierte_Moneda_a_Moneda(p_cod_empresa, v_prevision,     p_fecha_hoy,
                TO_NUMBER(reg.cod_moneda), TO_NUMBER(v_cod_moneda_usd), p_error, v_monto_orig);
            v_prevision := v_monto_orig;
        END IF;

        -- Insertar en GTT
        INSERT INTO pr.PR_PREVISION_DET_GTT (
            cod_empresa, num_tramite, codigo_estado, des_estado,
            cod_moneda, des_moneda, cod_tip_operacion, tipo_operacion,
            linea, plaza, no_credito, f_primer_desembolso,
            f_vcto_final, f_cuota, monto_desembolsado, saldo_actual,
            pre_con, prevision
        ) VALUES (
            p_cod_empresa, reg.num_tramite, reg.codigo_estado, v_des_estado,
            reg.cod_moneda, v_des_moneda, reg.cod_tip_operacion, reg.descripcion,
            reg.bajo_linea_credito, v_plaza, v_no_credito, v_f_prim_desemb,
            v_f_vcto_final, v_f_cuota, v_monto_desemb, v_saldo_actual,
            v_pre_con, v_prevision
        );

    END LOOP;

    IF p_commit THEN
        COMMIT;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        p_error := 'Error en carga_pr0349_prevdet: ' || SQLERRM;
        ROLLBACK;
END carga_pr0349_prevdet;
/
