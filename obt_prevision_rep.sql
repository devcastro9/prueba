PROCEDURE pr.obt_prevision_rep (
    p_cod_empresa  IN  VARCHAR2,
    pr_cod_cliente IN  VARCHAR2,
    p_fecha        IN  DATE,
    tot_prevision  OUT NUMBER,
    tot_pre_con    OUT NUMBER,
    p_error OUT VARCHAR2
)
IS
    --
    -- Obtiene los tramites del Cliente, con sus respectivas previsiones
    --

    CURSOR cur_tramite IS
        SELECT PR_TRAMITE.num_tramite,
               PR_TRAMITE.codigo_estado,
               PR_TRAMITE.cod_moneda,
               PR_TRAMITE.bajo_linea_credito,
               PR_TIP_CREDITO.descripcion,
               PR_TRAMITE.cod_tip_operacion,
               PR_TRAMITE.unidad_ejecutora
          FROM pr_tramite,
               personas_x_pr_tramite a,
               pr_tip_credito,
               pr_tip_operacion
         WHERE PR_TRAMITE.cod_tip_operacion = PR_TIP_OPERACION.cod_tip_operacion
           AND PR_TRAMITE.cod_tip_operacion IN (
                   pa.PARAMETRO_GENERAL('PR', 'COD_OPER_CARTERA'),
                  pa.PARAMETRO_GENERAL('PR', 'COD_OPER_COMEX'),
                   pa.PARAMETRO_GENERAL('PR', 'COD_OPER_SOBREGIRO'),
                   pa.PARAMETRO_GENERAL('PR', 'COD_OPER_TARJETA'),
                   pa.PARAMETRO_GENERAL('PR', 'COD_OPER_FACTORAJE')
               )
           AND PR_TRAMITE.codigo_estado     = Pr_Utl_Estados.Verif_Estado_Act_Cast(Pr_Tramite.Codigo_Estado)
           AND PR_TIP_CREDITO.cod_tip_credito = PR_TRAMITE.cod_tip_credito
           AND PR_TIP_CREDITO.cod_empresa    = PR_TRAMITE.cod_empresa
           AND PR_TRAMITE.cod_empresa        = p_cod_empresa
           AND a.cod_empresa                 = pr_tramite.cod_empresa
           AND a.num_tramite                 = pr_tramite.num_tramite
           AND a.cod_persona                 = pr_cod_cliente;

    -- Variables de Tarjetas de Créditos
    p_nro_tarjeta  NUMBER;
    p_fec_habilit  DATE;
    p_fec_vencimi  DATE;
    p_monto_aprob  NUMBER;
    p_saldo_actua  NUMBER;
    p_mensaje      VARCHAR2(20);

    -- Variables de Sobregiros
    Pcta_efectivo      NUMBER;
    pFec_Inicio        DATE;
    pCan_Dias_Gracia   NUMBER;
    pTasa_Interes      NUMBER;
    pFec_Vencimiento   DATE;
    pMon_Contratado    NUMBER;
    pPlazo_Dias        NUMBER;
    pFec_Cancelacion   DATE;
    pDias_Atraso       NUMBER;
    pFec_Ult_Mov       DATE;
    pSaldo_Vig         NUMBER;
    pCta_Vig           VARCHAR2(20);
    pInteres_Vig       NUMBER;
    pCta_Int_Vig       VARCHAR2(20);
    pSaldo_Ven         NUMBER;
    pCta_Ven           VARCHAR2(20);
    pInteres_Ven       NUMBER;
    pCta_Int_Ven       VARCHAR2(20);
    pInt_Susp_Ven      NUMBER;
    pCta_Int_Susp_Ven  VARCHAR2(20);
    pSaldo_Eje         NUMBER;
    pCta_Eje           VARCHAR2(20);
    pInteres_Eje       NUMBER;
    pCta_Int_Eje       VARCHAR2(20);
    pCod_Error         VARCHAR2(20);
    pCta_Contingente   VARCHAR2(20);
    vl_mon_contingente NUMBER(16,2);
    vl_fec_estado      DATE;
    pNumOperacion      NUMBER;
    pFechaInicio       DATE;
    pTasaInteres       NUMBER;
    pFechaVence        DATE;
    pMonContratado     NUMBER;
    pPlazoDias         NUMBER;
    pFechaCance        DATE;
    pDiasAtraso        NUMBER;
    pFecUltMov         DATE;
    pSaldoVig          NUMBER;
    pCtaSaldoVig       VARCHAR2(20);
    pInteresVig        NUMBER;
    pCtaIntVig         VARCHAR2(20);
    pSaldoVen          NUMBER;
    pCtaSaldoVen       VARCHAR2(20);
    pInteresVen        NUMBER;
    pCtaIntVen         VARCHAR2(20);
    pSaldoEjec         NUMBER;
    pCtaSaldoEjec      VARCHAR2(20);
    pMsjError          VARCHAR2(20);
    v_num_proceso      pr_provisiones.num_proceso%TYPE;
    v_pre_con          NUMBER(16,2);
    v_prevision        NUMBER(16,2);
    v_cod_moneda_usd    PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
BEGIN
    --
    -- Obtiene nro. de proceso de prevision
    --
   v_cod_moneda_usd := pa.PARAMETRO_GENERAL('PR', 'COD_MONEDA_DOLAR');-- [25/03/2026]
    BEGIN
        SELECT MAX(num_proceso)
          INTO v_num_proceso
          FROM PR_PROVISIONES
         WHERE cod_empresa                    = p_cod_empresa
           AND TRUNC(fec_ult_calificacion)   <= p_fecha
           AND provisionado                   = 'S';
    END;

    FOR reg_tramite IN cur_tramite LOOP

        --:bkdetpre.num_tramite       := reg_tramite.num_tramite;
        --:bkdetpre.codigo_estado     := reg_tramite.codigo_estado;
        --:bkdetpre.cod_moneda        := reg_tramite.cod_moneda;
        --:bkdetpre.cod_tip_operacion := reg_tramite.cod_tip_operacion;
        --:bkdetpre.tipo_operacion    := reg_tramite.descripcion;
        --:bkdetpre.linea             := reg_tramite.bajo_linea_credito;

        ---
        --- Obtiene prevision
        ---
        BEGIN
            -- SELECT mon_pre_contin, mon_prevision
            SELECT mon_pre_contin,
                   mon_prevision + NVL(mon_prev_diferido, 0)  -- OBCHOQUE 13/06/2022 MA 373820, Se adiciona la previsión diferida.
              INTO v_pre_con,
                   v_prevision
              FROM PR_HIS_CALIF_X_PR_TRAMITE
             WHERE cod_empresa = p_cod_empresa
               AND num_tramite = reg_tramite.num_tramite
               AND num_proceso = v_num_proceso;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;

        IF reg_tramite.cod_moneda <>     v_cod_moneda_usd THEN
            --
            -- Cambia la moneda a Dolares Prevision Contingente
            --
            Convierte_Moneda_a_Moneda(
                p_cod_empresa,
                v_pre_con,
                p_fecha,
                TO_NUMBER(reg_tramite.cod_moneda),
                v_cod_moneda_usd,
                p_error,
                v_pre_con
            );

            --
            -- Cambia la moneda a Dolares Prevision Normal
            --
            Convierte_Moneda_a_Moneda(
                p_cod_empresa,
                v_prevision,
                p_fecha,
                TO_NUMBER(reg_tramite.cod_moneda),
                v_cod_moneda_usd,
                p_error,
                v_prevision
            );

        END IF;

        tot_pre_con   := NVL(tot_pre_con,   0) + NVL(v_pre_con,   0);
        tot_prevision := NVL(tot_prevision, 0) + NVL(v_prevision, 0);

    END LOOP;

END;
