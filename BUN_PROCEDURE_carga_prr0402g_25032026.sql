CREATE OR REPLACE PROCEDURE    Carga_Prr0402g ( p_sesion          IN VARCHAR2,
                                                p_cod_empresa     IN VARCHAR2,
                                                p_cod_cliente     IN VARCHAR2,
                                                p_linea           IN VARCHAR2,
                                                p_nom_cliente     IN VARCHAR2,
                                                p_cod_moneda_usd  IN VARCHAR2,
                                                p_fecha_hoy       IN DATE,
                                                p_error           OUT VARCHAR2) IS
--
-- Fecha de creación : 14/02/2001
-- Analista          : Gustavo Anzoategui
-- Fecha de modific. : 13/11/2002
-- Analista          : Luis De la Quintana
-- Objetivo          : Insertar en la tabla PR_OPERACIONES_TMP los registros
--                     que cumplen los criterios del cursor, para luego,
--                     desplegar los datos en el reporte PRR0402G
-- Fecha de modific. : 13/04/2004
-- Analista          : Luis De la Quintana
-- Fecha de Modific. : 29/06/2005
-- Analista          : José luis Durán S.
-- Objetivo:De las deudas indirectas, ya no sale el instrumento sino más bien el nombre del cliente
--
-- Fecha de Modific. : 11/05/2017
-- Analista          : Noé Quenta Chavez.
-- Objetivo          :Convertir el monto desembolsado de todas las operaciones a dólares
-----------------------------------------------------------------------------------
   v_des_moneda            moneda.descripcion%TYPE;
   v_abrev_moneda          moneda.abreviatura%TYPE;
   v_des_sucursal          cg_sucursales.descripcion%TYPE;
   v_des_unidad_negocio    PR_UNIDADES_NEGOCIO.descripcion%TYPE;
   v_des_unidad_ejecutora  VARCHAR2(80);
   v_desc_producto         VARCHAR2(60);
   v_des_operacion         VARCHAR2(60);
   v_no_credito            VARCHAR2(20);
   v_desc_estado           PR_ESTADOS_CREDITO.descripcion_estado%TYPE;
   v_abrev_estado          PR_ESTADOS_CREDITO.abrev_estado%TYPE;
   v_desc_instrumento      VARCHAR2(60);
   v_saldo_tramite         NUMBER(16,2);
   v_des_calif             VARCHAR2(30);
   v_nom_cliente           personas.nombre%TYPE;
   v_nom_analista          personas.nombre%TYPE;
      -- Var. Utilizadas en Datos Generales Tramite
   v_tasa_interes      NUMBER(16,2);
   v_fecha_vencimiento DATE;
   v_fecha_cancelacion DATE;
   v_dias_atraso       NUMBER(10);
   v_dias_plazo        NUMBER(10);
   v_SaldoAct          NUMBER(16,2);
   v_SaldoDisp         NUMBER(16,2);
   v_SaldoAct_USD      NUMBER(16,2);
   v_SaldoDisp_USD     NUMBER(16,2);
   v_saldo_cont        NUMBER(16,2);
   v_saldo_cont_USD    NUMBER(16,2);
   v_saldo_directo     NUMBER(16,2);
   v_saldo_dir_USD     NUMBER(16,2);
   v_monto_operacion   NUMBER(16,2);
   v_MontoOp_USD       NUMBER(16,2);  --variable para obtener el monto desembolsado de lineas de credito en dólares
   v_MontoDes_USD       NUMBER(16,2);  --variable para obtener el monto desembolsado de operaciones bajo linea en dólares
   v_intereses_ope     NUMBER(16,2);
   v_comision          NUMBER(16,2);
   v_cargos            NUMBER(16,2);
   v_mora              NUMBER(16,2);
   v_puni              NUMBER(16,2);
   v_poliza            NUMBER(16,2);
   v_mensaje_error     VARCHAR2(10);
   v_fec_inicio        DATE;
   -- Variables utilizadas en la prevision anterior
   v_cod_estado_anterior  VARCHAR2(2);
   v_prev_cont_anterior   NUMBER(16,2);
   v_prev_anterior        NUMBER(16,2);
   v_mensaje              VARCHAR2(6);
   v_porc_prev            NUMBER(10,4);
   v_cod_calif            VARCHAR2(2);
  -- Variables utilizadas en la prevision actual
   vl_Cod_Estado_Actual    VARCHAR2(2);
   vl_Prev_Cont_Actual     NUMBER(16,2);
   vl_prev_actual          NUMBER(16,2);
   vl_prev_vigente         NUMBER(16,2);
   vl_prev_estado          NUMBER(16,2);
   vl_mensaje              VARCHAR2(6);
   vl_porc_prev            NUMBER(10,4);
   vl_cod_calif            VARCHAR2(2);
   v_desc_instrumento1     VARCHAR2(60);
   v_desc_instrumento2     VARCHAR2(60);
   v_desc_instrumento3     VARCHAR2(60);
   v_fec_cuota             DATE;
   vl_cuota6               NUMBER(5) := 0;
   vl_cuota5               NUMBER(5) := 0;
   vl_cuota4               NUMBER(5) := 0;
   vl_cuota3               NUMBER(5) := 0;
   vl_cuota2               NUMBER(5) := 0;
   vl_cuota1               NUMBER(5) := 0;
   vl_fecha1               DATE;
   vl_fecha2               DATE;
   vl_fecha3               DATE;
   vl_fecha4               DATE;
   vl_fecha5               DATE;
   vl_fecha6               DATE;
   --
   vl_saldo_dif            NUMBER(16,2);  --OBCHOQUE 19/05/2020
  --
  -- Cursor de Lineas del Cliente
  --
   CURSOR v_cur_lineas IS
   SELECT a.codigo_empresa, a.no_credito, A.Num_Tramite, f_proxima_revision,
          A.Tipo_credito, A.F_apertura, A.F_Vencimiento,
          A.Codigo_Moneda, A.Estado,  A.Monto_credito, a.tipo_linea
     FROM PR_CREDITOS A
    WHERE A.Codigo_Empresa = P_Cod_Empresa
      AND A.Estado         = Pr_Utl_Estados.Verif_Estado_Linea(A.Estado) --'P'
      AND a.es_linea_credito = 'S'
      AND A.Num_Tramite IN (SELECT B.Num_Tramite
                              FROM PERSONAS_X_PR_TRAMITE B
                             WHERE B.Cod_Empresa = P_Cod_Empresa
                               AND B.Cod_Persona = P_Cod_Cliente);
  --
  -- Cursor de Operaciones Bajo Linea del cliente
  --
  CURSOR v_cur_op_bl (v_cod_empresa IN VARCHAR2,
                      v_num_tramite IN NUMBER  ) IS
  SELECT num_tramite, fec_inicio, mon_operacion,
         codigo_estado, cod_moneda, cod_tip_operacion, cod_tip_producto, cod_tip_credito,
         num_tramite_padre
    FROM PR_TRAMITE
   WHERE codigo_estado IN ( Pr_Utl_Estados.verif_estado_act_cast (codigo_estado))
     AND cod_empresa = v_cod_empresa
     AND num_tramite_padre = v_num_tramite
   ORDER BY fec_inicio;
  --
  -- Cursor de Operaciones Fuera de Linea del cliente
  --
  CURSOR v_cur_op_fl IS
  SELECT num_tramite, fec_inicio, mon_operacion, codigo_estado,
         cod_moneda, cod_tip_operacion, cod_tip_producto, cod_tip_credito
    FROM PR_TRAMITE
   WHERE cod_empresa = P_Cod_Empresa
     AND codigo_estado IN (Pr_Utl_Estados.verif_estado_act_cast (codigo_estado))
     AND bajo_linea_credito = 'N'
     AND Num_Tramite IN (SELECT B.Num_Tramite
                           FROM PERSONAS_X_PR_TRAMITE B
                          WHERE B.Cod_Empresa = P_Cod_Empresa
                            AND B.Cod_Persona = P_Cod_Cliente)
   ORDER BY fec_inicio;
  --
  -- Cursor de Operaciones Indirectas del cliente
  --
  CURSOR v_cur_op_ind IS
  SELECT a.num_tramite, b.fec_inicio, b.mon_operacion, b.codigo_estado, b.cod_moneda,
         b.cod_tip_operacion, b.cod_tip_producto, b.cod_tip_credito
    FROM pr_v_garantes a, PR_TRAMITE b
   WHERE a.cod_empresa = p_cod_empresa
     AND a.quirografaria = 'N'
     AND a.cod_cliente   = p_cod_cliente
     AND a.cod_empresa = b.cod_empresa
     AND a.num_tramite = b.num_tramite
     AND (b.codigo_estado IN (Pr_Utl_Estados.verif_estado_act_cast (b.codigo_estado))
      OR b.codigo_estado IN ('P'));--(Pr_Utl_Estados.Verif_Estado_linea(b.codigo_estado)));
   --
   CURSOR cur_plan ( pc_cod_empresa IN VARCHAR2,
                     pc_no_credito  IN NUMBER  ) IS
      SELECT no_cuota, f_cancelacion, (f_cancelacion - f_cuota) dias_atraso, ROWNUM cant
        FROM PR_PLAN_PAGOS a
       WHERE a.no_credito     = pc_no_credito
         AND a.estado         = 'C'
         AND a.no_cuota       > 0
         AND a.codigo_empresa = pc_cod_empresa
         AND no_cuota  >= ( SELECT MAX(no_cuota) - 3
                               FROM PR_PLAN_PAGOS b
                              WHERE a.no_credito     = b.no_credito
                                AND a.codigo_empresa = b.codigo_empresa
                                AND a.estado         = b.estado )
       ORDER BY no_cuota;
BEGIN
   p_error := NULL;
   FOR reg_lineas IN v_cur_lineas LOOP
      Valida_Producto_Bd(reg_lineas.tipo_credito,
                            p_linea,
                            p_cod_empresa   ,
                            v_desc_producto,
                            p_error         );
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Estado_Bd (reg_lineas.estado,
                           v_desc_estado,
                           v_abrev_estado,
                           p_error    );
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Moneda_Bd(reg_lineas.codigo_moneda,
                           v_des_moneda,
                           v_abrev_moneda,
                           p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Pr_Utl.Saldo_Actual_Pr (P_Cod_Empresa,
                              reg_lineas.No_Credito,
                              v_SaldoAct,
                              p_Error);
      Pr_Utl.Saldo_Disponible_Pr (P_Cod_Empresa,
                                  reg_lineas.No_Credito,
                                  v_SaldoDisp,
                                  p_Error);
      --<--OBCHOQUE 19/05/2020 Circular asfi 2785, Se adiciona saldo diferido.
      /*BEGIN
         vl_saldo_dif := pr.pr_util_dif.saldo_dif_linea (p_cod_empresa, reg_lineas.num_tramite);
         v_saldoact   := v_saldoact + vl_saldo_dif;
         v_saldodisp  := v_saldodisp - vl_saldo_dif;
      EXCEPTION
         WHEN OTHERS THEN
            p_error := 'Error no controlado al obtener saldo diferido de linea. Error: ' || SQLERRM;
            RETURN;
      END;*/--Las lineas no tienen que ser afectados
      -->--OBCHOQUE 19/05/2020 Circular asfi 2785.
      Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                       v_SaldoAct,
                                       P_Fecha_Hoy,
                                       reg_lineas.Codigo_Moneda,
                                       P_Cod_Moneda_USD,
                                       p_Error,
                                       V_SaldoAct_USD);
      Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                        v_SaldoDisp,
                                        P_Fecha_Hoy,
                                        reg_lineas.Codigo_Moneda,
                                        P_Cod_Moneda_USD,
                                        p_Error,
                                        V_SaldoDisp_USD);
      --Convierte a dólares el monto desembolsado
      Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                        reg_lineas.monto_credito,
                                        P_Fecha_Hoy,
                                        reg_lineas.Codigo_Moneda,
                                        P_Cod_Moneda_USD,
                                        p_Error,
                                        v_MontoOp_USD);
      -- Inserta las Lineas
      INSERT INTO PR_OPERACIONES_TMP ( sesion               , num_tramite          ,no_credito,
                                       des_moneda           , cod_sucursal         ,fec_primer_desembolso,
                                       mon_saldo            , mon_utilizar         ,f_vencimiento,
                                       monto_desembolsado   , des_producto         ,des_estado,
                                       des_tip_credito      , f_paso_castigo)
                              VALUES ( p_sesion                , reg_lineas.num_tramite, reg_lineas.no_credito,
                                       v_des_moneda            , 100                   , reg_lineas.f_apertura,
                                       v_SaldoAct_USD          , v_SaldoDisp_USD       , reg_lineas.f_vencimiento,
                                       --reg_lineas.monto_credito, v_desc_producto       , v_abrev_estado,
                                       v_MontoOp_USD           , v_desc_producto       , v_abrev_estado,
                                       NULL                    , reg_lineas.f_proxima_revision);
        -- Recorre el Cursor de Operaciones Bajo Linea
      FOR r_op_bl IN v_cur_op_bl (p_cod_empresa, reg_lineas.num_tramite) LOOP
         Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                               r_op_bl.num_tramite,
                                               p_fecha_hoy,
                                               v_no_credito,
                                               v_tasa_interes,
                                               v_dias_plazo,
                                               v_fec_inicio,
                                               v_fecha_vencimiento,
                                               v_fecha_cancelacion,
                                               v_dias_atraso,
                                               v_monto_operacion,
                                               v_saldo_cont,
                                               v_saldo_directo,
                                               v_intereses_ope,
                                               v_comision,
                                               v_cargos,
                                               v_mora,
                                               v_puni,
                                               v_poliza,
                                               v_mensaje_error);
         Valida_Tip_Operacion_Bd ( r_op_bl.cod_tip_operacion,
                                   v_des_operacion,
                                   p_error);
         IF p_error IS NOT NULL THEN
            RETURN;
         END IF;
         Valida_Producto_Bd ( r_op_bl.cod_tip_producto,
                              r_op_bl.cod_tip_operacion,
                              p_cod_empresa,
                              v_desc_producto,
                              p_error);
         IF p_error IS NOT NULL THEN
            RETURN;
         END IF;
         Valida_Estado_Bd ( r_op_bl.codigo_estado,
                            v_desc_estado,
                            v_abrev_estado,
                            p_error);
         IF p_error IS NOT NULL THEN
            RETURN;
         END IF;
         Valida_Moneda_Bd ( r_op_bl.cod_moneda,
                            v_des_moneda,
                            v_abrev_moneda,
                            p_error);
         IF p_error IS NOT NULL THEN
            RETURN;
         END IF;
         --<--OBCHOQUE 11/05/2020 Circular asfi 2785, Se adiciona saldo diferido.
         /*BEGIN
            v_saldo_directo   := v_saldo_directo + pr.pr_util_dif.saldo_diferido (p_cod_empresa, v_no_credito);
         EXCEPTION
            WHEN OTHERS THEN
               p_error := 'Error no controlado al obtener saldo diferido. Error: ' || SQLERRM;
               RETURN;
         END;*/--la mejora esta en Pr_Abon3_Bd.Datos_Generales_Tramite--21/07/2020
         -->--OBCHOQUE 11/05/2020 Circular asfi 2785.
         Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                            v_Saldo_cont,
                                            P_Fecha_Hoy,
                                            r_op_bl.Cod_Moneda,
                                            P_Cod_Moneda_USD,
                                            p_Error,
                                            V_Saldo_cont_USD);
         Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                            v_Saldo_directo,
                                            P_Fecha_Hoy,
                                            r_op_bl.Cod_Moneda,
                                            P_Cod_Moneda_USD,
                                            p_Error,
                                            V_Saldo_dir_USD);
         --Convierte el monto desembolsado a dolares
         Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                            r_op_bl.mon_operacion,
                                            P_Fecha_Hoy,
                                            r_op_bl.Cod_Moneda,
                                            P_Cod_Moneda_USD,
                                            p_Error,
                                            v_MontoDes_USD);
         -- Inserta las operaciones bajo linea
         Valida_Instrumento_Credito_Bd ( r_op_bl.cod_tip_credito,
                                         p_cod_empresa,
                                         v_desc_instrumento1,
                                         p_error           );
         IF p_error IS NOT NULL THEN
            RETURN;
         END IF;
         IF r_op_bl.cod_tip_operacion = 1 THEN
            -- Obtiene la Fecha de Vencimiento de la Cuota, cuando el estado en el Plan de Pagos es D = Activo
            IF r_op_bl.codigo_estado = Pr_Utl_Estados.verif_estado_act_cast (r_op_bl.codigo_estado) THEN
               BEGIN
                  SELECT MIN(f_cuota)
                    INTO v_fec_cuota
                    FROM PR_PLAN_PAGOS
                   WHERE codigo_empresa = p_cod_empresa
                     AND no_credito     = TO_NUMBER(v_no_credito)
                     AND no_cuota       != 0
                     AND estado         = 'A';
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_fec_cuota := NULL;
               END;
            END IF;
            --
            -- Obtiene la Fecha de Vencimiento de la Cuota, cuando el estado en el Plan de Pagos es C = Cancelado
            --
            IF r_op_bl.codigo_estado = 'C' THEN
               BEGIN
                  SELECT MAX(f_cuota)
                    INTO v_fec_cuota
                    FROM PR_PLAN_PAGOS
                   WHERE codigo_empresa = p_cod_empresa
                     AND no_credito     = TO_NUMBER(v_no_credito)
                     AND no_cuota       != 0
                     AND estado         = 'C';
                  EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                        v_fec_cuota := NULL;
               END;
            END IF;
            --
            -- Busca los 6 últimos pagos y revisa los días de mora
            --
            FOR reg_plan IN cur_plan ( p_cod_empresa,
                                       TO_NUMBER(v_no_credito))
            LOOP
               IF    reg_plan.cant = 1 THEN vl_cuota6 := reg_plan.dias_atraso;
               ELSIF reg_plan.cant = 2 THEN vl_cuota5 := reg_plan.dias_atraso;
               ELSIF reg_plan.cant = 3 THEN vl_cuota4 := reg_plan.dias_atraso;
               ELSIF reg_plan.cant = 4 THEN vl_cuota3 := reg_plan.dias_atraso;
               ELSIF reg_plan.cant = 5 THEN vl_cuota2 := reg_plan.dias_atraso;
               ELSIF reg_plan.cant = 6 THEN vl_cuota1 := reg_plan.dias_atraso;
               END IF;
            END LOOP;
         ELSE
            v_fec_cuota := NULL;
         END IF; --r_op_bl.cod_tip_operacion = 1 THEN
         --
         INSERT INTO PR_OPERACIONES_TMP ( sesion            , num_tramite          , cod_persona,
                                          des_moneda        , cod_sucursal         , fec_primer_desembolso,
                                          mon_saldo         , mon_utilizar         , f_vencimiento,
                                          monto_desembolsado, des_producto         , des_estado,
                                          des_tip_credito   , numero_reprogramacion, f_ultimo_desembolso,
                                          codigo_origen     , mon_solicitado       , mon_operacion,
                                          mon_tramite       , deu_ven_dir30        , tasa_original,
                                          unidad_ejecutora  )
                                 VALUES ( p_sesion             , r_op_bl.num_tramite      , v_no_credito,
                                          v_abrev_moneda       , 200                      , v_fec_inicio,
                                          v_Saldo_dir_USD      , v_Saldo_cont_USD         , v_fecha_vencimiento,
                                          v_MontoDes_USD       , v_desc_producto          , v_abrev_estado,
                                          v_desc_instrumento1  , r_op_bl.num_tramite_padre, v_fec_cuota,
                                          vl_cuota1            , vl_cuota2                , vl_cuota3,
                                          vl_cuota4            , vl_cuota5                , vl_cuota6,
                                          v_dias_atraso        );
         --
         vl_cuota6 := 0;
         vl_cuota5 := 0;
         vl_cuota4 := 0;
         vl_cuota3 := 0;
         vl_cuota2 := 0;
         vl_cuota1 := 0;
      END LOOP; -- Operaciones Bajo Linea
    END LOOP;   -- Lineas
    -- Recorre el Cursor de Operaciones Fuera de Linea
   FOR r_op_fl IN v_cur_op_fl LOOP
      Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                            r_op_fl.num_tramite,
                                            p_fecha_hoy,
                                            v_no_credito,
                                            v_tasa_interes,
                                            v_dias_plazo,
                                            v_fec_inicio,
                                            v_fecha_vencimiento,
                                            v_fecha_cancelacion,
                                            v_dias_atraso,
                                            v_monto_operacion,
                                            v_saldo_cont,
                                            v_saldo_directo,
                                            v_intereses_ope,
                                            v_comision,
                                            v_cargos,
                                            v_mora,
                                            v_puni,
                                            v_poliza,
                                            v_mensaje_error);
      Valida_Tip_Operacion_Bd ( r_op_fl.cod_tip_operacion,
                                v_des_operacion,
                                p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Producto_Bd ( r_op_fl.cod_tip_producto,
                           r_op_fl.cod_tip_operacion,
                           p_cod_empresa,
                           v_desc_producto,
                           p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      --
      Valida_Estado_Bd ( r_op_fl.codigo_estado,
                         v_desc_estado,
                         v_abrev_estado,
                         p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Moneda_Bd ( r_op_fl.cod_moneda,
                         v_des_moneda,
                         v_abrev_moneda,
                         p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      --
      --<--OBCHOQUE 27/04/2020 Circular asfi 2785, Se adiciona saldo diferido.
      /*BEGIN
         v_saldo_directo   := v_saldo_directo + pr.pr_util_dif.saldo_diferido (p_cod_empresa, v_no_credito);
      EXCEPTION
         WHEN OTHERS THEN
            p_error := 'Error no controlado al obtener saldo diferido. Error: ' || SQLERRM;
            RETURN;
      END;*/--la mejora esta en Pr_Abon3_Bd.Datos_Generales_Tramite--21/07/2020
      -->--OBCHOQUE 27/04/2020 Circular asfi 2785.
      --
      Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                         v_Saldo_cont,
                                         P_Fecha_Hoy,
                                         r_op_fl.Cod_Moneda,
                                         P_Cod_Moneda_USD,
                                         p_Error,
                                         V_Saldo_cont_USD);
      --
      Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                         v_Saldo_directo,
                                         P_Fecha_Hoy,
                                         r_op_fl.Cod_Moneda,
                                         P_Cod_Moneda_USD,
                                         p_Error,
                                         V_Saldo_dir_USD);
      --
      Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                         r_op_fl.mon_operacion,
                                         P_Fecha_Hoy,
                                         r_op_fl.Cod_Moneda,
                                         P_Cod_Moneda_USD,
                                         p_Error,
                                         v_MontoDes_USD);
      --
      Valida_Instrumento_Credito_Bd ( r_op_fl.cod_tip_credito,
                                      p_cod_empresa,
                                      v_desc_instrumento2,
                                      p_error           );
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      IF r_op_fl.cod_tip_operacion = 1 THEN
         -- Obtiene la Fecha de Vencimiento de la Cuota, cuando el estado en el Plan de Pagos es D = Activo
         IF r_op_fl.codigo_estado = Pr_Utl_Estados.verif_estado_act_cast (r_op_fl.codigo_estado) THEN
            BEGIN
               SELECT MIN(f_cuota)
                 INTO v_fec_cuota
                 FROM PR_PLAN_PAGOS
                WHERE codigo_empresa = p_cod_empresa
                  AND no_credito     = TO_NUMBER(v_no_credito)
                  AND no_cuota       != 0
                  AND estado         = 'A';
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_fec_cuota := NULL;
            END;
         END IF;
         -- Obtiene la Fecha de Vencimiento de la Cuota, cuando el estado en el Plan de Pagos es C = Cancelado
         --
         IF r_op_fl.codigo_estado = 'C' THEN
            BEGIN
               SELECT MAX(f_cuota)
                 INTO v_fec_cuota
                 FROM PR_PLAN_PAGOS
                WHERE codigo_empresa = p_cod_empresa
                  AND no_credito     = TO_NUMBER(v_no_credito)
                  AND no_cuota       != 0
                  AND estado         = 'C';
               EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                     v_fec_cuota := NULL;
            END;
         END IF;
            --
            -- Busca los 6 últimos pagos y revisa los días de mora
            --
            FOR reg_plan IN cur_plan ( p_cod_empresa,
                                       TO_NUMBER(v_no_credito))
            LOOP
               IF    reg_plan.cant = 1 THEN vl_cuota4 := reg_plan.dias_atraso; vl_fecha4 := reg_plan.f_cancelacion;
               ELSIF reg_plan.cant = 2 THEN vl_cuota3 := reg_plan.dias_atraso; vl_fecha3 := reg_plan.f_cancelacion;
               ELSIF reg_plan.cant = 3 THEN vl_cuota2 := reg_plan.dias_atraso; vl_fecha2 := reg_plan.f_cancelacion;
               ELSIF reg_plan.cant = 4 THEN vl_cuota1 := reg_plan.dias_atraso; vl_fecha1 := reg_plan.f_cancelacion;
               END IF;
            END LOOP;
      ELSE
         v_fec_cuota := NULL;
      END IF;
      --
      -- Inserta las operaciones bajo linea
      --
      INSERT INTO PR_OPERACIONES_TMP ( sesion            , num_tramite        , cod_persona,
                                       des_moneda        , cod_sucursal       , fec_primer_desembolso,
                                       mon_saldo         , mon_utilizar       , f_vencimiento,
                                       monto_desembolsado, des_producto       , des_estado,
                                       des_tip_credito   , f_ultimo_desembolso, fecha_reprogramacion,
                                       f_paso_castigo    , f_paso_ejecucion   , f_paso_vencido      ,
                                       codigo_origen     , mon_solicitado     , mon_operacion,
                                       mon_tramite       , deu_ven_dir30      , tasa_original,
                                       unidad_ejecutora  )
                              VALUES ( p_sesion             , r_op_fl.num_tramite, v_no_credito,
                                       v_abrev_moneda       , 300                , v_fec_inicio,
                                       v_Saldo_dir_USD      , v_Saldo_cont_USD   , v_fecha_vencimiento,
                                       v_MontoDes_USD       , v_desc_producto    , v_abrev_estado,
                                       v_desc_instrumento2  , v_fec_cuota        , vl_fecha1,
                                       vl_fecha2            , vl_fecha3          , vl_fecha4,
                                       vl_cuota1            , vl_cuota2          , vl_cuota3,
                                       vl_cuota4            , vl_cuota5          , vl_cuota6,
                                       v_dias_atraso        );
      --
      vl_cuota4 := 0;
      vl_cuota3 := 0;
      vl_cuota2 := 0;
      vl_cuota1 := 0;
      vl_fecha1 := NULL;
      vl_fecha2 := NULL;
      vl_fecha3 := NULL;
      vl_fecha4 := NULL;
   END LOOP; -- Operaciones Fuera de Linea
    -- Recorre el Cursor de Operaciones Indirectas
   FOR r_op_ind IN v_cur_op_ind
   LOOP
      Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                            r_op_ind.num_tramite,
                                            p_fecha_hoy,
                                            v_no_credito,
                                            v_tasa_interes,
                                            v_dias_plazo,
                                            v_fec_inicio,
                                            v_fecha_vencimiento,
                                            v_fecha_cancelacion,
                                            v_dias_atraso,
                                            v_monto_operacion,
                                            v_saldo_cont,
                                            v_saldo_directo,
                                            v_intereses_ope,
                                            v_comision,
                                            v_cargos,
                                            v_mora,
                                            v_puni,
                                            v_poliza,
                                            v_mensaje_error);
      --
      Valida_Tip_Operacion_Bd ( r_op_ind.cod_tip_operacion,
                                v_des_operacion           ,
                                p_error                   );
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      --
      Valida_Producto_Bd ( r_op_ind.cod_tip_producto,
                           r_op_ind.cod_tip_operacion,
                           p_cod_empresa,
                           v_desc_producto,
                           p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Estado_Bd ( r_op_ind.codigo_estado,
                         v_desc_estado         ,
                         v_abrev_estado        ,
                         p_error               );
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
      Valida_Moneda_Bd ( r_op_ind.cod_moneda,
                         v_des_moneda,
                         v_abrev_moneda,
                         p_error);
      IF p_error IS NOT NULL THEN
         RETURN;
      END IF;
            Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                             v_Saldo_cont,
                                             P_Fecha_Hoy,
                                             r_op_ind.Cod_Moneda,
                                             P_Cod_Moneda_USD,
                                             p_Error,
                                             V_Saldo_cont_USD);
            Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                             v_Saldo_directo,
                                             P_Fecha_Hoy,
                                             r_op_ind.Cod_Moneda,
                                             P_Cod_Moneda_USD,
                                             p_Error,
                                             V_Saldo_dir_USD);
            --Convierte el monto desembolsado a dolares
            Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                             r_op_ind.mon_operacion,
                                             P_Fecha_Hoy,
                                             r_op_ind.Cod_Moneda,
                                             P_Cod_Moneda_USD,
                                             p_Error,
                                             v_MontoDes_USD);
      --
      -- Obtiene el nombre del Cliente
      --
      BEGIN
         SELECT SUBSTR(nombre,1,60)
           INTO v_desc_instrumento3
           FROM PERSONAS_X_PR_TRAMITE a, personas b
          WHERE a.cod_persona = b.cod_persona
            AND a.ind_titular = 'S'
            AND a.num_tramite = r_op_ind.num_tramite
            AND a.cod_empresa = p_cod_empresa;
      END;
      -- Inserta las operaciones indirectas
       INSERT INTO PR_OPERACIONES_TMP
                  ( sesion               , num_tramite          , cod_persona,
                    des_moneda           , cod_sucursal         , fec_primer_desembolso,
                    mon_saldo            , mon_utilizar         , f_vencimiento,
                    monto_desembolsado   , des_producto         , des_estado,
                    des_tip_credito      )
        VALUES
                  ( p_sesion                , r_op_ind.num_tramite  , v_no_credito,
                    v_abrev_moneda          , 400                   , v_fec_inicio,
                    v_Saldo_dir_USD         , v_Saldo_cont_USD      , v_fecha_vencimiento,
                    v_MontoDes_USD          , v_desc_producto       , v_abrev_estado,
                    v_desc_instrumento3     );
      END LOOP; -- Operaciones Indirectas
   IF p_error IS NOT NULL THEN
        RETURN;
   END IF;
   COMMIT;
END;
