CREATE OR REPLACE PROCEDURE pr.Carga_Prr0402g ( --p_sesion          IN VARCHAR2,
                                                p_cod_empresa     IN VARCHAR2,
                                                p_cod_cliente     IN VARCHAR2,
                                                --p_linea           IN VARCHAR2,
                                                --p_nom_cliente     IN VARCHAR2,
                                                --p_cod_moneda_usd  IN VARCHAR2,
                                                p_fecha_hoy       IN DATE,
                                                p_error           OUT VARCHAR2,
                                                -- [25/03/2026] p_commit: TRUE (default) para llamada directa desde PRR0402g;
                                                --              FALSE cuando llama carga_prr0402m para evitar COMMIT intermedio
                                                --              entre empresas (critico si GTT es ON COMMIT DELETE ROWS).
                                                p_commit          IN BOOLEAN DEFAULT TRUE) IS
--
-- Fecha de creaci�n : 14/02/2001
-- Analista          : Gustavo Anzoategui
-- Fecha de modific. : 13/11/2002
-- Analista          : Luis De la Quintana
-- Objetivo          : Insertar en la tabla PR_OPERACIONES_TMP los registros
--                     que cumplen los criterios del cursor, para luego,
--                     desplegar los datos en el reporte PRR0402G
-- Fecha de modific. : 13/04/2004
-- Analista          : Luis De la Quintana
-- Fecha de Modific. : 29/06/2005
-- Analista          : Jos� luis Dur�n S.
-- Objetivo:De las deudas indirectas, ya no sale el instrumento sino mas bien el nombre del cliente
--
-- Fecha de Modific. : 11/05/2017
-- Analista          : No� Quenta Chavez.
-- Objetivo          :Convertir el monto desembolsado de todas las operaciones a dolares
--
-- Fecha de Modific. : 25/03/2026
-- Objetivo          : Se reemplaza PR_OPERACIONES_TMP por la GTT PR_ANTEC_CRED_GTT
--                     eliminando la dependencia de la columna SESION (la GTT es por sesion).
--                     TIPO_REGISTRO discrimina el origen: LIN=L�neas, BL=Bajo l�nea,
--                     FL=Fuera de l�nea, IND=Indirectas (equiv. a cod_sucursal 100/200/300/400)
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
   v_MontoOp_USD       NUMBER(16,2);  --variable para obtener el monto desembolsado de lineas de credito en d�lares
   v_MontoDes_USD       NUMBER(16,2);  --variable para obtener el monto desembolsado de operaciones bajo linea en d�lares
   v_intereses_ope     NUMBER(16,2);
   v_comision          NUMBER(16,2);
   v_cargos            NUMBER(16,2);
   v_mora              NUMBER(16,2);
   v_puni              NUMBER(16,2);
   v_poliza            NUMBER(16,2);
   v_mensaje_error     VARCHAR2(10);
   v_fec_inicio        DATE;
   v_linea             PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;  -- COD_OPER_LINEAS_CR
   v_cod_moneda_usd    PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;  -- COD_MONEDA_DOLAR
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
   -- [25/03/2026] BULK COLLECT + FORALL: se definen un tipo registro espejo de la GTT
   --              y cuatro colecciones (una por TIPO_REGISTRO). Los loops acumulan filas
   --              en memoria y un unico FORALL INSERT por seccion reemplaza los INSERTs
   --              individuales, eliminando el context-switch SQL<->PL/SQL fila a fila.
   TYPE t_gtt_rec IS RECORD (
      cod_empresa            PR.PR_ANTEC_CRED_GTT.COD_EMPRESA%TYPE,
      tipo_registro          PR.PR_ANTEC_CRED_GTT.TIPO_REGISTRO%TYPE,
      num_tramite            PR.PR_ANTEC_CRED_GTT.NUM_TRAMITE%TYPE,
      no_operacion           PR.PR_ANTEC_CRED_GTT.NO_OPERACION%TYPE,
      des_moneda             PR.PR_ANTEC_CRED_GTT.DES_MONEDA%TYPE,
      fec_inicio             PR.PR_ANTEC_CRED_GTT.FEC_INICIO%TYPE,
      fec_vencimiento        PR.PR_ANTEC_CRED_GTT.FEC_VENCIMIENTO%TYPE,
      saldo_directo          PR.PR_ANTEC_CRED_GTT.SALDO_DIRECTO%TYPE,
      saldo_contingente      PR.PR_ANTEC_CRED_GTT.SALDO_CONTINGENTE%TYPE,
      monto_desembolsado     PR.PR_ANTEC_CRED_GTT.MONTO_DESEMBOLSADO%TYPE,
      des_producto           PR.PR_ANTEC_CRED_GTT.DES_PRODUCTO%TYPE,
      des_estado             PR.PR_ANTEC_CRED_GTT.DES_ESTADO%TYPE,
      des_instrumento        PR.PR_ANTEC_CRED_GTT.DES_INSTRUMENTO%TYPE,
      nom_cliente            PR.PR_ANTEC_CRED_GTT.NOM_CLIENTE%TYPE,
      fec_prox_revision      PR.PR_ANTEC_CRED_GTT.FEC_PROX_REVISION%TYPE,
      num_tramite_padre      PR.PR_ANTEC_CRED_GTT.NUM_TRAMITE_PADRE%TYPE,
      fec_vto_prox_cuota     PR.PR_ANTEC_CRED_GTT.FEC_VTO_PROX_CUOTA%TYPE,
      fec_cancelacion_cuota1 PR.PR_ANTEC_CRED_GTT.FEC_CANCELACION_CUOTA1%TYPE,
      fec_cancelacion_cuota2 PR.PR_ANTEC_CRED_GTT.FEC_CANCELACION_CUOTA2%TYPE,
      fec_cancelacion_cuota3 PR.PR_ANTEC_CRED_GTT.FEC_CANCELACION_CUOTA3%TYPE,
      fec_cancelacion_cuota4 PR.PR_ANTEC_CRED_GTT.FEC_CANCELACION_CUOTA4%TYPE,
      dias_mora_cuota1       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA1%TYPE,
      dias_mora_cuota2       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA2%TYPE,
      dias_mora_cuota3       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA3%TYPE,
      dias_mora_cuota4       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA4%TYPE,
      dias_mora_cuota5       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA5%TYPE,
      dias_mora_cuota6       PR.PR_ANTEC_CRED_GTT.DIAS_MORA_CUOTA6%TYPE,
      dias_atraso            PR.PR_ANTEC_CRED_GTT.DIAS_ATRASO%TYPE
   );
   TYPE t_gtt_tab IS TABLE OF t_gtt_rec INDEX BY PLS_INTEGER;
   t_lin_bulk    t_gtt_tab;   -- coleccion para Lineas
   t_bl_bulk     t_gtt_tab;   -- coleccion para Bajo Linea
   t_fl_bulk     t_gtt_tab;   -- coleccion para Fuera de Linea
   t_ind_bulk    t_gtt_tab;   -- coleccion para Indirectas
   v_rec         t_gtt_rec;   -- registro de trabajo temporal
   --
   -- [25/03/2026] Cache para datos del plan de pagos (opt. 3b):
   --              evita abrir cur_plan multiples veces para el mismo no_credito.
   --              Indexado por no_credito (VARCHAR2); cada entrada almacena
   --              hasta 6 filas (cant 1..6) con dias_atraso y f_cancelacion.
   TYPE t_plan_row IS RECORD (
      dias_atraso   NUMBER,
      f_cancelacion DATE
   );
   TYPE t_plan_rows  IS TABLE OF t_plan_row  INDEX BY PLS_INTEGER;  -- cant 1..6
   TYPE t_plan_cache IS TABLE OF t_plan_rows INDEX BY VARCHAR2(20); -- clave: no_credito
   vc_plan_cache    t_plan_cache;
   v_plan_rows_work t_plan_rows;
   --
  -- Cursor de Lineas del Cliente
  --
   -- [25/03/2026] v_cur_lineas (opt. 5): IN(subquery) reemplazado por EXISTS.
   --              EXISTS detiene la busqueda al primer match; IN materializa
   --              todos los valores antes de comparar. Permite al optimizador
   --              usar el indice de PERSONAS_X_PR_TRAMITE(Cod_Empresa,Cod_Persona,Num_Tramite).
   /*-- Original con IN subquery (comentado por trazabilidad):
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
   */
   CURSOR v_cur_lineas IS
   SELECT a.codigo_empresa, a.no_credito, A.Num_Tramite, f_proxima_revision,
          A.Tipo_credito, A.F_apertura, A.F_Vencimiento,
          A.Codigo_Moneda, A.Estado,  A.Monto_credito, a.tipo_linea
     FROM PR_CREDITOS A
    WHERE A.Codigo_Empresa = P_Cod_Empresa
      AND A.Estado         = Pr_Utl_Estados.Verif_Estado_Linea(A.Estado)
      AND a.es_linea_credito = 'S'
      AND EXISTS (SELECT 1
                    FROM PERSONAS_X_PR_TRAMITE B
                   WHERE B.Cod_Empresa = P_Cod_Empresa
                     AND B.Cod_Persona = P_Cod_Cliente
                     AND B.Num_Tramite = A.Num_Tramite);
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
  -- [25/03/2026] v_cur_op_fl (opt. 5): IN(subquery) reemplazado por EXISTS.
  /*-- Original con IN subquery (comentado por trazabilidad):
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
  */
  CURSOR v_cur_op_fl IS
  SELECT T.num_tramite, T.fec_inicio, T.mon_operacion, T.codigo_estado,
         T.cod_moneda, T.cod_tip_operacion, T.cod_tip_producto, T.cod_tip_credito
    FROM PR_TRAMITE T
   WHERE T.cod_empresa = P_Cod_Empresa
     AND T.codigo_estado IN (Pr_Utl_Estados.verif_estado_act_cast (T.codigo_estado))
     AND T.bajo_linea_credito = 'N'
     AND EXISTS (SELECT 1
                   FROM PERSONAS_X_PR_TRAMITE B
                  WHERE B.Cod_Empresa = P_Cod_Empresa
                    AND B.Cod_Persona = P_Cod_Cliente
                    AND B.Num_Tramite = T.Num_Tramite)
   ORDER BY T.fec_inicio;
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
   -- [25/03/2026] cur_plan reescrito (opt. 3a): la subquery correlacionada
   --              MAX(no_cuota)-3 se reemplaza por MAX() OVER () analitico.
   --              Esto elimina un full-scan adicional de PR_PLAN_PAGOS por cada
   --              fila del outer query, pasando de N+1 lecturas a una sola pasada.
   --              ROWNUM reemplazado por ROW_NUMBER() OVER (ORDER BY no_cuota)
   --              para garantizar numeracion secuencial independiente del plan.
   /*-- Original con subquery correlacionada (comentado por trazabilidad):
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
   */
   CURSOR cur_plan ( pc_cod_empresa IN VARCHAR2,
                     pc_no_credito  IN NUMBER  ) IS
      SELECT no_cuota, f_cancelacion, (f_cancelacion - f_cuota) dias_atraso,
             ROW_NUMBER() OVER (ORDER BY no_cuota) cant
        FROM ( SELECT no_cuota, f_cancelacion, f_cuota,
                      MAX(no_cuota) OVER () AS max_cuota
                 FROM PR_PLAN_PAGOS
                WHERE no_credito     = pc_no_credito
                  AND estado         = 'C'
                  AND no_cuota       > 0
                  AND codigo_empresa = pc_cod_empresa )
       WHERE no_cuota >= max_cuota - 3
       ORDER BY no_cuota;
BEGIN
   p_error := NULL;
   v_linea          := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_LINEAS_CR');-- [25/03/2026]
   v_cod_moneda_usd := pa.PARAMETRO_GENERAL('PR', 'COD_MONEDA_DOLAR');-- [25/03/2026]
   DELETE FROM pr.PR_ANTEC_CRED_GTT WHERE cod_empresa = p_cod_empresa;-- [25/03/2026]
   FOR reg_lineas IN v_cur_lineas LOOP
      Valida_Producto_Bd(reg_lineas.tipo_credito,
                            --p_linea,        -- [25/03/2026] p_linea fue comentado como parametro; se usa v_linea (PARAM_GENERALES)
                            v_linea,
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
                                       v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                       p_Error,
                                       V_SaldoAct_USD);
      Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                        v_SaldoDisp,
                                        P_Fecha_Hoy,
                                        reg_lineas.Codigo_Moneda,
                                        v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                        p_Error,
                                        V_SaldoDisp_USD);
      --Convierte a d�lares el monto desembolsado
      Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                        reg_lineas.monto_credito,
                                        P_Fecha_Hoy,
                                        reg_lineas.Codigo_Moneda,
                                        v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                        p_Error,
                                        v_MontoOp_USD);
      -- Inserta las Lineas
      /*-- [25/03/2026] Reemplazado por INSERT en PR.PR_ANTEC_CRED_GTT (ver abajo)
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
      */
      -- [25/03/2026] INSERT en GTT: sesion eliminada (GTT por sesion), cod_sucursal=100 -> TIPO_REGISTRO='LIN'
      --              no_credito -> NO_OPERACION, mon_saldo -> SALDO_DIRECTO, mon_utilizar -> SALDO_CONTINGENTE
      --              f_paso_castigo (f_proxima_revision) -> FEC_PROX_REVISION
      /*-- [25/03/2026] BULK: INSERT individual reemplazado por acumulacion en coleccion (FORALL al final del loop LIN)
      INSERT INTO PR.PR_ANTEC_CRED_GTT
                 ( COD_EMPRESA            , TIPO_REGISTRO           , NUM_TRAMITE,
                   NO_OPERACION           , DES_MONEDA              , FEC_INICIO,
                   FEC_VENCIMIENTO        , SALDO_DIRECTO           , SALDO_CONTINGENTE,
                   MONTO_DESEMBOLSADO     , DES_PRODUCTO            , DES_ESTADO,
                   DES_INSTRUMENTO        , FEC_PROX_REVISION       )
          VALUES ( p_cod_empresa             , 'LIN'                   , reg_lineas.num_tramite,
                   TO_CHAR(reg_lineas.no_credito), v_des_moneda         , reg_lineas.f_apertura,
                   reg_lineas.f_vencimiento  , v_SaldoAct_USD          , v_SaldoDisp_USD,
                   v_MontoOp_USD             , v_desc_producto         , v_abrev_estado,
                   NULL                      , reg_lineas.f_proxima_revision );
      */
      v_rec.cod_empresa       := p_cod_empresa;
      v_rec.tipo_registro     := 'LIN';
      v_rec.num_tramite       := reg_lineas.num_tramite;
      v_rec.no_operacion      := TO_CHAR(reg_lineas.no_credito);
      v_rec.des_moneda        := v_des_moneda;
      v_rec.fec_inicio        := reg_lineas.f_apertura;
      v_rec.fec_vencimiento   := reg_lineas.f_vencimiento;
      v_rec.saldo_directo     := v_SaldoAct_USD;
      v_rec.saldo_contingente := v_SaldoDisp_USD;
      v_rec.monto_desembolsado:= v_MontoOp_USD;
      v_rec.des_producto      := v_desc_producto;
      v_rec.des_estado        := v_abrev_estado;
      v_rec.des_instrumento   := NULL;
      v_rec.nom_cliente       := NULL;
      v_rec.fec_prox_revision := reg_lineas.f_proxima_revision;
      v_rec.num_tramite_padre := NULL;
      v_rec.fec_vto_prox_cuota     := NULL;
      v_rec.fec_cancelacion_cuota1 := NULL;
      v_rec.fec_cancelacion_cuota2 := NULL;
      v_rec.fec_cancelacion_cuota3 := NULL;
      v_rec.fec_cancelacion_cuota4 := NULL;
      v_rec.dias_mora_cuota1  := NULL;
      v_rec.dias_mora_cuota2  := NULL;
      v_rec.dias_mora_cuota3  := NULL;
      v_rec.dias_mora_cuota4  := NULL;
      v_rec.dias_mora_cuota5  := NULL;
      v_rec.dias_mora_cuota6  := NULL;
      v_rec.dias_atraso       := NULL;
      t_lin_bulk(t_lin_bulk.COUNT + 1) := v_rec;
        -- Recorre el Cursor de Operaciones Bajo Linea
      FOR r_op_bl IN v_cur_op_bl (p_cod_empresa, reg_lineas.num_tramite) LOOP
         Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                               r_op_bl.num_tramite,
                                               p_fecha_hoy,
                                               v_no_credito,--no_operacion en tabla gtt
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
                                            v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                            p_Error,
                                            V_Saldo_cont_USD);
         Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                            v_Saldo_directo,
                                            P_Fecha_Hoy,
                                            r_op_bl.Cod_Moneda,
                                            v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                            p_Error,
                                            V_Saldo_dir_USD);
         --Convierte el monto desembolsado a dolares
         Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                            r_op_bl.mon_operacion,
                                            P_Fecha_Hoy,
                                            r_op_bl.Cod_Moneda,
                                            v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
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
            -- Busca los 6 �ltimos pagos y revisa los d�as de mora
            --
            -- [25/03/2026] opt. 3b: cache de plan por no_credito (BL).
            --              Primer acceso consulta cur_plan y guarda resultado;
            --              accesos posteriores al mismo no_credito usan el cache.
            IF NOT vc_plan_cache.EXISTS(v_no_credito) THEN
               v_plan_rows_work.DELETE;
               FOR reg_plan IN cur_plan(p_cod_empresa, TO_NUMBER(v_no_credito)) LOOP
                  v_plan_rows_work(reg_plan.cant).dias_atraso   := reg_plan.dias_atraso;
                  v_plan_rows_work(reg_plan.cant).f_cancelacion := reg_plan.f_cancelacion;
               END LOOP;
               vc_plan_cache(v_no_credito) := v_plan_rows_work;
            END IF;
            -- BL: cant 1->cuota6 ... cant 6->cuota1
            IF vc_plan_cache(v_no_credito).EXISTS(1) THEN vl_cuota6 := vc_plan_cache(v_no_credito)(1).dias_atraso; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(2) THEN vl_cuota5 := vc_plan_cache(v_no_credito)(2).dias_atraso; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(3) THEN vl_cuota4 := vc_plan_cache(v_no_credito)(3).dias_atraso; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(4) THEN vl_cuota3 := vc_plan_cache(v_no_credito)(4).dias_atraso; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(5) THEN vl_cuota2 := vc_plan_cache(v_no_credito)(5).dias_atraso; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(6) THEN vl_cuota1 := vc_plan_cache(v_no_credito)(6).dias_atraso; END IF;
         ELSE
            v_fec_cuota := NULL;
         END IF; --r_op_bl.cod_tip_operacion = 1 THEN
         --
         /*-- [25/03/2026] Reemplazado por INSERT en PR.PR_ANTEC_CRED_GTT (ver abajo)
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
         */
         -- [25/03/2026] INSERT en GTT: sesion eliminada (GTT por sesion), cod_sucursal=200 -> TIPO_REGISTRO='BL'
         --              cod_persona (v_no_credito) -> NO_OPERACION, mon_saldo -> SALDO_DIRECTO, mon_utilizar -> SALDO_CONTINGENTE
         --              numero_reprogramacion -> NUM_TRAMITE_PADRE, f_ultimo_desembolso -> FEC_VTO_PROX_CUOTA
         --              codigo_origen/mon_solicitado/mon_operacion/mon_tramite/deu_ven_dir30/tasa_original -> DIAS_MORA_CUOTA1-6
         --              unidad_ejecutora (v_dias_atraso) -> DIAS_ATRASO
         /*-- [25/03/2026] BULK: INSERT individual reemplazado por acumulacion en coleccion (FORALL al final del loop LIN)
         INSERT INTO PR.PR_ANTEC_CRED_GTT
                    ( COD_EMPRESA         , TIPO_REGISTRO        , NUM_TRAMITE,
                      NO_OPERACION        , DES_MONEDA           , FEC_INICIO,
                      FEC_VENCIMIENTO     , SALDO_DIRECTO        , SALDO_CONTINGENTE,
                      MONTO_DESEMBOLSADO  , DES_PRODUCTO         , DES_ESTADO,
                      DES_INSTRUMENTO     , NUM_TRAMITE_PADRE    , FEC_VTO_PROX_CUOTA,
                      DIAS_MORA_CUOTA1    , DIAS_MORA_CUOTA2     , DIAS_MORA_CUOTA3,
                      DIAS_MORA_CUOTA4    , DIAS_MORA_CUOTA5     , DIAS_MORA_CUOTA6,
                      DIAS_ATRASO         )
             VALUES ( p_cod_empresa          , 'BL'                 , r_op_bl.num_tramite,
                      v_no_credito           , v_abrev_moneda       , v_fec_inicio,
                      v_fecha_vencimiento    , v_Saldo_dir_USD      , v_Saldo_cont_USD,
                      v_MontoDes_USD         , v_desc_producto      , v_abrev_estado,
                      v_desc_instrumento1    , r_op_bl.num_tramite_padre, v_fec_cuota,
                      vl_cuota1              , vl_cuota2            , vl_cuota3,
                      vl_cuota4              , vl_cuota5            , vl_cuota6,
                      v_dias_atraso          );
         */
         v_rec.cod_empresa            := p_cod_empresa;
         v_rec.tipo_registro          := 'BL';
         v_rec.num_tramite            := r_op_bl.num_tramite;
         v_rec.no_operacion           := v_no_credito;
         v_rec.des_moneda             := v_abrev_moneda;
         v_rec.fec_inicio             := v_fec_inicio;
         v_rec.fec_vencimiento        := v_fecha_vencimiento;
         v_rec.saldo_directo          := v_Saldo_dir_USD;
         v_rec.saldo_contingente      := v_Saldo_cont_USD;
         v_rec.monto_desembolsado     := v_MontoDes_USD;
         v_rec.des_producto           := v_desc_producto;
         v_rec.des_estado             := v_abrev_estado;
         v_rec.des_instrumento        := v_desc_instrumento1;
         v_rec.nom_cliente            := NULL;
         v_rec.fec_prox_revision      := NULL;
         v_rec.num_tramite_padre      := r_op_bl.num_tramite_padre;
         v_rec.fec_vto_prox_cuota     := v_fec_cuota;
         v_rec.fec_cancelacion_cuota1 := NULL;
         v_rec.fec_cancelacion_cuota2 := NULL;
         v_rec.fec_cancelacion_cuota3 := NULL;
         v_rec.fec_cancelacion_cuota4 := NULL;
         v_rec.dias_mora_cuota1       := vl_cuota1;
         v_rec.dias_mora_cuota2       := vl_cuota2;
         v_rec.dias_mora_cuota3       := vl_cuota3;
         v_rec.dias_mora_cuota4       := vl_cuota4;
         v_rec.dias_mora_cuota5       := vl_cuota5;
         v_rec.dias_mora_cuota6       := vl_cuota6;
         v_rec.dias_atraso            := v_dias_atraso;
         t_bl_bulk(t_bl_bulk.COUNT + 1) := v_rec;
         --
         vl_cuota6 := 0;
         vl_cuota5 := 0;
         vl_cuota4 := 0;
         vl_cuota3 := 0;
         vl_cuota2 := 0;
         vl_cuota1 := 0;
      END LOOP; -- Operaciones Bajo Linea
    END LOOP;   -- Lineas
   -- [25/03/2026] BULK INSERT: Lineas (LIN)
   IF t_lin_bulk.COUNT > 0 THEN
      FORALL i IN 1..t_lin_bulk.COUNT
         INSERT INTO PR.PR_ANTEC_CRED_GTT
                    ( COD_EMPRESA, TIPO_REGISTRO, NUM_TRAMITE,
                      NO_OPERACION, DES_MONEDA, FEC_INICIO,
                      FEC_VENCIMIENTO, SALDO_DIRECTO, SALDO_CONTINGENTE,
                      MONTO_DESEMBOLSADO, DES_PRODUCTO, DES_ESTADO,
                      DES_INSTRUMENTO, FEC_PROX_REVISION )
             VALUES ( t_lin_bulk(i).cod_empresa, t_lin_bulk(i).tipo_registro, t_lin_bulk(i).num_tramite,
                      t_lin_bulk(i).no_operacion, t_lin_bulk(i).des_moneda, t_lin_bulk(i).fec_inicio,
                      t_lin_bulk(i).fec_vencimiento, t_lin_bulk(i).saldo_directo, t_lin_bulk(i).saldo_contingente,
                      t_lin_bulk(i).monto_desembolsado, t_lin_bulk(i).des_producto, t_lin_bulk(i).des_estado,
                      t_lin_bulk(i).des_instrumento, t_lin_bulk(i).fec_prox_revision );
   END IF;
   -- [25/03/2026] BULK INSERT: Operaciones Bajo Linea (BL)
   IF t_bl_bulk.COUNT > 0 THEN
      FORALL i IN 1..t_bl_bulk.COUNT
         INSERT INTO PR.PR_ANTEC_CRED_GTT
                    ( COD_EMPRESA, TIPO_REGISTRO, NUM_TRAMITE,
                      NO_OPERACION, DES_MONEDA, FEC_INICIO,
                      FEC_VENCIMIENTO, SALDO_DIRECTO, SALDO_CONTINGENTE,
                      MONTO_DESEMBOLSADO, DES_PRODUCTO, DES_ESTADO,
                      DES_INSTRUMENTO, NUM_TRAMITE_PADRE, FEC_VTO_PROX_CUOTA,
                      DIAS_MORA_CUOTA1, DIAS_MORA_CUOTA2, DIAS_MORA_CUOTA3,
                      DIAS_MORA_CUOTA4, DIAS_MORA_CUOTA5, DIAS_MORA_CUOTA6,
                      DIAS_ATRASO )
             VALUES ( t_bl_bulk(i).cod_empresa, t_bl_bulk(i).tipo_registro, t_bl_bulk(i).num_tramite,
                      t_bl_bulk(i).no_operacion, t_bl_bulk(i).des_moneda, t_bl_bulk(i).fec_inicio,
                      t_bl_bulk(i).fec_vencimiento, t_bl_bulk(i).saldo_directo, t_bl_bulk(i).saldo_contingente,
                      t_bl_bulk(i).monto_desembolsado, t_bl_bulk(i).des_producto, t_bl_bulk(i).des_estado,
                      t_bl_bulk(i).des_instrumento, t_bl_bulk(i).num_tramite_padre, t_bl_bulk(i).fec_vto_prox_cuota,
                      t_bl_bulk(i).dias_mora_cuota1, t_bl_bulk(i).dias_mora_cuota2, t_bl_bulk(i).dias_mora_cuota3,
                      t_bl_bulk(i).dias_mora_cuota4, t_bl_bulk(i).dias_mora_cuota5, t_bl_bulk(i).dias_mora_cuota6,
                      t_bl_bulk(i).dias_atraso );
   END IF;
    -- Recorre el Cursor de Operaciones Fuera de Linea
   FOR r_op_fl IN v_cur_op_fl LOOP
      Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                            r_op_fl.num_tramite,
                                            p_fecha_hoy,
                                            v_no_credito,--no_operacion en tabla gtt
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
                                         v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                         p_Error,
                                         V_Saldo_cont_USD);
      --
      Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                         v_Saldo_directo,
                                         P_Fecha_Hoy,
                                         r_op_fl.Cod_Moneda,
                                         v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                         p_Error,
                                         V_Saldo_dir_USD);
      --
      Pr_Utl.Convierte_moneda_a_moneda ( P_Cod_Empresa,
                                         r_op_fl.mon_operacion,
                                         P_Fecha_Hoy,
                                         r_op_fl.Cod_Moneda,
                                         v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
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
            -- Busca los 6 �ltimos pagos y revisa los d�as de mora
            --
            -- [25/03/2026] opt. 3b: cache de plan por no_credito (FL).
            --              Reutiliza el cache si el no_credito ya fue consultado en BL.
            IF NOT vc_plan_cache.EXISTS(v_no_credito) THEN
               v_plan_rows_work.DELETE;
               FOR reg_plan IN cur_plan(p_cod_empresa, TO_NUMBER(v_no_credito)) LOOP
                  v_plan_rows_work(reg_plan.cant).dias_atraso   := reg_plan.dias_atraso;
                  v_plan_rows_work(reg_plan.cant).f_cancelacion := reg_plan.f_cancelacion;
               END LOOP;
               vc_plan_cache(v_no_credito) := v_plan_rows_work;
            END IF;
            -- FL: cant 1->cuota4+fecha4 ... cant 4->cuota1+fecha1
            IF vc_plan_cache(v_no_credito).EXISTS(1) THEN vl_cuota4 := vc_plan_cache(v_no_credito)(1).dias_atraso; vl_fecha4 := vc_plan_cache(v_no_credito)(1).f_cancelacion; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(2) THEN vl_cuota3 := vc_plan_cache(v_no_credito)(2).dias_atraso; vl_fecha3 := vc_plan_cache(v_no_credito)(2).f_cancelacion; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(3) THEN vl_cuota2 := vc_plan_cache(v_no_credito)(3).dias_atraso; vl_fecha2 := vc_plan_cache(v_no_credito)(3).f_cancelacion; END IF;
            IF vc_plan_cache(v_no_credito).EXISTS(4) THEN vl_cuota1 := vc_plan_cache(v_no_credito)(4).dias_atraso; vl_fecha1 := vc_plan_cache(v_no_credito)(4).f_cancelacion; END IF;
      ELSE
         v_fec_cuota := NULL;
      END IF;
      --
      -- Inserta las operaciones fuera de linea
      --
      /*-- [25/03/2026] Reemplazado por INSERT en PR.PR_ANTEC_CRED_GTT (ver abajo)
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
      */
      -- [25/03/2026] INSERT en GTT: sesion eliminada (GTT por sesion), cod_sucursal=300 -> TIPO_REGISTRO='FL'
      --              cod_persona (v_no_credito) -> NO_OPERACION, mon_saldo -> SALDO_DIRECTO, mon_utilizar -> SALDO_CONTINGENTE
      --              f_ultimo_desembolso -> FEC_VTO_PROX_CUOTA
      --              fecha_reprogramacion/f_paso_castigo/f_paso_ejecucion/f_paso_vencido -> FEC_CANCELACION_CUOTA1-4
      --              codigo_origen/mon_solicitado/mon_operacion/mon_tramite/deu_ven_dir30/tasa_original -> DIAS_MORA_CUOTA1-6
      --              unidad_ejecutora (v_dias_atraso) -> DIAS_ATRASO
      /*-- [25/03/2026] BULK: INSERT individual reemplazado por acumulacion en coleccion (FORALL al final del loop FL)
      INSERT INTO PR.PR_ANTEC_CRED_GTT
                 ( COD_EMPRESA            , TIPO_REGISTRO           , NUM_TRAMITE,
                   NO_OPERACION           , DES_MONEDA              , FEC_INICIO,
                   FEC_VENCIMIENTO        , SALDO_DIRECTO           , SALDO_CONTINGENTE,
                   MONTO_DESEMBOLSADO     , DES_PRODUCTO            , DES_ESTADO,
                   DES_INSTRUMENTO        , FEC_VTO_PROX_CUOTA      , FEC_CANCELACION_CUOTA1,
                   FEC_CANCELACION_CUOTA2 , FEC_CANCELACION_CUOTA3  , FEC_CANCELACION_CUOTA4,
                   DIAS_MORA_CUOTA1       , DIAS_MORA_CUOTA2        , DIAS_MORA_CUOTA3,
                   DIAS_MORA_CUOTA4       , DIAS_MORA_CUOTA5        , DIAS_MORA_CUOTA6,
                   DIAS_ATRASO            )
          VALUES ( p_cod_empresa             , 'FL'                    , r_op_fl.num_tramite,
                   v_no_credito              , v_abrev_moneda          , v_fec_inicio,
                   v_fecha_vencimiento       , v_Saldo_dir_USD         , v_Saldo_cont_USD,
                   v_MontoDes_USD            , v_desc_producto         , v_abrev_estado,
                   v_desc_instrumento2       , v_fec_cuota             , vl_fecha1,
                   vl_fecha2                 , vl_fecha3               , vl_fecha4,
                   vl_cuota1                 , vl_cuota2               , vl_cuota3,
                   vl_cuota4                 , vl_cuota5               , vl_cuota6,
                   v_dias_atraso             );
      */
      v_rec.cod_empresa            := p_cod_empresa;
      v_rec.tipo_registro          := 'FL';
      v_rec.num_tramite            := r_op_fl.num_tramite;
      v_rec.no_operacion           := v_no_credito;
      v_rec.des_moneda             := v_abrev_moneda;
      v_rec.fec_inicio             := v_fec_inicio;
      v_rec.fec_vencimiento        := v_fecha_vencimiento;
      v_rec.saldo_directo          := v_Saldo_dir_USD;
      v_rec.saldo_contingente      := v_Saldo_cont_USD;
      v_rec.monto_desembolsado     := v_MontoDes_USD;
      v_rec.des_producto           := v_desc_producto;
      v_rec.des_estado             := v_abrev_estado;
      v_rec.des_instrumento        := v_desc_instrumento2;
      v_rec.nom_cliente            := NULL;
      v_rec.fec_prox_revision      := NULL;
      v_rec.num_tramite_padre      := NULL;
      v_rec.fec_vto_prox_cuota     := v_fec_cuota;
      v_rec.fec_cancelacion_cuota1 := vl_fecha1;
      v_rec.fec_cancelacion_cuota2 := vl_fecha2;
      v_rec.fec_cancelacion_cuota3 := vl_fecha3;
      v_rec.fec_cancelacion_cuota4 := vl_fecha4;
      v_rec.dias_mora_cuota1       := vl_cuota1;
      v_rec.dias_mora_cuota2       := vl_cuota2;
      v_rec.dias_mora_cuota3       := vl_cuota3;
      v_rec.dias_mora_cuota4       := vl_cuota4;
      v_rec.dias_mora_cuota5       := vl_cuota5;
      v_rec.dias_mora_cuota6       := vl_cuota6;
      v_rec.dias_atraso            := v_dias_atraso;
      t_fl_bulk(t_fl_bulk.COUNT + 1) := v_rec;
      --
      vl_cuota4 := 0;
      vl_cuota3 := 0;
      vl_cuota2 := 0;
      vl_cuota1 := 0;
      -- [25/03/2026] Agregado reset de cuota5/6: el loop cur_plan para FL solo asigna cuota1-4,
      --              pero si hubo registros BL antes en la misma sesion, cuota5/6 podian quedar
      --              con valores residuales del loop anterior y ser insertados incorrectamente en GTT.
      vl_cuota5 := 0;
      vl_cuota6 := 0;
      vl_fecha1 := NULL;
      vl_fecha2 := NULL;
      vl_fecha3 := NULL;
      vl_fecha4 := NULL;
   END LOOP; -- Operaciones Fuera de Linea
   -- [25/03/2026] BULK INSERT: Operaciones Fuera de Linea (FL)
   IF t_fl_bulk.COUNT > 0 THEN
      FORALL i IN 1..t_fl_bulk.COUNT
         INSERT INTO PR.PR_ANTEC_CRED_GTT
                    ( COD_EMPRESA, TIPO_REGISTRO, NUM_TRAMITE,
                      NO_OPERACION, DES_MONEDA, FEC_INICIO,
                      FEC_VENCIMIENTO, SALDO_DIRECTO, SALDO_CONTINGENTE,
                      MONTO_DESEMBOLSADO, DES_PRODUCTO, DES_ESTADO,
                      DES_INSTRUMENTO, FEC_VTO_PROX_CUOTA,
                      FEC_CANCELACION_CUOTA1, FEC_CANCELACION_CUOTA2,
                      FEC_CANCELACION_CUOTA3, FEC_CANCELACION_CUOTA4,
                      DIAS_MORA_CUOTA1, DIAS_MORA_CUOTA2, DIAS_MORA_CUOTA3,
                      DIAS_MORA_CUOTA4, DIAS_MORA_CUOTA5, DIAS_MORA_CUOTA6,
                      DIAS_ATRASO )
             VALUES ( t_fl_bulk(i).cod_empresa, t_fl_bulk(i).tipo_registro, t_fl_bulk(i).num_tramite,
                      t_fl_bulk(i).no_operacion, t_fl_bulk(i).des_moneda, t_fl_bulk(i).fec_inicio,
                      t_fl_bulk(i).fec_vencimiento, t_fl_bulk(i).saldo_directo, t_fl_bulk(i).saldo_contingente,
                      t_fl_bulk(i).monto_desembolsado, t_fl_bulk(i).des_producto, t_fl_bulk(i).des_estado,
                      t_fl_bulk(i).des_instrumento, t_fl_bulk(i).fec_vto_prox_cuota,
                      t_fl_bulk(i).fec_cancelacion_cuota1, t_fl_bulk(i).fec_cancelacion_cuota2,
                      t_fl_bulk(i).fec_cancelacion_cuota3, t_fl_bulk(i).fec_cancelacion_cuota4,
                      t_fl_bulk(i).dias_mora_cuota1, t_fl_bulk(i).dias_mora_cuota2, t_fl_bulk(i).dias_mora_cuota3,
                      t_fl_bulk(i).dias_mora_cuota4, t_fl_bulk(i).dias_mora_cuota5, t_fl_bulk(i).dias_mora_cuota6,
                      t_fl_bulk(i).dias_atraso );
   END IF;
    -- Recorre el Cursor de Operaciones Indirectas
   FOR r_op_ind IN v_cur_op_ind
   LOOP
      Pr_Abon3_Bd.Datos_Generales_Tramite ( p_cod_empresa,
                                            r_op_ind.num_tramite,
                                            p_fecha_hoy,
                                            v_no_credito,--no_operacion en tabla gtt
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
                                             v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                             p_Error,
                                             V_Saldo_cont_USD);
            Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                             v_Saldo_directo,
                                             P_Fecha_Hoy,
                                             r_op_ind.Cod_Moneda,
                                             v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
                                             p_Error,
                                             V_Saldo_dir_USD);
            --Convierte el monto desembolsado a dolares
            Pr_Utl.Convierte_moneda_a_moneda(P_Cod_Empresa,
                                             r_op_ind.mon_operacion,
                                             P_Fecha_Hoy,
                                             r_op_ind.Cod_Moneda,
                                             v_cod_moneda_usd, --[25/03/2026] parametro P_Cod_Moneda_USD fue comentado; se usa variable local v_cod_moneda_usd
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
      EXCEPTION
         -- [25/03/2026] Agregado manejo de excepciones: el SELECT carecia de EXCEPTION y un
         --              NO_DATA_FOUND o TOO_MANY_ROWS abortaria el procedimiento sin mensaje controlado
         WHEN NO_DATA_FOUND THEN
            v_desc_instrumento3 := NULL;
         WHEN TOO_MANY_ROWS THEN
            p_error := 'Error: mas de un titular encontrado para el tramite ' || r_op_ind.num_tramite;
            RETURN;
         WHEN OTHERS THEN
            p_error := 'Error al obtener nombre de cliente indirecto. Tramite: ' || r_op_ind.num_tramite || '. Error: ' || SQLERRM;
            RETURN;
      END;
      -- Inserta las operaciones indirectas
      /*-- [25/03/2026] Reemplazado por INSERT en PR.PR_ANTEC_CRED_GTT (ver abajo)
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
      */
      -- [25/03/2026] INSERT en GTT: sesion eliminada (GTT por sesion), cod_sucursal=400 -> TIPO_REGISTRO='IND'
      --              cod_persona (v_no_credito) -> NO_OPERACION, mon_saldo -> SALDO_DIRECTO, mon_utilizar -> SALDO_CONTINGENTE
      --              des_tip_credito (v_desc_instrumento3 = nombre cliente deudor) -> DES_INSTRUMENTO
      /*-- [25/03/2026] BULK: INSERT individual reemplazado por acumulacion en coleccion (FORALL al final del loop IND)
      INSERT INTO PR.PR_ANTEC_CRED_GTT
                 ( COD_EMPRESA         , TIPO_REGISTRO        , NUM_TRAMITE,
                   NO_OPERACION        , DES_MONEDA           , FEC_INICIO,
                   FEC_VENCIMIENTO     , SALDO_DIRECTO        , SALDO_CONTINGENTE,
                   MONTO_DESEMBOLSADO  , DES_PRODUCTO         , DES_ESTADO,
                   NOM_CLIENTE     )
          VALUES ( p_cod_empresa          , 'IND'                , r_op_ind.num_tramite,
                   v_no_credito           , v_abrev_moneda       , v_fec_inicio,
                   v_fecha_vencimiento    , v_Saldo_dir_USD      , v_Saldo_cont_USD,
                   v_MontoDes_USD         , v_desc_producto      , v_abrev_estado,
                   v_desc_instrumento3    );
      */
      v_rec.cod_empresa            := p_cod_empresa;
      v_rec.tipo_registro          := 'IND';
      v_rec.num_tramite            := r_op_ind.num_tramite;
      v_rec.no_operacion           := v_no_credito;
      v_rec.des_moneda             := v_abrev_moneda;
      v_rec.fec_inicio             := v_fec_inicio;
      v_rec.fec_vencimiento        := v_fecha_vencimiento;
      v_rec.saldo_directo          := v_Saldo_dir_USD;
      v_rec.saldo_contingente      := v_Saldo_cont_USD;
      v_rec.monto_desembolsado     := v_MontoDes_USD;
      v_rec.des_producto           := v_desc_producto;
      v_rec.des_estado             := v_abrev_estado;
      v_rec.des_instrumento        := NULL;
      v_rec.nom_cliente            := v_desc_instrumento3;
      v_rec.fec_prox_revision      := NULL;
      v_rec.num_tramite_padre      := NULL;
      v_rec.fec_vto_prox_cuota     := NULL;
      v_rec.fec_cancelacion_cuota1 := NULL;
      v_rec.fec_cancelacion_cuota2 := NULL;
      v_rec.fec_cancelacion_cuota3 := NULL;
      v_rec.fec_cancelacion_cuota4 := NULL;
      v_rec.dias_mora_cuota1       := NULL;
      v_rec.dias_mora_cuota2       := NULL;
      v_rec.dias_mora_cuota3       := NULL;
      v_rec.dias_mora_cuota4       := NULL;
      v_rec.dias_mora_cuota5       := NULL;
      v_rec.dias_mora_cuota6       := NULL;
      v_rec.dias_atraso            := NULL;
      t_ind_bulk(t_ind_bulk.COUNT + 1) := v_rec;
      END LOOP; -- Operaciones Indirectas
   -- [25/03/2026] BULK INSERT: Operaciones Indirectas (IND)
   IF t_ind_bulk.COUNT > 0 THEN
      FORALL i IN 1..t_ind_bulk.COUNT
         INSERT INTO PR.PR_ANTEC_CRED_GTT
                    ( COD_EMPRESA, TIPO_REGISTRO, NUM_TRAMITE,
                      NO_OPERACION, DES_MONEDA, FEC_INICIO,
                      FEC_VENCIMIENTO, SALDO_DIRECTO, SALDO_CONTINGENTE,
                      MONTO_DESEMBOLSADO, DES_PRODUCTO, DES_ESTADO,
                      NOM_CLIENTE )
             VALUES ( t_ind_bulk(i).cod_empresa, t_ind_bulk(i).tipo_registro, t_ind_bulk(i).num_tramite,
                      t_ind_bulk(i).no_operacion, t_ind_bulk(i).des_moneda, t_ind_bulk(i).fec_inicio,
                      t_ind_bulk(i).fec_vencimiento, t_ind_bulk(i).saldo_directo, t_ind_bulk(i).saldo_contingente,
                      t_ind_bulk(i).monto_desembolsado, t_ind_bulk(i).des_producto, t_ind_bulk(i).des_estado,
                      t_ind_bulk(i).nom_cliente );
   END IF;
   IF p_error IS NOT NULL THEN
        RETURN;
   END IF;
   -- [25/03/2026] COMMIT condicional: solo si p_commit=TRUE (llamada directa desde PRR0402g).
   --              El wrapper carga_prr0402m pasa FALSE y ejecuta su propio COMMIT al final,
   --              evitando commits intermedios que vacearian la GTT entre empresas (ON COMMIT DELETE ROWS).
   IF p_commit THEN
      COMMIT;
   END IF;
END;
