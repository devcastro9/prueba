PROCEDURE pr.obt_prevision_rep (pr_cod_cliente in varchar2)
IS
--
-- Obtiene los tramites del Cliente, con sus respectivas previsiones
--
   cursor cur_tramite is 
      select PR_TRAMITE.num_tramite, PR_TRAMITE.codigo_estado, PR_TRAMITE.cod_moneda,
             PR_TRAMITE.bajo_linea_credito, PR_TIP_CREDITO.descripcion,
             PR_TRAMITE.cod_tip_operacion,PR_TRAMITE.unidad_ejecutora
        from pr_tramite, personas_x_pr_tramite a, pr_tip_credito, pr_tip_operacion
       where PR_TRAMITE.cod_tip_operacion = PR_TIP_OPERACION.cod_tip_operacion
         and PR_TRAMITE.cod_tip_operacion in ( :variables.cod_cartera,
                                               :variables.cod_boletas,
                                               :variables.cod_sobregiros,
                                               :variables.cod_tarjeta,
                                               :variables.cod_factoraje )
         and PR_TRAMITE.codigo_estado      =  Pr_Utl_Estados.Verif_Estado_Act_Cast (Pr_Tramite.Codigo_Estado)
     and PR_TIP_CREDITO.cod_tip_credito = PR_TRAMITE.cod_tip_credito
     and PR_TIP_CREDITO.cod_empresa = PR_TRAMITE.cod_empresa
     and PR_TRAMITE.cod_empresa = :variables.CodEmpresa
     and a.cod_empresa = pr_tramite.cod_empresa
     and a.num_tramite = pr_tramite.num_tramite
     and a.cod_persona = pr_cod_cliente;

   -- Variables de Tarjetas de Créditos
   p_nro_tarjeta NUMBER;
   p_fec_habilit DATE;
   p_fec_vencimi DATE;
   p_monto_aprob NUMBER;
   p_saldo_actua NUMBER;
   p_mensaje     VARCHAR2(20);

   -- Variables de Sobregiros
  Pcta_efectivo     NUMBER;
  pFec_Inicio       DATE;
  pCan_Dias_Gracia  NUMBER;
  pTasa_Interes     NUMBER;
  pFec_Vencimiento  DATE;
  pMon_Contratado   NUMBER;
  pPlazo_Dias       NUMBER;
  pFec_Cancelacion  DATE;
  pDias_Atraso      NUMBER;
  pFec_Ult_Mov      DATE;
  pSaldo_Vig        NUMBER;
  pCta_Vig          VARCHAR2(20);
  pInteres_Vig      NUMBER;
   pCta_Int_Vig      VARCHAR2(20);
   pSaldo_Ven        NUMBER;
   pCta_Ven          VARCHAR2(20);
   pInteres_Ven      NUMBER;
   pCta_Int_Ven      VARCHAR2(20);
   pInt_Susp_Ven     NUMBER;
   pCta_Int_Susp_Ven VARCHAR2(20);
   pSaldo_Eje        NUMBER;
   pCta_Eje          VARCHAR2(20);
   pInteres_Eje      NUMBER;
   pCta_Int_Eje      VARCHAR2(20);
   pCod_Error        VARCHAR2(20);
   pCta_Contingente  VARCHAR2(20);
   vl_mon_contingente NUMBER(16,2);
   vl_fec_estado    DATE;
   pNumOperacion  number;
   pFechaInicio   date;
   pTasaInteres   number;
   pFechaVence    date;
   pMonContratado number;
   pPlazoDias     number;
   pFechaCance    date;
   pDiasAtraso    number;
   pFecUltMov     date;
   pSaldoVig      number;
   pCtaSaldoVig   varchar2(20);
   pInteresVig    number;
   pCtaIntVig     varchar2(20);
   pSaldoVen      number;
   pCtaSaldoVen   varchar2(20);
   pInteresVen    number;
   pCtaIntVen     varchar2(20);
   pSaldoEjec     number;
   pCtaSaldoEjec  varchar2(20);
   pMsjError      varchar2(20);                 
   v_num_proceso  pr_provisiones.num_proceso%type;
   v_pre_con      number(16,2);
   v_prevision    number(16,2);
BEGIN
  -- 
  -- Obtiene nro. de proceso de prevision
  --
  Begin
     SELECT MAX(num_proceso)
       INTO v_num_proceso
       FROM PR_PROVISIONES
      WHERE cod_empresa  = :variables.codempresa
        AND TRUNC(fec_ult_calificacion) <= :variables.fecha
        AND provisionado = 'S';	
  End;
  for reg_tramite in cur_tramite loop
    :bkdetpre.num_tramite       := reg_tramite.num_tramite;
    :bkdetpre.codigo_estado     := reg_tramite.codigo_estado;
    :bkdetpre.cod_moneda        := reg_tramite.cod_moneda;
    :bkdetpre.cod_tip_operacion := reg_tramite.cod_tip_operacion;
    :bkdetpre.tipo_operacion    := reg_tramite.descripcion;
    :bkdetpre.linea             := reg_tramite.bajo_linea_credito;  
   --- 
   --- Obtiene prevision
   ---
   Begin
      --SELECT mon_pre_contin    , mon_prevision
      SELECT mon_pre_contin    , mon_prevision + NVL (mon_prev_diferido, 0) --OBCHOQUE 13/06/2022 MA 373820, Se adiciona la previsión diferida.
        INTO v_pre_con , v_prevision
        FROM PR_HIS_CALIF_X_PR_TRAMITE
       WHERE cod_empresa  = :variables.codempresa
         AND num_tramite  = reg_tramite.num_tramite
         AND num_proceso  = v_num_proceso;	
   Exception
        When no_data_found then
             null;
   End;


   if reg_tramite.cod_moneda <> :cod_dolares then
      --
      -- Cambia la moneda a Dolares Prevision Contingente
      --
      Convierte_Moneda_a_Moneda(:variables.codempresa,
                                v_pre_con,
                                :variables.fecha,
                                to_number(reg_tramite.cod_moneda),
                                :variables.cod_dolares,
                                :variables.mensaje,
                                :variables.mon_prevision);
      v_pre_con := :variables.mon_prevision;
      --
      -- Cambia la moneda a Dolares Prevision Normal
      --
      Convierte_Moneda_a_Moneda(:variables.codempresa,
                                v_prevision,
                                :variables.fecha,
                                to_number(reg_tramite.cod_moneda),
                                :variables.cod_dolares,
                                :variables.mensaje,
                                :variables.prevision);
      v_prevision := :variables.prevision;

   end if;
      :variables.tot_pre_con    := nvl(:variables.tot_pre_con,0)   + nvl(v_pre_con,0);
      :variables.tot_prevision  := nvl(:variables.tot_prevision,0) + nvl(v_prevision,0);      
  end loop;    
END;
