PROCEDURE VERIFICA_ANTECRED IS
	v_param  VARCHAR2(500);
  v_en_ant BOOLEAN;
  v_idx    NUMBER := 1;
  v_emp_actual  VARCHAR2(5);
  --mocastro 02/04/2026 FSN1852 Unificacion de operaciones de creditos e1 e5 e10 e11
BEGIN
  v_en_ant := pr.empresa_en_antec_cred(:GLOBAL.CodEmpresa);
  
  IF v_en_ant THEN

    -- Empresa actual esta en el parametro -> modo multi-empresa
    :variables.multi_empresa := 'S';
    v_emp_actual := :BKCONTROL.LST_EMPRESA;
    -- Leer el parametro solo para poblar el listbox (uso local, no se persiste)
    v_param := pa.parametro_general('PR', 'ANTECEDENTES_CRED');

    -- Poblar el listbox con las empresas del parametro
    CLEAR_LIST('BKCONTROL.LST_EMPRESA');
    FOR reg IN (
      SELECT e.cod_empresa, e.tit_reportes
        FROM pa.empresa e
       WHERE INSTR(v_param, '|' || e.cod_empresa || '|') > 0
       ORDER BY TO_NUMBER(e.cod_empresa)
    ) LOOP
      ADD_LIST_ELEMENT('BKCONTROL.LST_EMPRESA', v_idx,
                       reg.tit_reportes, reg.cod_empresa);
      v_idx := v_idx + 1;
    END LOOP;

    -- Seleccionar la empresa actual por defecto
    :BKCONTROL.LST_EMPRESA := NVL(v_emp_actual, NVL(:VARIABLES.CodEmpresa, :GLOBAL.CodEmpresa));

    SET_ITEM_PROPERTY('BKCONTROL.LST_EMPRESA', ENABLED, PROPERTY_TRUE);
    :variables.nom_reporte := 'PRR0402M';
  ELSE
    :variables.multi_empresa := 'N';
    CLEAR_LIST('BKCONTROL.LST_EMPRESA');
    ADD_LIST_ELEMENT('BKCONTROL.LST_EMPRESA', v_idx, :VARIABLES.NomEmpresa, :VARIABLES.CodEmpresa);
    SET_ITEM_PROPERTY('BKCONTROL.LST_EMPRESA', ENABLED, PROPERTY_FALSE);
    :variables.nom_reporte := 'PRR0402G';
  END IF;
END;